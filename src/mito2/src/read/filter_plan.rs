// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Unified filter plan that centralizes all filter decisions for mito scanners.
//!
//! The `FilterPlan` splits filter expressions into two phases:
//! - **Prefilter**: tag + time + partition expr filters (always applied), field filters
//!   (append mode only). Applied before merge/dedup.
//! - **Postfilter**: field filters + field-referencing partition expr (non-append mode only).
//!   Applied after merge/dedup.

use std::collections::HashSet;
use std::sync::Arc;

use common_recordbatch::filter::SimpleFilterEvaluator;
use datafusion_expr::Expr;
use datafusion_expr::utils::expr_to_columns;
use datatypes::schema::Schema;
use partition::expr::PartitionExpr;
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::{InvalidPartitionExprSnafu, Result};
use crate::region::options::MergeMode;
use crate::sst::index::bloom_filter::applier::BloomFilterIndexApplierRef;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplierRef;
use crate::sst::index::inverted_index::applier::InvertedIndexApplierRef;
use crate::sst::parquet::file_range::PartitionFilterContext;

/// Unified filter plan that centralizes all filter decisions into prefilter and postfilter phases.
#[derive(Default)]
pub struct FilterPlan {
    // --- Categorized SimpleFilterEvaluators ---
    /// Time filters (SimpleFilterEvaluator for timestamp column only).
    time_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,
    /// Field filters paired with their column IDs.
    /// Used by `PruneTimeIterator` in append mode for memtable prefiltering.
    /// `None` when not in append mode or no field filters exist.
    field_filters: Option<Arc<Vec<(ColumnId, SimpleFilterEvaluator)>>>,

    // --- Partition expression ---
    /// Region partition expression.
    region_partition_expr: Option<PartitionExpr>,

    // --- Predicates ---
    /// Predicate for prefilter phase (tag + time + partition expr; also field in append mode).
    prefilter_predicate: Option<Predicate>,
    /// Predicate excluding region partition expr (for files sharing the same partition).
    prefilter_predicate_without_region: Option<Predicate>,
    /// Field filter exprs for post-dedup evaluation.
    postfilter_exprs: Vec<Expr>,

    // --- Classified exprs for index appliers ---
    /// Exprs that do NOT reference field columns (for index applier[0]).
    non_field_exprs: Vec<Expr>,
    /// Exprs that reference field columns (for index applier[1]).
    field_exprs: Vec<Expr>,

    // --- Index appliers (built from classified exprs) ---
    inverted_index_appliers: [Option<InvertedIndexApplierRef>; 2],
    bloom_filter_index_appliers: [Option<BloomFilterIndexApplierRef>; 2],
    fulltext_index_appliers: [Option<FulltextIndexApplierRef>; 2],

    /// Whether to skip field filters in prefilter (true for non-append mode).
    skip_fields_in_prefilter: bool,
    /// Partition filter context for postfilter evaluation using physical expressions.
    /// Set when partition expr references field columns in non-append mode.
    partition_postfilter: Option<PartitionFilterContext>,
}

impl FilterPlan {
    /// Creates a new `FilterPlan` by classifying filter expressions.
    ///
    /// Index appliers are not built here; use `set_*_index_appliers()` methods
    /// to populate them after construction.
    pub fn new(
        metadata: &RegionMetadata,
        exprs: &[Expr],
        append_mode: bool,
        _merge_mode: MergeMode,
    ) -> Result<Self> {
        // --- Parse partition expr from region metadata ---
        let (region_partition_expr, partition_logical_expr) = Self::parse_partition_expr(metadata)?;

        // --- Classify expressions by semantic type ---
        let field_column_names: HashSet<&str> = metadata
            .field_columns()
            .map(|col| col.column_schema.name.as_str())
            .collect();
        let ts_column_name = metadata.time_index_column().column_schema.name.as_str();

        let mut time_evaluators = Vec::new();
        let mut field_evaluators = Vec::new();
        let mut non_field_filter_exprs = Vec::new();
        let mut field_filter_exprs = Vec::new();
        let mut postfilter_exprs = Vec::new();

        let mut columns = HashSet::new();
        for expr in exprs {
            columns.clear();
            if expr_to_columns(expr, &mut columns).is_err() {
                // If we can't extract columns, treat as non-field filter for safety
                non_field_filter_exprs.push(expr.clone());
                continue;
            }

            let references_field = columns
                .iter()
                .any(|col| field_column_names.contains(col.name.as_str()));
            let references_only_ts = columns.len() == 1
                && columns
                    .iter()
                    .all(|col| col.name.as_str() == ts_column_name);

            // Classify for index appliers: non-field vs field
            if references_field {
                field_filter_exprs.push(expr.clone());
            } else {
                non_field_filter_exprs.push(expr.clone());
            }

            // Build SimpleFilterEvaluators for time and field columns
            if references_only_ts {
                if let Some(evaluator) = SimpleFilterEvaluator::try_new(expr) {
                    time_evaluators.push(evaluator);
                }
            } else if references_field
                && columns.len() == 1
                && let Some(evaluator) = SimpleFilterEvaluator::try_new(expr)
            {
                // Look up column_id for this single-column field filter
                let col_name = columns.iter().next().unwrap().name.as_str();
                if let Some(col_meta) = metadata.column_by_name(col_name) {
                    field_evaluators.push((col_meta.column_id, evaluator));
                }
            }

            // For non-append mode, field-referencing exprs go to postfilter
            if !append_mode && references_field {
                postfilter_exprs.push(expr.clone());
            }
        }

        // --- Check if partition expr references field columns ---
        // If we can't extract columns, conservatively treat it as referencing fields.
        let partition_references_field = if let Some(partition_expr) = &partition_logical_expr {
            let mut partition_cols = HashSet::new();
            if expr_to_columns(partition_expr, &mut partition_cols).is_err() {
                true
            } else {
                partition_cols
                    .iter()
                    .any(|col| field_column_names.contains(col.name.as_str()))
            }
        } else {
            false
        };

        // If partition expr references field columns and we're not in append mode,
        // build a PartitionFilterContext for postfilter evaluation using physical expressions.
        // SimpleFilterEvaluator can't handle compound expressions like `a >= 5 AND a < 99`,
        // so we use DataFusion physical expressions instead.
        let partition_postfilter = if !append_mode
            && partition_references_field
            && let Some(partition_expr) = &region_partition_expr
        {
            // Collect referenced column names
            let partition_logical = partition_logical_expr.as_ref().unwrap();
            let mut partition_cols = HashSet::new();
            let _ = expr_to_columns(partition_logical, &mut partition_cols);
            let referenced_names: HashSet<&str> =
                partition_cols.iter().map(|c| c.name.as_str()).collect();

            // Build a minimal schema from metadata for referenced columns
            let column_schemas: Vec<_> = metadata
                .column_metadatas
                .iter()
                .filter(|col| referenced_names.contains(col.column_schema.name.as_str()))
                .map(|col| col.column_schema.clone())
                .collect();
            let partition_schema = Arc::new(Schema::new(column_schemas));

            let region_partition_physical_expr = partition_expr
                .try_as_physical_expr(partition_schema.arrow_schema())
                .context(InvalidPartitionExprSnafu {
                    expr: format!("{:?}", partition_expr),
                })?;

            Some(PartitionFilterContext {
                region_partition_physical_expr,
                partition_schema,
            })
        } else {
            None
        };

        // --- Build predicates ---
        let skip_fields_in_prefilter = !append_mode;

        // Prefilter predicate: all exprs + partition expr
        // Skip partition expr in prefilter if it references field columns
        // and region is not append-only — field filters must run after dedup.
        let mut prefilter_all_exprs = exprs.to_vec();
        if let Some(partition_expr) = &partition_logical_expr
            && (append_mode || !partition_references_field)
        {
            prefilter_all_exprs.push(partition_expr.clone());
        }
        let prefilter_predicate = if prefilter_all_exprs.is_empty() {
            None
        } else {
            Some(Predicate::new(prefilter_all_exprs))
        };

        // Prefilter predicate without region partition expr
        let prefilter_predicate_without_region = if exprs.is_empty() {
            None
        } else {
            Some(Predicate::new(exprs.to_vec()))
        };

        // --- Build filter evaluators ---
        let time_filters = if time_evaluators.is_empty() {
            None
        } else {
            Some(Arc::new(time_evaluators))
        };

        let field_filters = if field_evaluators.is_empty() || !append_mode {
            None
        } else {
            Some(Arc::new(field_evaluators))
        };

        Ok(Self {
            time_filters,
            field_filters,
            region_partition_expr,
            prefilter_predicate,
            prefilter_predicate_without_region,
            postfilter_exprs,
            non_field_exprs: non_field_filter_exprs,
            field_exprs: field_filter_exprs,
            inverted_index_appliers: [None, None],
            bloom_filter_index_appliers: [None, None],
            fulltext_index_appliers: [None, None],
            skip_fields_in_prefilter,
            partition_postfilter,
        })
    }

    /// Parses the partition expression from region metadata.
    fn parse_partition_expr(
        metadata: &RegionMetadata,
    ) -> Result<(Option<PartitionExpr>, Option<Expr>)> {
        if let Some(expr_json) = metadata.partition_expr.as_ref()
            && !expr_json.is_empty()
            && let Some(partition_expr) = PartitionExpr::from_json_str(expr_json)
                .context(InvalidPartitionExprSnafu { expr: expr_json })?
        {
            let logical_expr =
                partition_expr
                    .try_as_logical_expr()
                    .context(InvalidPartitionExprSnafu {
                        expr: expr_json.clone(),
                    })?;
            Ok((Some(partition_expr), Some(logical_expr)))
        } else {
            Ok((None, None))
        }
    }

    // --- Accessors ---

    /// Returns time filters for `PruneTimeIterator`.
    pub(crate) fn time_filters(&self) -> Option<Arc<Vec<SimpleFilterEvaluator>>> {
        self.time_filters.clone()
    }

    /// Returns the prefilter predicate (all exprs + partition expr).
    /// Used for SST row group pruning.
    pub(crate) fn prefilter_predicate(&self) -> Option<&Predicate> {
        self.prefilter_predicate.as_ref()
    }

    /// Returns the prefilter predicate excluding the region partition expr.
    /// Used for files that share the same partition.
    pub(crate) fn prefilter_predicate_without_region(&self) -> Option<&Predicate> {
        self.prefilter_predicate_without_region.as_ref()
    }

    /// Returns true if postfilter is needed (non-append mode with field filters or
    /// field-referencing partition expr).
    pub(crate) fn needs_postfilter(&self) -> bool {
        !self.postfilter_exprs.is_empty() || self.partition_postfilter.is_some()
    }

    /// Returns the postfilter exprs for post-dedup evaluation.
    pub(crate) fn postfilter_exprs(&self) -> &[Expr] {
        &self.postfilter_exprs
    }

    /// Returns the partition filter context for postfilter evaluation.
    pub(crate) fn partition_postfilter(&self) -> Option<&PartitionFilterContext> {
        self.partition_postfilter.as_ref()
    }

    /// Returns whether to skip field filters in prefilter phase.
    pub(crate) fn skip_fields_in_prefilter(&self) -> bool {
        self.skip_fields_in_prefilter
    }

    /// Returns the region partition expression.
    pub(crate) fn region_partition_expr(&self) -> Option<&PartitionExpr> {
        self.region_partition_expr.as_ref()
    }

    /// Returns exprs that do NOT reference field columns (for index applier[0]).
    pub(crate) fn non_field_exprs(&self) -> &[Expr] {
        &self.non_field_exprs
    }

    /// Returns exprs that reference field columns (for index applier[1]).
    pub(crate) fn field_exprs(&self) -> &[Expr] {
        &self.field_exprs
    }

    /// Returns inverted index appliers.
    pub(crate) fn inverted_index_appliers(&self) -> &[Option<InvertedIndexApplierRef>; 2] {
        &self.inverted_index_appliers
    }

    /// Returns bloom filter index appliers.
    pub(crate) fn bloom_filter_index_appliers(&self) -> &[Option<BloomFilterIndexApplierRef>; 2] {
        &self.bloom_filter_index_appliers
    }

    /// Returns fulltext index appliers.
    pub(crate) fn fulltext_index_appliers(&self) -> &[Option<FulltextIndexApplierRef>; 2] {
        &self.fulltext_index_appliers
    }

    // --- Setters for index appliers (built externally by ScanRegion) ---

    /// Sets inverted index appliers.
    pub(crate) fn set_inverted_index_appliers(
        &mut self,
        appliers: [Option<InvertedIndexApplierRef>; 2],
    ) {
        self.inverted_index_appliers = appliers;
    }

    /// Sets bloom filter index appliers.
    pub(crate) fn set_bloom_filter_index_appliers(
        &mut self,
        appliers: [Option<BloomFilterIndexApplierRef>; 2],
    ) {
        self.bloom_filter_index_appliers = appliers;
    }

    /// Sets fulltext index appliers.
    pub(crate) fn set_fulltext_index_appliers(
        &mut self,
        appliers: [Option<FulltextIndexApplierRef>; 2],
    ) {
        self.fulltext_index_appliers = appliers;
    }

    /// Returns field filters paired with column IDs for memtable prefiltering in append mode.
    /// Returns `None` in non-append mode.
    pub(crate) fn field_filters(&self) -> Option<Arc<Vec<(ColumnId, SimpleFilterEvaluator)>>> {
        self.field_filters.clone()
    }
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    fn test_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag0", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field0",
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field1",
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![1]);
        builder.build().unwrap()
    }

    #[test]
    fn test_filter_plan_append_mode() {
        use datafusion_expr::{col, lit};

        let metadata = test_metadata();
        let exprs = vec![
            col("ts").gt_eq(lit(100i64)),
            col("tag0").eq(lit("foo")),
            col("field0").gt(lit(1.0)),
        ];

        let plan = FilterPlan::new(&metadata, &exprs, true, MergeMode::LastRow).unwrap();

        // In append mode: no postfilter needed
        assert!(!plan.needs_postfilter());
        assert!(plan.postfilter_exprs().is_empty());
        assert!(!plan.skip_fields_in_prefilter());

        // Time filters should exist
        assert!(plan.time_filters().is_some());
        // Field filters should exist in append mode
        assert!(plan.field_filters().is_some());

        // Prefilter predicate should include all exprs
        assert!(plan.prefilter_predicate().is_some());

        // Non-field exprs: ts + tag0
        assert_eq!(plan.non_field_exprs().len(), 2);
        // Field exprs: field0
        assert_eq!(plan.field_exprs().len(), 1);
    }

    #[test]
    fn test_filter_plan_non_append_mode() {
        use datafusion_expr::{col, lit};

        let metadata = test_metadata();
        let exprs = vec![
            col("ts").gt_eq(lit(100i64)),
            col("tag0").eq(lit("foo")),
            col("field0").gt(lit(1.0)),
        ];

        let plan = FilterPlan::new(&metadata, &exprs, false, MergeMode::LastRow).unwrap();

        // In non-append mode: postfilter needed for field filters
        assert!(plan.needs_postfilter());
        assert_eq!(plan.postfilter_exprs().len(), 1);
        assert!(plan.skip_fields_in_prefilter());

        // Time filters should exist
        assert!(plan.time_filters().is_some());
        // Field filters should NOT exist (non-append mode)
        assert!(plan.field_filters().is_none());

        // Non-field exprs: ts + tag0
        assert_eq!(plan.non_field_exprs().len(), 2);
        // Field exprs: field0
        assert_eq!(plan.field_exprs().len(), 1);
    }

    /// Creates test metadata with a partition expr that references a field column (field0 >= 5.0).
    fn test_metadata_with_field_partition_expr() -> RegionMetadata {
        use partition::expr::{Operand, PartitionExpr as PExpr, RestrictedOp};

        let partition_expr = PExpr::new(
            Operand::Column("field0".to_string()),
            RestrictedOp::GtEq,
            Operand::Value(datatypes::value::Value::Float64(
                datatypes::value::OrderedFloat(5.0),
            )),
        );
        let json = partition_expr.as_json_str().unwrap();

        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag0", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field0",
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .primary_key(vec![1])
            .partition_expr_json(Some(json));
        builder.build().unwrap()
    }

    #[test]
    fn test_filter_plan_non_append_field_partition_expr() {
        use datafusion_expr::{col, lit};

        let metadata = test_metadata_with_field_partition_expr();
        let exprs = vec![col("ts").gt_eq(lit(100i64)), col("tag0").eq(lit("foo"))];

        let plan = FilterPlan::new(&metadata, &exprs, false, MergeMode::LastRow).unwrap();

        // The field-referencing partition expr should be handled via partition_postfilter
        assert!(plan.needs_postfilter());
        // postfilter_exprs is empty because the partition expr is handled by physical expr
        assert_eq!(plan.postfilter_exprs().len(), 0);
        // partition_postfilter should be set
        assert!(plan.partition_postfilter().is_some());

        // Prefilter predicate should NOT include the partition expr (only user exprs)
        let prefilter = plan.prefilter_predicate().unwrap();
        // Should have only the 2 user exprs, not the partition expr
        assert_eq!(prefilter.exprs().len(), 2);

        // Prefilter without region should also have 2 user exprs
        let prefilter_no_region = plan.prefilter_predicate_without_region().unwrap();
        assert_eq!(prefilter_no_region.exprs().len(), 2);
    }

    #[test]
    fn test_filter_plan_append_field_partition_expr() {
        use datafusion_expr::{col, lit};

        let metadata = test_metadata_with_field_partition_expr();
        let exprs = vec![col("ts").gt_eq(lit(100i64)), col("tag0").eq(lit("foo"))];

        let plan = FilterPlan::new(&metadata, &exprs, true, MergeMode::LastRow).unwrap();

        // In append mode, field-referencing partition expr is safe in prefilter
        assert!(!plan.needs_postfilter());
        assert!(plan.postfilter_exprs().is_empty());

        // Prefilter predicate should include user exprs + partition expr
        let prefilter = plan.prefilter_predicate().unwrap();
        assert_eq!(prefilter.exprs().len(), 3);
    }

    #[test]
    fn test_filter_plan_no_filters() {
        let metadata = test_metadata();
        let plan = FilterPlan::new(&metadata, &[], true, MergeMode::LastRow).unwrap();

        assert!(!plan.needs_postfilter());
        assert!(plan.time_filters().is_none());
        assert!(plan.field_filters().is_none());
        assert!(plan.prefilter_predicate().is_none());
        assert!(plan.prefilter_predicate_without_region().is_none());
    }
}
