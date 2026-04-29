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

//! Read-time options for the flat SST format.
//!
//! [`FlatReadOptions`] declares which physical columns must actually be read
//! from parquet. Columns whose flag is `false` are not projected at the
//! parquet layer; the SST read boundary substitutes a cheap mocked array so
//! downstream stages still see the canonical layout
//! `[raw tag cols, field cols, time index, __primary_key, __sequence, __op_type]`.
//!
//! Construction sites do not set the flags directly. They build a
//! [`FlatReadContext`] describing the situation (append mode, compaction,
//! encoding, projection / filter shape, merge-split decision) and call
//! [`FlatReadOptions::from_context`], which encodes the rule table.

use store_api::codec::PrimaryKeyEncoding;

/// Controls which physical columns are read from a flat-format parquet file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlatReadOptions {
    /// Read raw tag columns from the SST.
    ///
    /// When `false`, the SST read boundary substitutes mocked tag arrays.
    /// Consumers that need real tag values must decode them from
    /// `__primary_key` (e.g. `FlatProjectionMapper` does this lazily).
    ///
    /// Always `false` for sparse encoding because raw tag columns are not
    /// stored in sparse-encoded SSTs.
    pub read_raw_tag_columns: bool,
    /// Read the encoded `__primary_key` column from the SST.
    ///
    /// When `false`, a mocked binary dictionary (single empty-string value)
    /// is substituted at the boundary. Setting this to `false` is unsafe if
    /// any downstream stage actually consumes `__primary_key`: merge / dedup
    /// / tag decode (sparse, low-card split) / pk-filter / partition filter.
    pub read_primary_key_column: bool,
    /// Read the `__sequence` column from the SST.
    ///
    /// When `false`, a constant `UInt64Array` (zeroed) is substituted. Safe
    /// in append-mode + non-compaction reads where no dedup / merge stage
    /// consumes the sequence number.
    pub read_sequence_column: bool,
    /// Read the `__op_type` column from the SST.
    ///
    /// When `false`, a constant `UInt8Array` (`OpType::Put`) is substituted.
    /// Safe in append-mode + non-compaction reads.
    pub read_op_type_column: bool,
}

impl FlatReadOptions {
    /// Returns options that read every column. Matches the behavior before
    /// this struct was introduced; safe default for any caller.
    pub const fn full() -> Self {
        Self {
            read_raw_tag_columns: true,
            read_primary_key_column: true,
            read_sequence_column: true,
            read_op_type_column: true,
        }
    }

    /// Derives the options from a [`FlatReadContext`].
    ///
    /// Rule table:
    ///
    /// | append | compaction | encoding | tag_proj | low-card split | tag | pk | seq | op |
    /// |--------|-----------|---------|----------|----------------|-----|----|-----|-----|
    /// | yes    | no        | dense   | no       | n/a            | ❌  | ❌ | ❌  | ❌  |
    /// | yes    | no        | dense   | yes      | n/a            | ✅  | ❌ | ❌  | ❌  |
    /// | yes    | no        | sparse  | no       | n/a            | ❌  | ❌ | ❌  | ❌  |
    /// | yes    | no        | sparse  | yes      | n/a            | ❌  | ✅ | ❌  | ❌  |
    /// | yes    | yes       | any     | any      | n/a            | ✅  | ✅ | ✅  | ✅  |
    /// | no     | any       | dense   | any      | yes            | ❌  | ✅ | ✅  | ✅  |
    /// | no     | any       | dense   | any      | no             | ✅  | ✅ | ✅  | ✅  |
    /// | no     | any       | sparse  | any      | any            | ❌  | ✅ | ✅  | ✅  |
    ///
    /// `need_pk_filter` and `filter_columns_have_tag` may force a flag back
    /// to `true` to keep precise filters correct.
    pub fn from_context(ctx: FlatReadContext) -> Self {
        let mut opts = if ctx.compaction {
            // Compaction always needs the full canonical layout to feed
            // dedup / merge.
            Self::full()
        } else if ctx.append_mode {
            Self::for_append(ctx.encoding, ctx.projection_has_tag)
        } else {
            Self::for_merge(ctx.encoding, ctx.merge_split_low_cardinality)
        };

        // Force-on guards: precise filters and partition filters must see real
        // values, regardless of the rule-table outcome.
        if ctx.need_pk_filter {
            opts.read_primary_key_column = true;
        }
        // A per-series row selector (e.g. `LastRow`) groups rows by their
        // primary key, so the real `__primary_key` column must be present.
        if ctx.series_row_selector {
            opts.read_primary_key_column = true;
        }
        if ctx.filter_columns_have_tag {
            // The pk is needed to evaluate the filter under sparse encoding;
            // the raw tag column is needed under dense encoding. Force the pk
            // on for both, and the raw tag for dense — the sparse path
            // overrides `read_raw_tag_columns` back to `false` below.
            opts.read_primary_key_column = true;
            if ctx.encoding == PrimaryKeyEncoding::Dense {
                opts.read_raw_tag_columns = true;
            }
        }

        // Sparse encoding never has raw tag columns physically stored in the
        // SST. Outside of compaction we rely on lazy decoding at the
        // projection layer, so disable raw tag reads here. During compaction
        // the legacy primary-key path may still decode them eagerly via
        // `FlatConvertFormat`, so we leave the flag at its rule-table value.
        if ctx.encoding == PrimaryKeyEncoding::Sparse && !ctx.compaction {
            opts.read_raw_tag_columns = false;
        }

        opts
    }

    /// Append-mode + non-compaction sub-rule.
    fn for_append(encoding: PrimaryKeyEncoding, projection_has_tag: bool) -> Self {
        match (encoding, projection_has_tag) {
            (PrimaryKeyEncoding::Dense, false) => Self {
                read_raw_tag_columns: false,
                read_primary_key_column: false,
                read_sequence_column: false,
                read_op_type_column: false,
            },
            (PrimaryKeyEncoding::Dense, true) => Self {
                read_raw_tag_columns: true,
                read_primary_key_column: false,
                read_sequence_column: false,
                read_op_type_column: false,
            },
            (PrimaryKeyEncoding::Sparse, false) => Self {
                read_raw_tag_columns: false,
                read_primary_key_column: false,
                read_sequence_column: false,
                read_op_type_column: false,
            },
            (PrimaryKeyEncoding::Sparse, true) => Self {
                read_raw_tag_columns: false,
                read_primary_key_column: true,
                read_sequence_column: false,
                read_op_type_column: false,
            },
        }
    }

    /// Merge-mode (non-compaction) sub-rule. Compaction is handled separately
    /// and uses [`Self::full`].
    fn for_merge(encoding: PrimaryKeyEncoding, merge_split_low_cardinality: bool) -> Self {
        match (encoding, merge_split_low_cardinality) {
            (PrimaryKeyEncoding::Dense, true) => Self {
                read_raw_tag_columns: false,
                read_primary_key_column: true,
                read_sequence_column: true,
                read_op_type_column: true,
            },
            (PrimaryKeyEncoding::Dense, false) => Self::full(),
            (PrimaryKeyEncoding::Sparse, _) => Self {
                read_raw_tag_columns: false,
                read_primary_key_column: true,
                read_sequence_column: true,
                read_op_type_column: true,
            },
        }
    }

    /// Returns `true` if any internal column is mocked at the boundary.
    pub fn has_mocked_columns(&self) -> bool {
        !self.read_primary_key_column || !self.read_sequence_column || !self.read_op_type_column
    }
}

impl Default for FlatReadOptions {
    fn default() -> Self {
        Self::full()
    }
}

/// Inputs to [`FlatReadOptions::from_context`].
#[derive(Debug, Clone, Copy)]
pub struct FlatReadContext {
    /// Region is configured in append mode.
    pub append_mode: bool,
    /// Reader is being constructed for compaction.
    pub compaction: bool,
    /// Primary key encoding of the region.
    pub encoding: PrimaryKeyEncoding,
    /// Output projection includes at least one tag column.
    pub projection_has_tag: bool,
    /// Pushed-down filters reference at least one tag column.
    pub filter_columns_have_tag: bool,
    /// A pk-prefilter or partition filter is in effect for this scan.
    pub need_pk_filter: bool,
    /// A per-series selector (e.g. `LastRow`) groups rows by primary key, so
    /// the real `__primary_key` column must be read.
    pub series_row_selector: bool,
    /// Scan-level decision: sources will be split by primary key for low
    /// cardinality, so raw tag columns can be decoded after merge instead of
    /// before. Ignored when `compaction` is `true`.
    pub merge_split_low_cardinality: bool,
}

impl FlatReadContext {
    /// Builds a context that always yields [`FlatReadOptions::full`].
    pub const fn full() -> Self {
        Self {
            append_mode: false,
            compaction: true,
            encoding: PrimaryKeyEncoding::Dense,
            projection_has_tag: true,
            filter_columns_have_tag: true,
            need_pk_filter: true,
            series_row_selector: false,
            merge_split_low_cardinality: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(
        append_mode: bool,
        compaction: bool,
        encoding: PrimaryKeyEncoding,
        projection_has_tag: bool,
        merge_split_low_cardinality: bool,
    ) -> FlatReadContext {
        FlatReadContext {
            append_mode,
            compaction,
            encoding,
            projection_has_tag,
            filter_columns_have_tag: false,
            need_pk_filter: false,
            series_row_selector: false,
            merge_split_low_cardinality,
        }
    }

    #[test]
    fn append_no_compaction_dense_no_tag() {
        let opts = FlatReadOptions::from_context(ctx(
            true,
            false,
            PrimaryKeyEncoding::Dense,
            false,
            false,
        ));
        assert_eq!(
            opts,
            FlatReadOptions {
                read_raw_tag_columns: false,
                read_primary_key_column: false,
                read_sequence_column: false,
                read_op_type_column: false,
            }
        );
    }

    #[test]
    fn append_no_compaction_dense_with_tag() {
        let opts =
            FlatReadOptions::from_context(ctx(true, false, PrimaryKeyEncoding::Dense, true, false));
        assert_eq!(
            opts,
            FlatReadOptions {
                read_raw_tag_columns: true,
                read_primary_key_column: false,
                read_sequence_column: false,
                read_op_type_column: false,
            }
        );
    }

    #[test]
    fn append_no_compaction_sparse_no_tag() {
        let opts = FlatReadOptions::from_context(ctx(
            true,
            false,
            PrimaryKeyEncoding::Sparse,
            false,
            false,
        ));
        assert_eq!(
            opts,
            FlatReadOptions {
                read_raw_tag_columns: false,
                read_primary_key_column: false,
                read_sequence_column: false,
                read_op_type_column: false,
            }
        );
    }

    #[test]
    fn append_no_compaction_sparse_with_tag() {
        let opts = FlatReadOptions::from_context(ctx(
            true,
            false,
            PrimaryKeyEncoding::Sparse,
            true,
            false,
        ));
        assert_eq!(
            opts,
            FlatReadOptions {
                read_raw_tag_columns: false,
                read_primary_key_column: true,
                read_sequence_column: false,
                read_op_type_column: false,
            }
        );
    }

    #[test]
    fn append_compaction_dense_full() {
        let opts =
            FlatReadOptions::from_context(ctx(true, true, PrimaryKeyEncoding::Dense, false, false));
        assert_eq!(opts, FlatReadOptions::full());
    }

    #[test]
    fn append_compaction_sparse_keeps_full() {
        // Compaction keeps the full layout even on sparse encoding; the
        // legacy decode path in `FlatConvertFormat` still injects raw tag
        // columns and downstream merge/dedup expects them.
        let opts = FlatReadOptions::from_context(ctx(
            true,
            true,
            PrimaryKeyEncoding::Sparse,
            false,
            false,
        ));
        assert_eq!(opts, FlatReadOptions::full());
    }

    #[test]
    fn merge_dense_low_cardinality_split() {
        let opts =
            FlatReadOptions::from_context(ctx(false, false, PrimaryKeyEncoding::Dense, true, true));
        assert_eq!(
            opts,
            FlatReadOptions {
                read_raw_tag_columns: false,
                read_primary_key_column: true,
                read_sequence_column: true,
                read_op_type_column: true,
            }
        );
    }

    #[test]
    fn merge_dense_high_cardinality_no_split() {
        let opts = FlatReadOptions::from_context(ctx(
            false,
            false,
            PrimaryKeyEncoding::Dense,
            true,
            false,
        ));
        assert_eq!(opts, FlatReadOptions::full());
    }

    #[test]
    fn merge_sparse_any_split() {
        for split in [true, false] {
            let opts = FlatReadOptions::from_context(ctx(
                false,
                false,
                PrimaryKeyEncoding::Sparse,
                true,
                split,
            ));
            assert_eq!(
                opts,
                FlatReadOptions {
                    read_raw_tag_columns: false,
                    read_primary_key_column: true,
                    read_sequence_column: true,
                    read_op_type_column: true,
                }
            );
        }
    }

    #[test]
    fn pk_filter_forces_pk_column_on() {
        let mut c = ctx(true, false, PrimaryKeyEncoding::Dense, false, false);
        c.need_pk_filter = true;
        let opts = FlatReadOptions::from_context(c);
        assert!(opts.read_primary_key_column);
        assert!(!opts.read_raw_tag_columns);
    }

    #[test]
    fn tag_filter_forces_tag_column_on_dense() {
        let mut c = ctx(true, false, PrimaryKeyEncoding::Dense, false, false);
        c.filter_columns_have_tag = true;
        let opts = FlatReadOptions::from_context(c);
        assert!(opts.read_raw_tag_columns);
        assert!(opts.read_primary_key_column);
    }

    #[test]
    fn tag_filter_keeps_tag_off_sparse() {
        let mut c = ctx(true, false, PrimaryKeyEncoding::Sparse, false, false);
        c.filter_columns_have_tag = true;
        let opts = FlatReadOptions::from_context(c);
        assert!(!opts.read_raw_tag_columns);
        assert!(opts.read_primary_key_column);
    }

    #[test]
    fn series_row_selector_forces_pk_on() {
        let mut c = ctx(true, false, PrimaryKeyEncoding::Dense, true, false);
        c.series_row_selector = true;
        let opts = FlatReadOptions::from_context(c);
        assert!(opts.read_primary_key_column);
        // Append-mode + dense + tag-projected still keeps raw tag reads on.
        assert!(opts.read_raw_tag_columns);
        // Append-mode trims sequence/op_type even with the selector — the
        // selector groups by pk + ts, not by sequence.
        assert!(!opts.read_sequence_column);
        assert!(!opts.read_op_type_column);
    }

    #[test]
    fn full_context_yields_full_options() {
        let opts = FlatReadOptions::from_context(FlatReadContext::full());
        assert_eq!(opts, FlatReadOptions::full());
    }

    #[test]
    fn full_default_matches_full() {
        assert_eq!(FlatReadOptions::default(), FlatReadOptions::full());
    }

    #[test]
    fn has_mocked_columns_only_when_some_off() {
        assert!(!FlatReadOptions::full().has_mocked_columns());
        let opts = FlatReadOptions::from_context(ctx(
            true,
            false,
            PrimaryKeyEncoding::Dense,
            false,
            false,
        ));
        assert!(opts.has_mocked_columns());
    }
}
