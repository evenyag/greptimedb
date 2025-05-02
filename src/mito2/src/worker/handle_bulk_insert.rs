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

//! Handles bulk insert requests.

use std::collections::HashMap;
use std::sync::Arc;

use api::helper::{value_to_grpc_value, ColumnDataTypeWrapper};
use api::v1::{ColumnSchema, OpType, Row, Rows};
use common_base::AffectedRows;
use common_recordbatch::DfRecordBatch;
use datatypes::arrow;
use datatypes::arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use datatypes::prelude::VectorRef;
use datatypes::vectors::Helper;
use futures::channel::oneshot::Receiver;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::RegionBulkInsertsRequest;

use crate::config::MitoConfig;
use crate::error::{self, RegionNotFoundSnafu, RegionStateSnafu};
use crate::flush::{FlushReason, RegionFlushTask};
use crate::memtable::bulk::part::BulkPart;
use crate::metrics;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{OptionOutputTx, SenderBulkRequest, SenderWriteRequest, WriteRequest};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_bulk_insert_batch(
        &mut self,
        region_metadata: RegionMetadataRef,
        request: RegionBulkInsertsRequest,
        pending_bulk_request: &mut Vec<SenderBulkRequest>,
        sender: OptionOutputTx,
    ) {
        if self.config.enable_plain_format {
            self.handle_bulk_inserts_plain(request, sender).await;
            return;
        }

        let _timer = metrics::REGION_WORKER_HANDLE_WRITE_ELAPSED
            .with_label_values(&["process_bulk_req"])
            .start_timer();
        let batch = request.payload;
        let Some((ts_index, ts)) = batch
            .schema()
            .column_with_name(&region_metadata.time_index_column().column_schema.name)
            .map(|(index, _)| (index, batch.column(index)))
        else {
            sender.send(
                error::InvalidRequestSnafu {
                    region_id: region_metadata.region_id,
                    reason: format!(
                        "timestamp column `{}` not found",
                        region_metadata.time_index_column().column_schema.name
                    ),
                }
                .fail(),
            );
            return;
        };

        let DataType::Timestamp(unit, _) = ts.data_type() else {
            // safety: ts data type must be a timestamp type.
            unreachable!()
        };

        let (min_ts, max_ts) = match unit {
            TimeUnit::Second => {
                let ts = ts.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                (
                    //safety: ts array must contain at least one row so this won't return None.
                    arrow::compute::min(ts).unwrap(),
                    arrow::compute::max(ts).unwrap(),
                )
            }

            TimeUnit::Millisecond => {
                let ts = ts
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                (
                    //safety: ts array must contain at least one row so this won't return None.
                    arrow::compute::min(ts).unwrap(),
                    arrow::compute::max(ts).unwrap(),
                )
            }
            TimeUnit::Microsecond => {
                let ts = ts
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                (
                    //safety: ts array must contain at least one row so this won't return None.
                    arrow::compute::min(ts).unwrap(),
                    arrow::compute::max(ts).unwrap(),
                )
            }
            TimeUnit::Nanosecond => {
                let ts = ts
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                (
                    //safety: ts array must contain at least one row so this won't return None.
                    arrow::compute::min(ts).unwrap(),
                    arrow::compute::max(ts).unwrap(),
                )
            }
        };

        let part = BulkPart {
            batch,
            max_ts,
            min_ts,
            sequence: 0,
            timestamp_index: ts_index,
            raw_data: Some(request.raw_data),
        };
        pending_bulk_request.push(SenderBulkRequest {
            sender,
            request: part,
            region_id: request.region_id,
            region_metadata,
        });
    }

    fn handle_payload(
        region_metadata: &RegionMetadataRef,
        payload: DfRecordBatch,
        pending_write_requests: &mut Vec<SenderWriteRequest>,
        column_schemas: Vec<ColumnSchema>,
        name_to_index: HashMap<String, usize>,
    ) -> error::Result<Receiver<error::Result<AffectedRows>>> {
        let rx = Self::handle_arrow_ipc(
            region_metadata,
            payload,
            pending_write_requests,
            column_schemas,
            name_to_index,
        )?;

        Ok(rx)
    }

    fn handle_arrow_ipc(
        region_metadata: &RegionMetadataRef,
        df_record_batch: DfRecordBatch,
        pending_write_requests: &mut Vec<SenderWriteRequest>,
        column_schemas: Vec<ColumnSchema>,
        name_to_index: HashMap<String, usize>,
    ) -> error::Result<Receiver<error::Result<AffectedRows>>> {
        let has_null: Vec<_> = df_record_batch
            .columns()
            .iter()
            .map(|c| c.null_count() > 0)
            .collect();

        let rows = record_batch_to_rows(region_metadata, &df_record_batch)?;

        let write_request = WriteRequest {
            region_id: region_metadata.region_id,
            op_type: OpType::Put,
            rows: Rows {
                schema: column_schemas,
                rows,
            },
            name_to_index,
            has_null,
            hint: None,
            region_metadata: Some(region_metadata.clone()),
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        let sender = OptionOutputTx::from(tx);
        let req = SenderWriteRequest {
            sender,
            request: write_request,
        };
        pending_write_requests.push(req);
        Ok(rx)
    }

    pub(crate) async fn handle_bulk_inserts_plain(
        &mut self,
        request: RegionBulkInsertsRequest,
        mut sender: OptionOutputTx,
    ) {
        let region_id = request.region_id;
        let Some(region) = self.regions.get_region_or(region_id, &mut sender) else {
            // No such region.
            sender.send(RegionNotFoundSnafu { region_id }.fail());
            return;
        };
        match region.state() {
            RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Altering) => (),
            state => {
                // The region is not writable.
                sender.send(
                    RegionStateSnafu {
                        region_id,
                        state,
                        expect: RegionRoleState::Leader(RegionLeaderState::Writable),
                    }
                    .fail(),
                );
                return;
            }
        }

        let mut task =
            self.new_bulk_insert_flush_task(&region, self.config.clone(), vec![request.payload]);
        task.push_sender(sender);
        if let Err(e) =
            self.flush_scheduler
                .schedule_flush(region.region_id, &region.version_control, task)
        {
            common_telemetry::error!(e; "Failed to schedule bulk insert flush task for region {}", region.region_id);
        }
    }

    /// Creates a flush task to bulk insert to the `region`.
    pub(crate) fn new_bulk_insert_flush_task(
        &mut self,
        region: &MitoRegionRef,
        engine_config: Arc<MitoConfig>,
        payloads: Vec<DfRecordBatch>,
    ) -> RegionFlushTask {
        RegionFlushTask {
            region_id: region.region_id,
            reason: FlushReason::BulkInsert,
            senders: Vec::new(),
            request_sender: self.sender.clone(),
            access_layer: region.access_layer.clone(),
            listener: self.listener.clone(),
            engine_config,
            row_group_size: None,
            cache_manager: self.cache_manager.clone(),
            manifest_ctx: region.manifest_ctx.clone(),
            index_options: region.version().options.index_options.clone(),
            bulk_insert_payloads: payloads,
        }
    }
}

// fn region_metadata_to_column_schema(
//     region_meta: &RegionMetadataRef,
// ) -> error::Result<(Vec<ColumnSchema>, HashMap<String, usize>)> {
//     let mut column_schemas = Vec::with_capacity(region_meta.column_metadatas.len());
//     let mut name_to_index = HashMap::with_capacity(region_meta.column_metadatas.len());

//     for (idx, c) in region_meta.column_metadatas.iter().enumerate() {
//         let wrapper = ColumnDataTypeWrapper::try_from(c.column_schema.data_type.clone())
//             .with_context(|_| error::ConvertDataTypeSnafu {
//                 data_type: c.column_schema.data_type.clone(),
//             })?;
//         column_schemas.push(ColumnSchema {
//             column_name: c.column_schema.name.clone(),
//             datatype: wrapper.datatype() as i32,
//             semantic_type: c.semantic_type as i32,
//             ..Default::default()
//         });

//         name_to_index.insert(c.column_schema.name.clone(), idx);
//     }

//     Ok((column_schemas, name_to_index))
// }

/// Convert [DfRecordBatch] to gRPC rows.
fn record_batch_to_rows(
    region_metadata: &RegionMetadataRef,
    rb: &DfRecordBatch,
) -> error::Result<Vec<Row>> {
    let num_rows = rb.num_rows();
    let mut rows = Vec::with_capacity(num_rows);
    if num_rows == 0 {
        return Ok(rows);
    }
    let vectors: Vec<Option<VectorRef>> = region_metadata
        .column_metadatas
        .iter()
        .map(|c| {
            rb.column_by_name(&c.column_schema.name)
                .map(|column| Helper::try_into_vector(column).context(error::ConvertVectorSnafu))
                .transpose()
        })
        .collect::<error::Result<_>>()?;

    for row_idx in 0..num_rows {
        let row = Row {
            values: row_at(&vectors, row_idx),
        };
        rows.push(row);
    }
    Ok(rows)
}

fn row_at(vectors: &[Option<VectorRef>], row_idx: usize) -> Vec<api::v1::Value> {
    let mut row = Vec::with_capacity(vectors.len());
    for a in vectors {
        let value = if let Some(a) = a {
            value_to_grpc_value(a.get(row_idx))
        } else {
            api::v1::Value { value_data: None }
        };
        row.push(value)
    }
    row
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray};

    use super::*;
    use crate::test_util::meta_util::TestRegionMetadataBuilder;

    fn build_record_batch(num_rows: usize) -> DfRecordBatch {
        let region_metadata = Arc::new(TestRegionMetadataBuilder::default().build());
        let schema = region_metadata.schema.arrow_schema().clone();
        let values = (0..num_rows).map(|v| v as i64).collect::<Vec<_>>();
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter_values(values.clone()));
        let k0_array = Arc::new(Int64Array::from_iter_values(values.clone()));
        let v0_array = Arc::new(Int64Array::from_iter_values(values));
        DfRecordBatch::try_new(schema, vec![ts_array, k0_array, v0_array]).unwrap()
    }

    #[test]
    fn test_region_metadata_to_column_schema() {
        let region_metadata = Arc::new(TestRegionMetadataBuilder::default().build());
        let (result, _) = region_metadata_to_column_schema(&region_metadata).unwrap();
        assert_eq!(result.len(), 3);

        assert_eq!(result[0].column_name, "ts");
        assert_eq!(result[0].semantic_type, SemanticType::Timestamp as i32);

        assert_eq!(result[1].column_name, "k0");
        assert_eq!(result[1].semantic_type, SemanticType::Tag as i32);

        assert_eq!(result[2].column_name, "v0");
        assert_eq!(result[2].semantic_type, SemanticType::Field as i32);
    }

    #[test]
    fn test_record_batch_to_rows() {
        // Create record batch
        let region_metadata = Arc::new(TestRegionMetadataBuilder::default().build());
        let record_batch = build_record_batch(10);
        let rows = record_batch_to_rows(&region_metadata, &record_batch).unwrap();

        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0].values.len(), 3);

        for (row_idx, row) in rows.iter().enumerate().take(10) {
            assert_eq!(
                row.values[0].value_data.as_ref().unwrap(),
                &api::v1::value::ValueData::TimestampMillisecondValue(row_idx as i64)
            );
        }
    }

    #[test]
    fn test_record_batch_to_rows_schema_mismatch() {
        let region_metadata = Arc::new(TestRegionMetadataBuilder::default().num_fields(2).build());
        let record_batch = build_record_batch(1);

        let rows = record_batch_to_rows(&region_metadata, &record_batch).unwrap();
        assert_eq!(rows.len(), 1);

        // Check first row
        let row1 = &rows[0];
        assert_eq!(row1.values.len(), 4);
        assert_eq!(
            row1.values[0].value_data.as_ref().unwrap(),
            &api::v1::value::ValueData::TimestampMillisecondValue(0)
        );
        assert_eq!(
            row1.values[1].value_data.as_ref().unwrap(),
            &api::v1::value::ValueData::I64Value(0)
        );
        assert_eq!(
            row1.values[2].value_data.as_ref().unwrap(),
            &api::v1::value::ValueData::I64Value(0)
        );

        assert!(row1.values[3].value_data.is_none());
    }
}
