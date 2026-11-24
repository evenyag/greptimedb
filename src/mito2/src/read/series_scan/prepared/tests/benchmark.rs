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

//! Run each ignored case in a separate process to isolate peak RSS.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::*;
use object_store::layers::mock;

struct CountingReader(mock::Reader, Arc<AtomicU64>);
struct CountingStream(Box<dyn mock::ReadStreamDyn>, Arc<AtomicU64>);

impl mock::ReadStream for CountingStream {
    async fn read(&mut self) -> mock::Result<mock::Buffer> {
        let bytes = self.0.read_dyn().await?;
        self.1.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

impl mock::Read for CountingReader {
    async fn open(
        &self,
        range: mock::BytesRange,
    ) -> mock::Result<(mock::RpRead, Box<dyn mock::ReadStreamDyn>)> {
        let (response, stream) = self.0.open_dyn(range).await?;
        Ok((response, Box::new(CountingStream(stream, self.1.clone()))))
    }

    async fn read(&self, range: mock::BytesRange) -> mock::Result<(mock::RpRead, mock::Buffer)> {
        let (response, bytes) = self.0.read_dyn(range).await?;
        self.1.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok((response, bytes))
    }
}

#[rstest::rstest]
#[case::full_new(false, 0)]
#[case::delta_new(true, 0)]
#[case::full_typical(false, 95)]
#[case::delta_typical(true, 95)]
#[case::full_unchanged(false, 100)]
#[case::delta_unchanged(true, 100)]
#[tokio::test]
#[ignore = "native index maintenance measurement"]
async fn native_index_cycles(#[case] delta: bool, #[case] unchanged: u64) {
    const TABLES: u32 = 32;
    const SERIES: u64 = 100;
    let reads = Arc::new(AtomicU64::default());
    let writes = Arc::new(Mutex::new(mock::WriterObservations::default()));
    let read_counter = reads.clone();
    let write_counter = writes.clone();
    let layer = mock::MockLayerBuilder::default()
        .reader_factory(Arc::new(move |_, _, inner| {
            Box::new(CountingReader(inner, read_counter.clone())) as mock::Reader
        }))
        .writer_factory(Arc::new(move |_, _, inner| {
            Box::new(mock::ObservedWriter::new(inner, write_counter.clone())) as mock::Writer
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::with_prefix("native-index-cycles")
        .await
        .with_mock_layer(layer);
    let engine = env
        .create_engine(MitoConfig {
            experimental_enable_series_index: true,
            experimental_narthex_series_delta: delta,
            experimental_series_index_maintenance_interval: Duration::from_secs(86400),
            experimental_series_index_bucket_width: Duration::from_secs(60),
            min_compaction_interval: Duration::from_secs(86400),
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let id = store_api::storage::RegionId::new(42, 0);
    let mut create = crate::test_util::CreateRequestBuilder::new()
        .tag_num(8)
        .field_num(1)
        .insert_option("primary_key_encoding", "sparse")
        .insert_option("sst_format", "narthex")
        .insert_option("compaction.twcs.time_window", "1m")
        .build();
    let reserved = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
    create
        .column_metadatas
        .extend(reserved.column_metadatas.into_iter().take(2));
    create
        .primary_key
        .splice(0..0, reserved.primary_key.into_iter().take(2));
    engine
        .handle_request(id, RegionRequest::Create(create))
        .await
        .unwrap();
    let region = engine.get_region(id).unwrap();
    let metadata = region.metadata();
    let mut build_time = Duration::ZERO;
    let mut discovery = Vec::new();
    let mut total_read = 0;
    let mut total_written = 0usize;
    let mut sample_hashes = std::collections::HashMap::new();
    let mut snapshots = crc32fast::Hasher::new();
    // Three overlapping admissions per window, across four windows, also
    // exercise the scheduler's four-artifact series compaction trigger.
    for cycle in 0..12_u64 {
        let timestamp = ((cycle / 3) * 60_000 + cycle % 3) as i64;
        let RegionRequest::Put(mut put) =
            crate::test_util::sparse_metric_put(Vec::new(), timestamp, Some(1.0))
        else {
            unreachable!()
        };
        let template = put.rows.rows.pop().unwrap();
        for table in 0..TABLES {
            for ordinal in 0..SERIES {
                let generation = if ordinal < unchanged { 0 } else { cycle };
                let host = format!("host-{ordinal}-{generation}");
                let tags = if table % 2 == 0 {
                    vec!["a", host.as_str()]
                } else {
                    vec!["a", host.as_str(), "west"]
                };
                let key = new_sparse_primary_key(
                    &tags,
                    &metadata,
                    table + 1,
                    (generation * SERIES + ordinal) / 2,
                );
                let mut row = template.clone();
                row.values[0].value_data = Some(ValueData::BinaryValue(key));
                put.rows.rows.push(row);
            }
        }
        engine
            .handle_request(id, RegionRequest::Put(put))
            .await
            .unwrap();
        flush_region(&engine, id, None).await;
        // Compare the actual part bytes of every snapshot across modes. UUIDs
        // identify independently created files and do not participate in the digest.
        let version = region.version();
        let mut snapshot = Vec::new();
        for file in version.ssts.levels().iter().flat_map(|level| level.files()) {
            let id = file.file_id();
            if let std::collections::hash_map::Entry::Vacant(entry) = sample_hashes.entry(id) {
                let dir = region.access_layer.build_region_dir(file.region_id());
                let mut hash = crc32fast::Hasher::new();
                for part in file
                    .meta_ref()
                    .narthex_objects(&dir)
                    .unwrap()
                    .iter()
                    .filter(|object| object.path.ends_with(".samples"))
                {
                    let bytes = region
                        .access_layer
                        .object_store()
                        .read(&part.path)
                        .await
                        .unwrap()
                        .to_bytes();
                    hash.update(&bytes);
                }
                entry.insert(hash.finalize());
            }
            snapshot.push(sample_hashes[&id]);
        }
        snapshot.sort_unstable();
        for hash in snapshot {
            snapshots.update(&hash.to_le_bytes());
        }
        reads.store(0, Ordering::Relaxed);
        writes.lock().unwrap().write_sizes.clear();
        let started = Instant::now();
        build(&engine, &region).await;
        build_time += started.elapsed();
        total_read += reads.load(Ordering::Relaxed);
        total_written += writes.lock().unwrap().write_sizes.iter().sum::<usize>();
        let scan = scanner(&region);
        let started = Instant::now();
        let source = scan
            .prepare_series(
                &QueryScanContext::default(),
                &ExecutionPlanMetricsSet::new(),
                &labels(metadata.clone()),
            )
            .await
            .unwrap()
            .unwrap();
        discovery.push(started.elapsed().as_micros());
        assert_eq!(
            source.num_series() as u64,
            u64::from(TABLES) * (SERIES + cycle * (SERIES - unchanged))
        );
    }
    discovery.sort_unstable();
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let peak = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .unwrap();
    let digest = snapshots.finalize();
    println!(
        "snapshots={digest:08x} delta={delta} unchanged={unchanged} read_bytes={total_read} written_bytes={total_written} maintenance_ms={} discovery_median_us={} {peak}",
        build_time.as_millis(),
        discovery[discovery.len() / 2]
    );
    engine.stop().await.unwrap();
}
