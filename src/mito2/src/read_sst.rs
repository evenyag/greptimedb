use std::sync::Arc;
use std::time::Instant;

use common_time::Timestamp;
use mito2::read::BatchReader;
use mito2::sst::file::{FileHandle, FileId, FileMeta, Level};
use mito2::sst::file_purger::{FilePurger, FilePurgerRef, PurgeRequest};
use mito2::sst::parquet::reader::ParquetReaderBuilder;
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::region_request::PathType;

fn new_fs_store(path: &str) -> ObjectStore {
    let builder = Fs::default();
    ObjectStore::new(builder.root(path)).unwrap().finish()
}

#[derive(Debug)]
struct NoopFilePurger;

impl FilePurger for NoopFilePurger {
    fn send_request(&self, _request: PurgeRequest) {}
}

fn new_noop_file_purger() -> FilePurgerRef {
    Arc::new(NoopFilePurger {})
}

/// Test util to create file handles.
fn new_file_handle(
    file_id: FileId,
    start_ts_millis: i64,
    end_ts_millis: i64,
    level: Level,
) -> FileHandle {
    let file_purger = new_noop_file_purger();
    FileHandle::new(
        FileMeta {
            region_id: 0.into(),
            file_id,
            time_range: (
                Timestamp::new_millisecond(start_ts_millis),
                Timestamp::new_millisecond(end_ts_millis),
            ),
            level,
            file_size: 0,
            available_indexes: Default::default(),
            index_file_size: 0,
            num_rows: 0,
            num_row_groups: 0,
            sequence: None,
        },
        file_purger,
    )
}

#[tokio::main]
async fn main() {
    common_telemetry::init_default_ut_logging();

    let file_dir = std::env::var("FILE_DIR").unwrap();
    let file_id = std::env::var("FILE_ID").unwrap();
    let file_root = std::env::var("FILE_ROOT").unwrap();
    let loops = std::env::var("LOOPS")
        .ok()
        .and_then(|x| x.parse::<usize>().ok())
        .unwrap_or(1);

    let file_handle = new_file_handle(FileId::parse_str(&file_id).unwrap(), 0, 1000, 1);
    let object_store = new_fs_store(&file_root);
    for i in 0..loops {
        let start = Instant::now();
        let mut reader = ParquetReaderBuilder::new(
            file_dir.clone(),
            PathType::Bare,
            file_handle.clone(),
            object_store.clone(),
        )
        .build()
        .await
        .unwrap();
        common_telemetry::info!("loop: {}, build reader cost is {:?}", i, start.elapsed());
        let mut total_batches = 0;
        let mut total_rows = 0;
        while let Some(batch) = reader.next_batch().await.unwrap() {
            total_batches += 1;
            total_rows += batch.num_rows();
        }
        common_telemetry::info!(
            "loop: {}, scan reader done, {} batches, {} rows, total cost {:?}",
            i,
            total_batches,
            total_rows,
            start.elapsed()
        );
    }
}
