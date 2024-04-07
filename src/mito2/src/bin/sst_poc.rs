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

//! POC of the SST format.

use clap::Parser;
use mito2::sst::parquet::parallel_scan;
use mito2::sst::rewrite::{rewrite_file, split_key};
use mito2::sst::split_kv::{
    create_data_file, create_mark_file, create_pk_file, scan_dir, scan_file,
};
use object_store::services::Fs;
use object_store::ObjectStore;

#[derive(Parser)]
#[command(name = "sst-poc")]
#[command(bin_name = "sst-poc")]
enum PocCli {
    /// Creates a primary key file.
    CreatePk(CreatePkArgs),
    /// Creates a data file.
    CreateData(CreateArgs),
    /// Creates a mark file.
    CreateMark(CreateArgs),
    /// Scans a file.
    Scan(ScanArgs),
    /// Splits primary key in a SST file.
    SplitKey(CreateArgs),
    /// Rewrites a SST file.
    Rewrite(RewriteArgs),
}

#[derive(Debug, clap::Args)]
#[command(author, version, about, long_about = None)]
struct CreatePkArgs {
    /// Input directory.
    #[arg(short, long)]
    input_dir: String,
    /// File id of the file under input directory.
    #[arg(long)]
    file_id: String,
    /// Output file path.
    #[arg(short, long)]
    output_path: String,
    /// Add tags to the primary key file.
    #[arg(long, default_value_t = false)]
    with_tags: bool,
}

#[derive(Debug, clap::Args)]
#[command(author, version, about, long_about = None)]
struct CreateArgs {
    /// Input directory.
    #[arg(short, long)]
    input_dir: String,
    /// File id of the file under input directory.
    #[arg(long)]
    file_id: String,
    /// Output file path.
    #[arg(short, long)]
    output_path: String,
}

#[derive(Debug, clap::Args)]
#[command(author, version, about, long_about = None)]
struct ScanArgs {
    /// Input file path.
    #[arg(short, long)]
    input_path: String,
    /// Path to the input file.
    #[arg(short, long)]
    file: String,
    /// Path to the input directory.
    #[arg(short, long)]
    directory: String,
    /// Scan times.
    #[arg(short, long, default_value_t = 1)]
    times: usize,
    /// Use row group level scan.
    #[arg(long, default_value_t = false)]
    row_group_parallel: bool,
    /// Parallelism to scan the file. Only takes effect when row_group_parallel
    /// is true.
    #[arg(short, long, default_value_t = 0)]
    jobs: usize,
    /// Channel size.
    #[arg(long, default_value_t = 0)]
    channel_size: usize,
}

#[derive(Debug, clap::Args)]
#[command(author, version, about, long_about = None)]
struct RewriteArgs {
    /// Input path.
    #[arg(short, long)]
    input_path: String,
    /// Output file path.
    #[arg(short, long)]
    output_path: String,
    /// Use dictionary type to store tags.
    #[arg(long, default_value_t = false)]
    tag_use_dictionary: bool,
}

fn new_fs_store() -> ObjectStore {
    let mut builder = Fs::default();
    builder.root("/");
    ObjectStore::new(builder).unwrap().finish()
}

#[tokio::main]
async fn main() {
    let cli = PocCli::parse();
    match cli {
        PocCli::CreatePk(args) => run_create_pk(args).await,
        PocCli::CreateData(args) => run_create_data(args).await,
        PocCli::CreateMark(args) => run_create_mark(args).await,
        PocCli::Scan(args) => run_scan(args).await,
        PocCli::SplitKey(args) => run_split_key(args).await,
        PocCli::Rewrite(args) => run_rewrite(args).await,
    }
}

async fn run_create_pk(args: CreatePkArgs) {
    println!("Create pk, args: {args:?}");

    if args.file_id.is_empty() {
        println!("File id is empty");
        return;
    }

    let store = new_fs_store();
    match create_pk_file(
        &args.input_dir,
        &args.file_id,
        &args.output_path,
        &store,
        args.with_tags,
    )
    .await
    {
        Ok(metrics) => {
            println!("Created pk file, metrics: {:?}", metrics);
        }
        Err(e) => {
            println!("Failed to create pk file, {e:?}");
        }
    }
}

async fn run_create_data(args: CreateArgs) {
    println!("Create mark, args: {args:?}");

    if args.file_id.is_empty() {
        println!("File id is empty");
        return;
    }

    let store = new_fs_store();
    match create_data_file(&args.input_dir, &args.file_id, &args.output_path, &store).await {
        Ok(metrics) => {
            println!("Created data file, metrics: {:?}", metrics);
        }
        Err(e) => {
            println!("Failed to create data file, {e:?}");
        }
    }
}

async fn run_create_mark(args: CreateArgs) {
    println!("Create mark, args: {args:?}");

    if args.file_id.is_empty() {
        println!("File id is empty");
        return;
    }

    let store = new_fs_store();
    match create_mark_file(&args.input_dir, &args.file_id, &args.output_path, &store).await {
        Ok(metrics) => {
            println!("Created mark file, metrics: {:?}", metrics);
        }
        Err(e) => {
            println!("Failed to create mark file, {e:?}");
        }
    }
}

async fn run_scan(args: ScanArgs) {
    println!("Scan, args: {args:?}");

    if args.file.is_empty() && args.directory.is_empty() {
        println!("File path is empty");
        return;
    }
    if !args.file.is_empty() && !args.directory.is_empty() {
        println!("Only specify one of file and directory");
        return;
    }
    let channel_size = if args.channel_size == 0 {
        None
    } else {
        Some(args.channel_size)
    };

    let store = new_fs_store();
    if args.row_group_parallel {
        if !args.file.is_empty() {
            for _ in 0..args.times {
                match parallel_scan::parallel_scan_file(&args.file, &store, args.jobs, channel_size)
                    .await
                {
                    Ok(metrics) => {
                        println!("Scan metrics: {:?}", metrics);
                    }
                    Err(e) => {
                        println!("Failed to scan file, {e:?}");
                        return;
                    }
                }
            }
        } else {
            for _ in 0..args.times {
                match parallel_scan::parallel_scan_dir(&args.directory, &store, args.jobs).await {
                    Ok(metrics) => {
                        println!("Scan metrics: {:?}", metrics);
                    }
                    Err(e) => {
                        println!("Failed to scan directory, {e:?}");
                        return;
                    }
                }
            }
        }

        return;
    }

    if !args.file.is_empty() {
        for _ in 0..args.times {
            match scan_file(&args.file, &store).await {
                Ok(metrics) => {
                    println!("Scan metrics: {:?}", metrics);
                }
                Err(e) => {
                    println!("Failed to scan file, {e:?}");
                    return;
                }
            }
        }
    } else {
        for _ in 0..args.times {
            match scan_dir(&args.directory, &store, args.jobs, channel_size).await {
                Ok(metrics) => {
                    println!("Scan metrics: {:?}", metrics);
                }
                Err(e) => {
                    println!("Failed to scan directory, {e:?}");
                    return;
                }
            }
        }
    }
}

async fn run_split_key(args: CreateArgs) {
    println!("Split key, args: {args:?}");

    if args.file_id.is_empty() {
        println!("File id is empty");
        return;
    }

    let store = new_fs_store();
    match split_key(&args.input_dir, &args.file_id, &args.output_path, &store).await {
        Ok(metrics) => {
            println!("Split key, metrics: {:?}", metrics);
        }
        Err(e) => {
            println!("Failed to split key, {e:?}");
        }
    }
}

async fn run_rewrite(args: RewriteArgs) {
    println!("Rewrite file, args: {args:?}");

    let store = new_fs_store();
    match rewrite_file(
        &args.input_path,
        &args.output_path,
        args.tag_use_dictionary,
        &store,
    )
    .await
    {
        Ok(metrics) => {
            println!("Rewrite file, metrics: {:?}", metrics);
        }
        Err(e) => {
            println!("Failed to write file, {e:?}");
        }
    }
}
