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
use mito2::sst::split_kv::{create_data_file, create_pk_file};
use object_store::services::Fs;
use object_store::ObjectStore;

#[derive(Parser)]
#[command(name = "sst-poc")]
#[command(bin_name = "sst-poc")]
enum PocCli {
    /// Creates a primary key file.
    CreatePk(CreatePkArgs),
    /// Creates a data file.
    CreateData(CreateDataArgs),
}

#[derive(Debug, clap::Args)]
#[command(author, version, about, long_about = None)]
struct CreatePkArgs {
    #[arg(short, long)]
    input_dir: String,
    #[arg(long)]
    file_id: String,
    #[arg(short, long)]
    output_path: String,
}

#[derive(Debug, clap::Args)]
#[command(author, version, about, long_about = None)]
struct CreateDataArgs {
    #[arg(short, long)]
    input_dir: String,
    #[arg(long)]
    file_id: String,
    #[arg(short, long)]
    output_path: String,
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
    }
}

async fn run_create_pk(args: CreatePkArgs) {
    println!("Create pk, args: {args:?}");

    if args.file_id.is_empty() {
        println!("File id is empty");
        return;
    }

    let store = new_fs_store();
    match create_pk_file(&args.input_dir, &args.file_id, &args.output_path, &store).await {
        Ok(metrics) => {
            println!("Created pk file, metrics: {:?}", metrics);
        }
        Err(e) => {
            println!("Failed to create pk file, {e:?}");
        }
    }
}

async fn run_create_data(args: CreateDataArgs) {
    println!("Create pk, args: {args:?}");

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
