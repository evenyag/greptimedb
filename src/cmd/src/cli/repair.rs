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

use async_trait::async_trait;
use clap::Parser;
use common_telemetry::logging::LoggingOptions;
use repairer::manifest::{RebuildCatalog, RebuildDb, RebuildRegion, RebuildSchema};
use repairer::Repairer;
use snafu::ResultExt;
use table::metadata::TableId;

use crate::cli::{Instance, Tool};
use crate::error::{RepairerSnafu, Result};
use crate::options::{Options, RepairOptions};

#[derive(Debug, Parser)]
pub struct RepairCommand {
    #[clap(long, default_value_t = false)]
    dry_run: bool,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(long, default_value = "GREPTIMEDB_CLI")]
    env_prefix: String,
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl RepairCommand {
    pub fn load_options(&self, logging_opts: LoggingOptions) -> Result<Options> {
        let mut opts: RepairOptions =
            Options::load_layered_options(self.config_file.as_deref(), &self.env_prefix, None)?;
        opts.logging = logging_opts;

        Ok(Options::Cli(Box::new(crate::options::CliOptions::Repair(
            opts,
        ))))
    }

    pub async fn build(&self, opts: RepairOptions) -> Result<Instance> {
        self.cmd.build(opts).await
    }
}

#[derive(Debug, Clone, Parser)]
enum SubCommand {
    Db(RepairDb),
    Catalog(RepairDir),
    Schema(RepairSchema),
    Region(RepairDir),
}

impl SubCommand {
    pub async fn build(&self, opts: RepairOptions) -> Result<Instance> {
        let repairer = Repairer::new(opts.storage).await.context(RepairerSnafu)?;

        Ok(Instance::Tool(Box::new(RepairTool {
            repairer,
            cmd: self.clone(),
        })))
    }
}

#[derive(Debug, Clone, Parser)]
struct RepairDb {
    #[clap(long)]
    dir: String,
    #[clap(long)]
    start_catalog: Option<String>,
}

#[derive(Debug, Clone, Parser)]
struct RepairDir {
    #[clap(long)]
    dir: String,
}

#[derive(Debug, Clone, Parser)]
struct RepairSchema {
    #[clap(long)]
    dir: String,
    #[clap(long)]
    start_table_id: Option<TableId>,
}

struct RepairTool {
    repairer: Repairer,
    cmd: SubCommand,
}

#[async_trait]
impl Tool for RepairTool {
    async fn do_work(&self) -> Result<()> {
        match &self.cmd {
            SubCommand::Db(cmd) => {
                let req = RebuildDb {
                    db_dir: cmd.dir.clone(),
                    start_catalog: cmd.start_catalog.clone(),
                };
                self.repairer
                    .repair_db_manifest(req)
                    .await
                    .context(RepairerSnafu)?;
            }
            SubCommand::Catalog(cmd) => {
                let req = RebuildCatalog {
                    catalog_dir: cmd.dir.clone(),
                };
                self.repairer
                    .repair_catalog_manifest(req)
                    .await
                    .context(RepairerSnafu)?;
            }
            SubCommand::Schema(cmd) => {
                let req = RebuildSchema {
                    schema_dir: cmd.dir.clone(),
                    start_table_id: cmd.start_table_id,
                };
                self.repairer
                    .repair_schema_manifest(req)
                    .await
                    .context(RepairerSnafu)?;
            }
            SubCommand::Region(cmd) => {
                let req = RebuildRegion {
                    region_dir: cmd.dir.clone(),
                };
                self.repairer
                    .repair_region_manifest(req)
                    .await
                    .context(RepairerSnafu)?;
            }
        }

        Ok(())
    }
}
