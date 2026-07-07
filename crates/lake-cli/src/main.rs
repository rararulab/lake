// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The all-in-one `lake` binary. Thin entry point: parse args, build the
//! shared context, dispatch to a command module. Command logic lives in
//! `commands/`, not here.

// CLI binary: stdout is the output channel.
#![allow(clippy::print_stdout)]

mod commands;

use clap::{Parser, Subcommand};

/// lake — a lakehouse for embodied-AI data.
#[derive(Parser)]
#[command(name = "lake", version, about)]
struct Cli {
    /// Root directory for the dev metastore and table data.
    #[arg(long, default_value = "./data", global = true)]
    data_dir: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the end-to-end self-check: create → ingest → SQL query.
    Selftest,
    /// Execute a SQL statement against the catalog.
    Sql {
        /// The SQL to run, e.g. `SELECT * FROM robots.arm`.
        query: String,
    },
    /// Load a Parquet file into a table (creating it if absent).
    Ingest {
        /// `<namespace>.<name>`, e.g. `robots.arm_left`.
        table: String,
        /// Path to the Parquet file to load.
        file:  String,
    },
    /// Table administration.
    #[command(subcommand)]
    Table(commands::table::TableCmd),
    /// Run the stateless query-layer server.
    Query {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
    /// Run the stateful metadata-layer server.
    Meta {
        #[arg(long, default_value = "127.0.0.1:50052")]
        addr: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let ctx = commands::Context::open(&cli.data_dir).await?;

    match cli.command {
        Command::Selftest => commands::selftest::run(&ctx).await,
        Command::Sql { query } => commands::sql::run(&ctx, &query).await,
        Command::Ingest { table, file } => commands::ingest::run(&ctx, &table, &file).await,
        Command::Table(cmd) => commands::table::run(&ctx, cmd).await,
        Command::Query { addr } => commands::serve::query(&ctx, &addr).await,
        Command::Meta { addr } => commands::serve::meta(&ctx, &addr).await,
    }
}
