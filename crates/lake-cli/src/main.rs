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
//! command-specific context, dispatch to a command module. Command logic lives
//! in `commands/`, not here.

// CLI binary: stdout is the output channel.
#![allow(clippy::print_stdout)]

mod commands;
mod metrics;
mod observability;

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
    /// Activate generation-based catalog refresh after a quiescent rollout.
    CatalogFinalize(commands::catalog_finalize::CatalogFinalizeCmd),
    /// Backfill and optionally finalize DynamoDB's prefix-isolated layout.
    DynamoMigrate(commands::dynamo_migrate::DynamoMigrateCmd),
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
    /// Plan or apply managed-object garbage collection.
    Gc(commands::gc::GcCmd),
    /// Run the stateless query-layer server.
    Query {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr:          String,
        /// Metadata Flight endpoint used for leader-aware FILE appends.
        #[arg(long, default_value = "http://127.0.0.1:50052")]
        metadata_addr: String,
    },
    /// Run the stateful metadata-layer server.
    Meta {
        #[arg(long, default_value = "127.0.0.1:50052")]
        addr: String,
    },
    /// Talk to a running metadata-layer server over Flight (network client).
    Client {
        /// The metasrv Flight address to connect to.
        #[arg(long, default_value = "127.0.0.1:50052")]
        addr:    String,
        #[command(subcommand)]
        command: commands::client::ClientCmd,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    observability::run_process(observability::init_from_env, async {
        let Cli { data_dir, command } = Cli::parse();
        match command {
            // A pure network client: it holds no local storage, so it must not
            // build a Context (which would require a data-dir or S3 config).
            Command::Client { addr, command } => commands::client::run(&addr, command).await,
            Command::DynamoMigrate(command) => commands::dynamo_migrate::run(command).await,
            Command::Query {
                addr,
                metadata_addr,
            } => run_query(&data_dir, &addr, &metadata_addr).await,
            command => run_with_context(&data_dir, command).await,
        }
    })
    .await
}

/// Run stateless Query without constructing the catalog/admin context.
async fn run_query(data_dir: &str, addr: &str, metadata_addr: &str) -> anyhow::Result<()> {
    let ctx = commands::QueryContext::open(data_dir).await?;
    commands::serve::query(&ctx, addr, metadata_addr).await
}

/// Run a command that needs the in-process tiers wired from `--data-dir`.
async fn run_with_context(data_dir: &str, command: Command) -> anyhow::Result<()> {
    let ctx = commands::Context::open(data_dir).await?;
    match command {
        Command::Selftest => commands::selftest::run(&ctx).await,
        Command::Sql { query } => commands::sql::run(&ctx, &query).await,
        Command::Ingest { table, file } => commands::ingest::run(&ctx, &table, &file).await,
        Command::Table(cmd) => commands::table::run(&ctx, cmd).await,
        Command::Gc(cmd) => commands::gc::run(&ctx, cmd).await,
        Command::CatalogFinalize(command) => commands::catalog_finalize::run(&ctx, command).await,
        Command::DynamoMigrate(_) => {
            unreachable!("DynamoMigrate is dispatched before Context::open")
        }
        Command::Query { .. } => unreachable!("Query is dispatched before Context::open"),
        Command::Meta { addr } => commands::serve::meta(&ctx, &addr).await,
        Command::Client { .. } => unreachable!("Client is dispatched before Context::open"),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command};
    use crate::commands::client::ClientCmd;

    #[test]
    fn remote_create_table_has_no_location_argument() {
        let cli = Cli::try_parse_from([
            "lake",
            "client",
            "create-table",
            "robots.episodes",
            "--column",
            "episode_id:utf8",
        ])
        .expect("remote create-table does not require a storage location");
        let Command::Client { command, .. } = cli.command else {
            panic!("expected client command");
        };
        let ClientCmd::CreateTable { table, columns, .. } = command else {
            panic!("expected create-table command");
        };
        assert_eq!(table, "robots.episodes");
        assert_eq!(columns, ["episode_id:utf8"]);

        assert!(
            Cli::try_parse_from([
                "lake",
                "client",
                "create-table",
                "robots.episodes",
                "--column",
                "episode_id:utf8",
                "--location",
                "s3://caller/selected.lance",
            ])
            .is_err(),
            "legacy caller-selected placement must be rejected"
        );
    }

    #[test]
    fn query_accepts_metadata_address() {
        let cli = Cli::try_parse_from([
            "lake",
            "query",
            "--addr",
            "127.0.0.1:60051",
            "--metadata-addr",
            "http://meta.internal:60052",
        ])
        .unwrap();

        let Command::Query {
            addr,
            metadata_addr,
        } = cli.command
        else {
            panic!("expected query command");
        };
        assert_eq!(addr, "127.0.0.1:60051");
        assert_eq!(metadata_addr, "http://meta.internal:60052");
    }

    #[test]
    fn gc_command_is_dry_run_unless_apply_is_explicit() {
        let dry_run = Cli::try_parse_from([
            "lake",
            "gc",
            "--plan",
            "gc-plan",
            "--safety-age-secs",
            "3600",
        ])
        .unwrap();
        let Command::Gc(dry_run) = dry_run.command else {
            panic!("expected gc command");
        };
        assert!(!dry_run.apply);
        assert!(dry_run.checkpoint.is_none());

        let apply = Cli::try_parse_from([
            "lake",
            "gc",
            "--plan",
            "gc-plan",
            "--apply",
            "--checkpoint",
            "gc-apply.json",
        ])
        .unwrap();
        let Command::Gc(apply) = apply.command else {
            panic!("expected gc command");
        };
        assert!(apply.apply);
        assert_eq!(
            apply.checkpoint.as_deref(),
            Some(std::path::Path::new("gc-apply.json"))
        );

        assert!(Cli::try_parse_from(["lake", "gc", "--plan", "gc-plan", "--apply"]).is_err());
        assert!(
            Cli::try_parse_from(["lake", "gc", "--plan", "gc-plan", "--safety-age-secs", "0",])
                .is_err()
        );
    }

    #[test]
    fn dynamo_v2_finalize_requires_exact_verified_backfill() {
        let command =
            Cli::try_parse_from(["lake", "dynamo-migrate", "--page-size", "128", "--json"])
                .unwrap();
        let Command::DynamoMigrate(command) = command.command else {
            panic!("expected dynamo-migrate command");
        };
        assert_eq!(command.page_size, 128);
        assert!(!command.finalize);
        assert!(command.json);

        assert!(
            Cli::try_parse_from(["lake", "dynamo-migrate", "--finalize", "--json"]).is_err(),
            "finalization must require explicit rollout and write-quiescence acknowledgements"
        );
        assert!(
            Cli::try_parse_from([
                "lake",
                "dynamo-migrate",
                "--finalize",
                "--acknowledge-dual-rollout",
                "--json",
            ])
            .is_err(),
            "dual rollout acknowledgement alone is insufficient"
        );
        assert!(
            Cli::try_parse_from([
                "lake",
                "dynamo-migrate",
                "--finalize",
                "--acknowledge-dual-rollout",
                "--acknowledge-write-quiescence",
                "--json",
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(["lake", "dynamo-migrate", "--page-size", "0", "--json",]).is_err()
        );
    }

    #[test]
    fn catalog_generation_finalize_requires_rollout_acknowledgements() {
        assert!(Cli::try_parse_from(["lake", "catalog-finalize"]).is_err());
        assert!(
            Cli::try_parse_from(["lake", "catalog-finalize", "--acknowledge-writer-rollout",])
                .is_err()
        );
        assert!(
            Cli::try_parse_from(["lake", "catalog-finalize", "--acknowledge-write-quiescence",])
                .is_err()
        );
        let cli = Cli::try_parse_from([
            "lake",
            "catalog-finalize",
            "--acknowledge-writer-rollout",
            "--acknowledge-write-quiescence",
            "--json",
        ])
        .unwrap();
        let Command::CatalogFinalize(command) = cli.command else {
            panic!("expected catalog-finalize command");
        };
        assert!(command.acknowledge_writer_rollout);
        assert!(command.acknowledge_write_quiescence);
        assert!(command.json);
    }
}
