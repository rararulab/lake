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

//! Explicit, resumable DynamoDB prefix-layout migration.

use anyhow::Context as _;
use clap::Args;
use lake_meta::{DynamoMeta, DynamoMigrationPage, DynamoMigrationVerification};
use serde::Serialize;

#[derive(Args, Clone, Debug)]
pub struct DynamoMigrateCmd {
    /// Maximum legacy items evaluated by this invocation.
    #[arg(long, default_value_t = 500, value_parser = parse_page_size)]
    pub page_size: usize,

    /// Verify exact parity and publish the monotonic v2 authority marker.
    #[arg(
        long,
        requires_all = ["acknowledge_dual_rollout", "acknowledge_write_quiescence"]
    )]
    pub finalize: bool,

    /// Confirm every commit-capable metadata node is running dual-write code.
    #[arg(long)]
    pub acknowledge_dual_rollout: bool,

    /// Confirm write admission is paused until runtime pods restart on v2.
    #[arg(long)]
    pub acknowledge_write_quiescence: bool,

    /// Emit one machine-readable JSON object.
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct MigrationOutput {
    page:         Option<DynamoMigrationPage>,
    verification: Option<DynamoMigrationVerification>,
}

fn parse_page_size(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid page size: {error}"))?;
    if !(1..=10_000).contains(&value) {
        return Err("page size must be within 1..=10000".to_owned());
    }
    Ok(value)
}

pub async fn run(command: DynamoMigrateCmd) -> anyhow::Result<()> {
    let endpoint = std::env::var("LAKE_DYNAMODB_ENDPOINT").ok();
    let table = std::env::var("LAKE_DYNAMODB_TABLE").unwrap_or_else(|_| "lake_registry".to_owned());
    anyhow::ensure!(
        !table.trim().is_empty(),
        "LAKE_DYNAMODB_TABLE must not be empty"
    );
    let meta = DynamoMeta::connect(endpoint.as_deref(), &table)
        .await
        .context("connect DynamoDB migrator")?;
    meta.ensure_table()
        .await
        .context("ensure DynamoDB layouts")?;
    let verification = if command.finalize {
        Some(
            meta.verify_and_finalize_v2(command.page_size)
                .await
                .context("verify and finalize DynamoDB v2 authority")?,
        )
    } else {
        None
    };
    let page = if command.finalize {
        None
    } else {
        Some(
            meta.migrate_v2_page(command.page_size)
                .await
                .context("migrate one DynamoDB page")?,
        )
    };
    let output = MigrationOutput { page, verification };
    if command.json {
        println!("{}", serde_json::to_string(&output)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&output)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_page_size;

    #[test]
    fn migration_page_size_is_finite() {
        assert_eq!(parse_page_size("1").unwrap(), 1);
        assert_eq!(parse_page_size("10000").unwrap(), 10_000);
        assert!(parse_page_size("0").is_err());
        assert!(parse_page_size("10001").is_err());
    }
}
