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

//! `lake ingest` — load a Parquet file into a table.

use std::sync::Arc;

use anyhow::Context as _;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use super::Context;
use crate::commands::table::parse_table_ref;

pub async fn run(ctx: &Context, table: &str, file: &str) -> anyhow::Result<()> {
    let table = parse_table_ref(table)?;

    // Read the Parquet file into a DataFusion stream.
    let df_ctx = SessionContext::new();
    let df = df_ctx
        .read_parquet(file, ParquetReadOptions::default())
        .await
        .with_context(|| format!("reading parquet {file}"))?;
    let schema = Arc::new(df.schema().as_arrow().clone());

    // Create the table on first ingest, using the file's schema.
    if ctx.metasrv.resolve(&table).await?.is_none() {
        ctx.metasrv
            .create_table(&table, ctx.location(&table), schema)
            .await
            .with_context(|| format!("creating {table}"))?;
        println!("created table {table}");
    }

    let stream = df
        .execute_stream()
        .await
        .context("executing parquet scan")?;
    let version = ctx
        .metasrv
        .append(&table, stream)
        .await
        .with_context(|| format!("appending to {table}"))?;
    println!("ingested {file} into {table} at {version}");
    Ok(())
}
