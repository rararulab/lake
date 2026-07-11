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

//! `lake selftest` — the end-to-end path in one command:
//! create table → ingest a batch → SQL query, then assert the result.

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Float64Array, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    },
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use lake_common::{AppendOperation, AppendOperationId, AppendPayloadDigest, TableRef, TenantId};
use lake_query::QueryEngine;

use super::Context;

pub async fn run(ctx: &Context) -> anyhow::Result<()> {
    let table = TableRef::new("robots", "episodes");
    let schema = Arc::new(Schema::new(vec![
        Field::new("robot_id", DataType::Utf8, false),
        Field::new("episode", DataType::Int64, false),
        Field::new("reward", DataType::Float64, false),
    ]));

    // 1. Create the table (idempotent for repeat runs: ignore "exists").
    let location = ctx.location(&table);
    if ctx
        .metasrv
        .create_table(&table, location, schema.clone())
        .await
        .is_ok()
    {
        println!("created table {table}");
    } else {
        println!("table {table} already exists — appending");
    }

    // 2. Ingest a batch of episodes through the commit protocol.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alpha", "alpha", "beta"])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(Float64Array::from(vec![0.9, 0.7, 0.4])),
        ],
    )?;
    let stream: SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(schema, futures_iter(batch)));
    let operation = AppendOperation::builder()
        .tenant(TenantId::try_new("development")?)
        .operation_id(AppendOperationId::generate())
        .payload_digest(
            AppendPayloadDigest::parse(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .expect("valid selftest digest"),
        )
        .build();
    let version = ctx.metasrv.append(&table, &operation, stream).await?;
    println!("committed {table} at {version}");

    // 3. Query it back with plain SQL.
    let engine = QueryEngine::new(ctx.meta.clone(), ctx.engine.clone());
    let batches = engine
        .execute_sql(
            "SELECT robot_id, count(*) AS episodes, avg(reward) AS avg_reward FROM \
             lake.robots.episodes GROUP BY robot_id ORDER BY robot_id",
        )
        .await?;
    println!(
        "{}",
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)?
    );

    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    anyhow::ensure!(rows >= 2, "expected at least one row per robot, got {rows}");
    println!("self-check ok");
    Ok(())
}

/// Wrap a single batch as the `Result` stream item type DataFusion expects.
fn futures_iter(
    batch: RecordBatch,
) -> futures::stream::Iter<std::vec::IntoIter<Result<RecordBatch, DataFusionError>>> {
    futures::stream::iter(vec![Ok(batch)])
}
