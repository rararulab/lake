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

//! End-to-end self-check: ingest parquet -> commit manifest -> SQL query.

// CLI binary: stdout is the output channel.
#![allow(clippy::print_stdout)]

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Float64Array, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    },
    parquet::arrow::ArrowWriter,
    prelude::*,
};
use lake_catalog::LakeCatalog;
use lake_meta::{MetaStoreRef, RocksMeta};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let root = std::path::PathBuf::from("./data");
    let table_root = root.join("tables");
    std::fs::create_dir_all(table_root.join("episodes"))?;
    let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.join("meta"))?);

    // 1. Ingest: write a parquet data file (stand-in for robot episode data).
    let schema = Arc::new(Schema::new(vec![
        Field::new("robot_id", DataType::Utf8, false),
        Field::new("episode", DataType::Int64, false),
        Field::new("reward", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alpha", "alpha", "beta"])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(Float64Array::from(vec![0.9, 0.7, 0.4])),
        ],
    )?;
    let file = table_root.join("episodes/part-0.parquet");
    let mut writer = ArrowWriter::try_new(std::fs::File::create(&file)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    // 2. Commit: immutable manifest file + CAS the version pointer.
    let version = lake_manifest::commit(
        meta.as_ref(),
        &table_root,
        "episodes",
        vec![file.canonicalize()?.display().to_string()],
    )
    .await?;
    println!("committed table 'episodes' at v{version}");

    // 3. Query through the catalog with plain SQL.
    let ctx = SessionContext::new();
    ctx.register_catalog("lake", Arc::new(LakeCatalog::new(meta.clone(), table_root)));
    let df = ctx
        .sql(
            "SELECT robot_id, count(*) AS episodes, avg(reward) AS avg_reward FROM \
             lake.public.episodes GROUP BY robot_id ORDER BY robot_id",
        )
        .await?;
    let results = df.collect().await?;
    datafusion::arrow::util::pretty::print_batches(&results)?;

    let rows: usize = results.iter().map(|b| b.num_rows()).sum();
    anyhow::ensure!(rows == 2, "expected one row per robot, got {rows}");
    println!("self-check ok");
    Ok(())
}
