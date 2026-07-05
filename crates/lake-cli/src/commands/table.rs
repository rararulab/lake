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

//! `lake table` — create and list tables.

use std::sync::Arc;

use anyhow::{Context as _, bail};
use clap::Subcommand;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use lake_common::{Namespace, TableRef};

use super::Context;

#[derive(Subcommand)]
pub enum TableCmd {
    /// Create an empty table with a column schema.
    Create {
        /// `<namespace>.<name>`, e.g. `robots.arm_left`.
        table:   String,
        /// Columns as `name:type` (types: i64, f64, utf8, bool). Repeatable.
        #[arg(long = "column", value_name = "name:type", required = true)]
        columns: Vec<String>,
    },
    /// List namespaces, or the tables in one namespace.
    List {
        /// Namespace to list tables for; omit to list namespaces.
        namespace: Option<String>,
    },
}

pub async fn run(ctx: &Context, cmd: TableCmd) -> anyhow::Result<()> {
    match cmd {
        TableCmd::Create { table, columns } => create(ctx, &table, &columns).await,
        TableCmd::List { namespace } => list(ctx, namespace.as_deref()).await,
    }
}

fn parse_table_ref(s: &str) -> anyhow::Result<TableRef> {
    let (ns, name) = s
        .split_once('.')
        .context("table must be <namespace>.<name>")?;
    Ok(TableRef::new(ns, name))
}

fn parse_schema(columns: &[String]) -> anyhow::Result<Arc<Schema>> {
    let fields = columns
        .iter()
        .map(|c| {
            let (name, ty) = c.split_once(':').context("column must be name:type")?;
            let dt = match ty {
                "i64" => DataType::Int64,
                "f64" => DataType::Float64,
                "utf8" => DataType::Utf8,
                "bool" => DataType::Boolean,
                other => bail!("unknown column type '{other}' (use i64|f64|utf8|bool)"),
            };
            Ok(Field::new(name, dt, false))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

async fn create(ctx: &Context, table: &str, columns: &[String]) -> anyhow::Result<()> {
    let table = parse_table_ref(table)?;
    let schema = parse_schema(columns)?;
    let location = ctx.location(&table);
    ctx.metasrv
        .create_table(&table, location, schema)
        .await
        .with_context(|| format!("creating {table}"))?;
    println!("created table {table}");
    Ok(())
}

async fn list(ctx: &Context, namespace: Option<&str>) -> anyhow::Result<()> {
    match namespace {
        Some(ns) => {
            let tables = ctx.metasrv.list_tables(&Namespace(ns.to_string())).await?;
            for t in tables {
                println!("{ns}.{}", t.0);
            }
        }
        None => {
            for ns in ctx.metasrv.list_namespaces().await? {
                println!("{}", ns.0);
            }
        }
    }
    Ok(())
}
