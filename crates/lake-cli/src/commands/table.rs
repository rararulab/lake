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
use lake_objects::data_location_field;

use super::Context;

const CONTROL_ENUMERATION_PAGE_ENTRIES: usize = 128;

#[derive(Subcommand)]
pub enum TableCmd {
    /// Create an empty table with a column schema.
    Create {
        /// `<namespace>.<name>`, e.g. `robots.arm_left`.
        table:   String,
        /// Columns as `name:type` (types: i64, f64, utf8, bool, file).
        /// Repeatable.
        #[arg(long = "column", value_name = "name:type", required = true)]
        columns: Vec<String>,
    },
    /// List namespaces, or the tables in one namespace.
    List {
        /// Namespace to list tables for; omit to list namespaces.
        namespace: Option<String>,
    },
    /// Drop a table: delete its data and deregister it.
    Drop {
        /// `<namespace>.<name>`, e.g. `robots.arm_left`.
        table: String,
    },
}

pub async fn run(ctx: &Context, cmd: TableCmd) -> anyhow::Result<()> {
    match cmd {
        TableCmd::Create { table, columns } => create(ctx, &table, &columns).await,
        TableCmd::List { namespace } => list(ctx, namespace.as_deref()).await,
        TableCmd::Drop { table } => drop_table(ctx, &table).await,
    }
}

pub(crate) fn parse_table_ref(s: &str) -> anyhow::Result<TableRef> {
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
            let field = match ty {
                "i64" => Field::new(name, DataType::Int64, false),
                "f64" => Field::new(name, DataType::Float64, false),
                "utf8" => Field::new(name, DataType::Utf8, false),
                "bool" => Field::new(name, DataType::Boolean, false),
                "file" => data_location_field(name, false),
                other => bail!("unknown column type '{other}' (use i64|f64|utf8|bool|file)"),
            };
            Ok(field)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

async fn create(ctx: &Context, table: &str, columns: &[String]) -> anyhow::Result<()> {
    let table = parse_table_ref(table)?;
    let schema = parse_schema(columns)?;
    let location = ctx.location(&table)?;
    ctx.metasrv
        .create_table(&table, location, schema)
        .await
        .with_context(|| format!("creating {table}"))?;
    println!("created table {table}");
    Ok(())
}

async fn drop_table(ctx: &Context, table: &str) -> anyhow::Result<()> {
    let table = parse_table_ref(table)?;
    ctx.metasrv
        .drop_table(&table)
        .await
        .with_context(|| format!("dropping {table}"))?;
    println!("dropped table {table}");
    Ok(())
}

async fn list(ctx: &Context, namespace: Option<&str>) -> anyhow::Result<()> {
    match namespace {
        Some(ns) => {
            let namespace = Namespace(ns.to_string());
            let mut continuation = None;
            loop {
                let page = ctx
                    .metasrv
                    .list_tables_page(
                        &namespace,
                        continuation.as_deref(),
                        CONTROL_ENUMERATION_PAGE_ENTRIES,
                    )
                    .await?;
                let (tables, next) = page.into_parts();
                for table in tables {
                    println!("{ns}.{}", table.0);
                }
                let Some(next) = next else { break };
                continuation = Some(next);
            }
        }
        None => {
            anyhow::bail!(
                "global namespace enumeration requires a durable namespace index; pass --namespace"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use lake_objects::data_location_field;

    use super::parse_schema;

    #[test]
    fn local_schema_dsl_accepts_file() {
        let schema = parse_schema(&["video:file".to_owned()]).unwrap();

        assert_eq!(schema.field(0), &data_location_field("video", false));
    }
}
