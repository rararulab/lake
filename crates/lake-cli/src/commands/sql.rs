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

//! `lake sql` — run a SQL statement through the query engine.

use futures::TryStreamExt;
use lake_query::QueryEngine;

use super::Context;

pub async fn run(ctx: &Context, query: &str) -> anyhow::Result<()> {
    let engine = QueryEngine::new(ctx.meta.clone(), ctx.engine.clone());
    let mut batches = engine.execute_sql(query).await?;
    while let Some(batch) = batches.try_next().await? {
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch])?
        );
    }
    Ok(())
}
