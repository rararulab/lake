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

//! `lake query` / `lake meta` — run the tier servers.

use std::sync::Arc;

use lake_query::QueryEngine;

use super::Context;

pub async fn query(ctx: &Context, addr: &str, metadata_addr: &str) -> anyhow::Result<()> {
    let engine = Arc::new(QueryEngine::new(ctx.meta.clone(), ctx.engine.clone()));
    lake_query::serve_with_metadata_and_stage(
        engine,
        addr,
        metadata_addr,
        ctx.managed_stage().clone(),
    )
    .await?;
    Ok(())
}

pub async fn meta(ctx: &Context, addr: &str) -> anyhow::Result<()> {
    lake_metasrv::serve(ctx.metasrv.clone(), addr).await?;
    Ok(())
}
