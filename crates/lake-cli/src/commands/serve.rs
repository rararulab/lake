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

use lake_metasrv::MetasrvServerConfig;
use lake_query::{QueryEngine, QueryServerConfig};

use super::{
    Context,
    limits::query_limits_from_env,
    security::{
        allow_insecure_from_env, metadata_client_security_from_env, peer_client_security_from_env,
        server_security_from_env,
    },
};

pub async fn query(ctx: &Context, addr: &str, metadata_addr: &str) -> anyhow::Result<()> {
    let engine = Arc::new(QueryEngine::new(ctx.meta.clone(), ctx.engine.clone()));
    let config = QueryServerConfig::new()
        .with_metadata(metadata_addr, metadata_client_security_from_env()?)
        .with_managed_stage(ctx.managed_stage().clone())
        .with_server_security(server_security_from_env()?)
        .with_limits(query_limits_from_env()?)
        .allow_insecure(allow_insecure_from_env()?);
    lake_query::serve_with_config(engine, addr, config).await?;
    Ok(())
}

pub async fn meta(ctx: &Context, addr: &str) -> anyhow::Result<()> {
    let config = MetasrvServerConfig::new()
        .with_server_security(server_security_from_env()?)
        .with_peer_security(peer_client_security_from_env()?)
        .allow_insecure(allow_insecure_from_env()?);
    lake_metasrv::serve_with_config(ctx.metasrv.clone(), addr, config).await?;
    Ok(())
}
