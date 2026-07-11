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

use std::{future::Future, sync::Arc};

use lake_metasrv::MetasrvServerConfig;
use lake_query::{QueryEngine, QueryServerConfig};

use super::{
    Context,
    limits::{
        append_limits_from_env, discovery_limits_from_env, maintenance_limits_from_env,
        query_limits_from_env, query_resources_from_env, shutdown_grace_from_env,
    },
    security::{
        allow_insecure_from_env, metadata_client_security_from_env, peer_client_security_from_env,
        server_security_from_env,
    },
};

pub async fn query(ctx: &Context, addr: &str, metadata_addr: &str) -> anyhow::Result<()> {
    query_with_shutdown(ctx, addr, metadata_addr, shutdown_signal()).await
}

async fn query_with_shutdown<F>(
    ctx: &Context,
    addr: &str,
    metadata_addr: &str,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let engine = Arc::new(QueryEngine::try_with_resources(
        ctx.meta.clone(),
        ctx.engine.clone(),
        query_resources_from_env()?,
    )?);
    let config = QueryServerConfig::new()
        .with_metadata(metadata_addr, metadata_client_security_from_env()?)
        .with_managed_stage(ctx.managed_stage().clone())
        .with_server_security(server_security_from_env()?)
        .with_limits(query_limits_from_env()?)
        .with_discovery_limits(discovery_limits_from_env()?)
        .with_shutdown_grace(shutdown_grace_from_env()?)
        .allow_insecure(allow_insecure_from_env()?);
    lake_query::serve_with_config_and_shutdown(engine, addr, config, shutdown).await?;
    Ok(())
}

pub async fn meta(ctx: &Context, addr: &str) -> anyhow::Result<()> {
    meta_with_shutdown(ctx, addr, shutdown_signal()).await
}

async fn meta_with_shutdown<F>(ctx: &Context, addr: &str, shutdown: F) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let config = MetasrvServerConfig::new()
        .with_table_placement(ctx.table_placement().clone())
        .with_server_security(server_security_from_env()?)
        .with_peer_security(peer_client_security_from_env()?)
        .with_append_limits(append_limits_from_env()?)
        .with_maintenance_limits(maintenance_limits_from_env()?)
        .with_shutdown_grace(shutdown_grace_from_env()?)
        .allow_insecure(allow_insecure_from_env()?);
    lake_metasrv::serve_with_config_and_shutdown(ctx.metasrv.clone(), addr, config, shutdown)
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    result = ctrl_c => {
                        if let Err(error) = result {
                            tracing::error!(%error, "failed to listen for SIGINT");
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to listen for SIGTERM; waiting for SIGINT");
                if let Err(error) = ctrl_c.await {
                    tracing::error!(%error, "failed to listen for SIGINT");
                }
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = ctrl_c.await {
        tracing::error!(%error, "failed to listen for Ctrl-C");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn server_commands_use_injected_shutdown_path() {
        let source = include_str!("serve.rs");
        assert!(
            source.contains("query_with_shutdown(ctx, addr, metadata_addr, shutdown_signal())")
        );
        assert!(source.contains("meta_with_shutdown(ctx, addr, shutdown_signal())"));
        assert!(source.contains("lake_query::serve_with_config_and_shutdown"));
        assert!(source.contains("lake_metasrv::serve_with_config_and_shutdown"));
    }
}
