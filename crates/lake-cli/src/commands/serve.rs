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

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use lake_common::ManagedStageBackend;
use lake_flight::ClientSecurity;
use lake_meta::{DynamoMeta, MetaStoreRef, RocksMeta};
use lake_metasrv::MetasrvServerConfig;
use lake_objects::{LocalObjectStore, ManagedObjectStore, S3ObjectStore};
use lake_query::{
    AsyncQueryConfig, QueryEngine, QueryResources, QueryServerConfig, connect_remote_catalog_source,
};

use super::{
    Context,
    limits::{
        append_limits_from_env, async_scheduler_limits_from_env, discovery_limits_from_env,
        maintenance_limits_from_env, query_limits_from_env, query_resources_from_env,
        query_ticket_ttl_from_env, shutdown_grace_from_env,
    },
    security::{
        allow_insecure_from_env, metadata_client_security_from_env, peer_client_security_from_env,
        query_ticket_keys_from_env, server_security_from_env,
    },
};
use crate::metrics;

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
    let metadata_security = metadata_client_security_from_env()?;
    let (engine, metadata_endpoint) = query_engine_for_server(
        ctx,
        metadata_addr,
        metadata_security.clone(),
        query_resources_from_env()?,
    )
    .await?;
    let mut config = QueryServerConfig::new()
        .with_metadata(metadata_endpoint, metadata_security)
        .with_managed_stage(ctx.managed_stage().clone())
        .with_server_security(server_security_from_env()?)
        .with_limits(query_limits_from_env()?)
        .with_discovery_limits(discovery_limits_from_env()?)
        .with_ticket_ttl(query_ticket_ttl_from_env()?)
        .with_shutdown_grace(shutdown_grace_from_env()?)
        .allow_insecure(allow_insecure_from_env()?);
    if let Some(keys) = query_ticket_keys_from_env()? {
        config = config.with_ticket_keys(keys);
    }
    if async_queries_enabled_from_env()? {
        config = config.with_async_queries(async_query_config(ctx).await?);
    }
    metrics::run_with_metrics("query", shutdown, |cancellation| async move {
        lake_query::serve_with_config_and_shutdown(
            engine,
            addr,
            config,
            cancellation.cancelled_owned(),
        )
        .await?;
        Ok(())
    })
    .await
}

async fn query_engine_for_server(
    ctx: &Context,
    metadata_addr: &str,
    security: ClientSecurity,
    resources: QueryResources,
) -> anyhow::Result<(Arc<QueryEngine>, String)> {
    let endpoint = if metadata_addr.contains("://") {
        metadata_addr.to_owned()
    } else {
        security.endpoint_for_authority(metadata_addr)
    };
    let source = connect_remote_catalog_source(endpoint.clone(), security).await?;
    let engine =
        QueryEngine::try_with_catalog_source_and_resources(source, ctx.engine.clone(), resources)?;
    Ok((Arc::new(engine), endpoint))
}

fn async_queries_enabled_from_env() -> anyhow::Result<bool> {
    match std::env::var("LAKE_ASYNC_QUERIES") {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(error.into()),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => anyhow::bail!("LAKE_ASYNC_QUERIES must be a boolean"),
        },
    }
}

async fn async_query_config(ctx: &Context) -> anyhow::Result<AsyncQueryConfig> {
    let (state, results): (MetaStoreRef, Arc<dyn ManagedObjectStore>) =
        match ctx.managed_stage().backend() {
            ManagedStageBackend::Local { root } => {
                let data_root = std::path::Path::new(root)
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("managed stage has no data root"))?;
                let state: MetaStoreRef =
                    Arc::new(RocksMeta::open(data_root.join("async-query-state"))?);
                let results: Arc<dyn ManagedObjectStore> =
                    Arc::new(LocalObjectStore::open(data_root.join("async-query-results")).await?);
                (state, results)
            }
            ManagedStageBackend::S3 {
                bucket,
                region,
                endpoint,
                force_path_style,
                ..
            } => {
                let table = std::env::var("LAKE_ASYNC_DYNAMODB_TABLE")
                    .unwrap_or_else(|_| "lake_async_queries".to_owned());
                let dynamo_endpoint = std::env::var("LAKE_DYNAMODB_ENDPOINT").ok();
                let dynamo = DynamoMeta::connect(dynamo_endpoint.as_deref(), &table).await?;
                dynamo.open_tables().await?;
                let state: MetaStoreRef = Arc::new(dynamo);
                let mut loader = aws_config::defaults(BehaviorVersion::latest());
                if let Some(region) = region {
                    loader = loader.region(Region::new(region.clone()));
                }
                let shared = loader.load().await;
                let mut config =
                    aws_sdk_s3::config::Builder::from(&shared).force_path_style(*force_path_style);
                if let Some(endpoint) = endpoint {
                    config = config.endpoint_url(endpoint);
                }
                let prefix = std::env::var("LAKE_ASYNC_RESULT_PREFIX")
                    .unwrap_or_else(|_| "async-query-results".to_owned());
                let results: Arc<dyn ManagedObjectStore> = Arc::new(S3ObjectStore::new(
                    aws_sdk_s3::Client::from_conf(config.build()),
                    bucket,
                    prefix,
                )?);
                (state, results)
            }
        };
    let (workers, workers_per_tenant, execution_time) = async_scheduler_limits_from_env()?;
    AsyncQueryConfig::new(state, results)
        .try_with_scheduler_limits(workers, workers_per_tenant, execution_time)
        .map_err(Into::into)
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
    metrics::run_with_metrics("metasrv", shutdown, |cancellation| async move {
        lake_metasrv::serve_with_config_and_shutdown(
            ctx.metasrv.clone(),
            addr,
            config,
            cancellation.cancelled_owned(),
        )
        .await?;
        Ok(())
    })
    .await
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
    use lake_engine_lance::LanceMaintenancePolicy;
    use lake_flight::ClientSecurity;
    use lake_query::QueryResources;

    use super::query_engine_for_server;
    use crate::commands::Context;

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

    #[tokio::test]
    async fn query_catalog_wiring_requires_remote_metadata_source() {
        let root = tempfile::tempdir().unwrap();
        let ctx = Context::open_local(
            root.path().to_str().unwrap(),
            LanceMaintenancePolicy::default(),
        )
        .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let unavailable = listener.local_addr().unwrap();
        drop(listener);

        let result = query_engine_for_server(
            &ctx,
            &unavailable.to_string(),
            ClientSecurity::new(),
            QueryResources::default(),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("served Query must not fall back to its directly available registry"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("catalog authority"));
    }
}
