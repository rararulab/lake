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

use std::{env, future::Future, sync::Arc};

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use lake_common::{ManagedStageBackend, ManagedStageDescriptor};
use lake_flight::ClientSecurity;
use lake_iceberg::{IcebergCatalog, IcebergCatalogConfig, IcebergOAuthOptions, IcebergRestAuth};
use lake_meta::{DynamoMeta, MetaStoreRef, RocksMeta};
use lake_metasrv::MetasrvServerConfig;
use lake_objects::{
    LocalObjectStore, ManagedObjectStore, ManagedReadCapabilityIssuerRef, S3ObjectStore,
    S3ReadCapabilityIssuer,
};
use lake_query::{
    AsyncQueryConfig, QueryEngine, QueryResources, QueryServerConfig, connect_remote_catalog_source,
};

use super::{
    Context, QueryContext,
    limits::{
        append_limits_from_env, async_resource_limits_from_env, async_scheduler_limits_from_env,
        discovery_limits_from_env, maintenance_limits_from_env, query_limits_from_env,
        query_resources_from_env, query_ticket_ttl_from_env, shutdown_grace_from_env,
    },
    security::{
        allow_insecure_from_env, metadata_client_security_from_env, peer_client_security_from_env,
        query_ticket_keys_from_env, server_security_from_env,
    },
};
use crate::metrics;

pub async fn query(ctx: &QueryContext, addr: &str, metadata_addr: &str) -> anyhow::Result<()> {
    query_with_shutdown(ctx, addr, metadata_addr, shutdown_signal()).await
}

async fn query_with_shutdown<F>(
    ctx: &QueryContext,
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
    if let Some(issuer) = read_capability_issuer(ctx.managed_stage()).await? {
        config = config.with_read_capability_issuer(issuer);
    }
    if ctx.async_enabled() {
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

/// Construct a server-side read-capability issuer only for an S3 managed stage.
///
/// The client and its AWS credentials stay in the Query process; the SDK sees
/// only the bounded result of the Query Flight action.
async fn read_capability_issuer(
    stage: &ManagedStageDescriptor,
) -> anyhow::Result<Option<ManagedReadCapabilityIssuerRef>> {
    match stage.backend() {
        ManagedStageBackend::Local { .. } => Ok(None),
        ManagedStageBackend::S3 { .. } => {
            let client = s3_client_for_stage(stage).await?;
            read_capability_issuer_for_stage(stage, Some(client))
        }
    }
}

fn read_capability_issuer_for_stage(
    stage: &ManagedStageDescriptor,
    s3_client: Option<aws_sdk_s3::Client>,
) -> anyhow::Result<Option<ManagedReadCapabilityIssuerRef>> {
    match stage.backend() {
        ManagedStageBackend::Local { .. } => Ok(None),
        ManagedStageBackend::S3 { .. } => {
            let client = s3_client.ok_or_else(|| {
                anyhow::anyhow!("S3 managed stage requires a Query-owned S3 client")
            })?;
            Ok(Some(Arc::new(S3ReadCapabilityIssuer::new(client))))
        }
    }
}

async fn s3_client_for_stage(stage: &ManagedStageDescriptor) -> anyhow::Result<aws_sdk_s3::Client> {
    let ManagedStageBackend::S3 {
        region,
        endpoint,
        force_path_style,
        ..
    } = stage.backend()
    else {
        anyhow::bail!("local managed stage cannot construct an S3 client");
    };
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(Region::new(region.clone()));
    }
    let shared = loader.load().await;
    let mut config = aws_sdk_s3::config::Builder::from(&shared).force_path_style(*force_path_style);
    if let Some(endpoint) = endpoint {
        config = config.endpoint_url(endpoint);
    }
    Ok(aws_sdk_s3::Client::from_conf(config.build()))
}

async fn query_engine_for_server(
    ctx: &QueryContext,
    metadata_addr: &str,
    security: ClientSecurity,
    resources: QueryResources,
) -> anyhow::Result<(Arc<QueryEngine>, String)> {
    let iceberg = match iceberg_catalog_config_from_env()? {
        Some(config) => Some(IcebergCatalog::connect(config).await?),
        None => None,
    };
    let endpoint = if metadata_addr.contains("://") {
        metadata_addr.to_owned()
    } else {
        security.endpoint_for_authority(metadata_addr)
    };
    let source = connect_remote_catalog_source(endpoint.clone(), security).await?;
    let engine =
        QueryEngine::try_with_catalog_source_and_resources(source, ctx.engine.clone(), resources)?;
    let engine = match iceberg {
        Some(catalog) => engine.with_iceberg_catalog(catalog),
        None => engine,
    };
    Ok((Arc::new(engine), endpoint))
}

fn iceberg_catalog_config_from_env() -> anyhow::Result<Option<IcebergCatalogConfig>> {
    let endpoint = optional_env("LAKE_ICEBERG_REST_ENDPOINT")?;
    let warehouse = optional_env("LAKE_ICEBERG_WAREHOUSE")?;
    let namespaces = optional_env("LAKE_ICEBERG_NAMESPACES")?;
    let auth = IcebergRestAuthValues {
        token:             optional_env("LAKE_ICEBERG_REST_TOKEN")?,
        credential:        optional_env("LAKE_ICEBERG_REST_CREDENTIAL")?,
        oauth2_server_uri: optional_env("LAKE_ICEBERG_REST_OAUTH2_SERVER_URI")?,
        scope:             optional_env("LAKE_ICEBERG_REST_OAUTH_SCOPE")?,
        audience:          optional_env("LAKE_ICEBERG_REST_OAUTH_AUDIENCE")?,
        resource:          optional_env("LAKE_ICEBERG_REST_OAUTH_RESOURCE")?,
    };
    iceberg_catalog_config_from_all_values(
        endpoint.as_deref(),
        warehouse.as_deref(),
        namespaces.as_deref(),
        auth.as_deref(),
    )
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn iceberg_catalog_config_from_values(
    endpoint: Option<&str>,
    warehouse: Option<&str>,
    namespaces: Option<&str>,
) -> anyhow::Result<Option<IcebergCatalogConfig>> {
    match (endpoint, warehouse, namespaces) {
        (None, None, None) => Ok(None),
        (Some(endpoint), Some(warehouse), Some(namespaces)) => {
            let namespaces = namespaces.split(',').map(str::trim);
            Ok(Some(IcebergCatalogConfig::try_new(
                endpoint, warehouse, namespaces,
            )?))
        }
        _ => anyhow::bail!(
            "LAKE_ICEBERG_REST_ENDPOINT, LAKE_ICEBERG_WAREHOUSE, and LAKE_ICEBERG_NAMESPACES must \
             all be set together"
        ),
    }
}

struct IcebergRestAuthValues<T> {
    token:             Option<T>,
    credential:        Option<T>,
    oauth2_server_uri: Option<T>,
    scope:             Option<T>,
    audience:          Option<T>,
    resource:          Option<T>,
}

impl<T> IcebergRestAuthValues<T> {
    fn as_deref(&self) -> IcebergRestAuthValues<&str>
    where
        T: AsRef<str>,
    {
        IcebergRestAuthValues {
            token:             self.token.as_ref().map(AsRef::as_ref),
            credential:        self.credential.as_ref().map(AsRef::as_ref),
            oauth2_server_uri: self.oauth2_server_uri.as_ref().map(AsRef::as_ref),
            scope:             self.scope.as_ref().map(AsRef::as_ref),
            audience:          self.audience.as_ref().map(AsRef::as_ref),
            resource:          self.resource.as_ref().map(AsRef::as_ref),
        }
    }
}

fn iceberg_catalog_config_from_all_values(
    endpoint: Option<&str>,
    warehouse: Option<&str>,
    namespaces: Option<&str>,
    auth_values: IcebergRestAuthValues<&str>,
) -> anyhow::Result<Option<IcebergCatalogConfig>> {
    let config = iceberg_catalog_config_from_values(endpoint, warehouse, namespaces)?;
    let auth = iceberg_rest_auth_from_values(auth_values)?;
    match (config, auth) {
        (None, None) => Ok(None),
        (None, Some(_)) => anyhow::bail!(
            "Iceberg REST authentication requires LAKE_ICEBERG_REST_ENDPOINT, \
             LAKE_ICEBERG_WAREHOUSE, and LAKE_ICEBERG_NAMESPACES"
        ),
        (Some(config), None) => Ok(Some(config)),
        (Some(config), Some(auth)) => Ok(Some(config.with_rest_auth(auth))),
    }
}

fn iceberg_rest_auth_from_values(
    values: IcebergRestAuthValues<&str>,
) -> anyhow::Result<Option<IcebergRestAuth>> {
    let has_oauth_options = [
        values.oauth2_server_uri,
        values.scope,
        values.audience,
        values.resource,
    ]
    .into_iter()
    .any(|value| value.is_some());
    match (values.token, values.credential) {
        (None, None) if !has_oauth_options => Ok(None),
        (None, None) => anyhow::bail!("Iceberg OAuth options require LAKE_ICEBERG_REST_CREDENTIAL"),
        (Some(_), Some(_)) => anyhow::bail!(
            "set only one of LAKE_ICEBERG_REST_TOKEN and LAKE_ICEBERG_REST_CREDENTIAL"
        ),
        (Some(_), None) if has_oauth_options => {
            anyhow::bail!("Iceberg OAuth options require LAKE_ICEBERG_REST_CREDENTIAL")
        }
        (Some(token), None) => IcebergRestAuth::bearer_token(token)
            .map(Some)
            .map_err(Into::into),
        (None, Some(credential)) => IcebergRestAuth::oauth_client_credentials(
            credential,
            IcebergOAuthOptions::builder()
                .maybe_oauth2_server_uri(values.oauth2_server_uri.map(str::to_owned))
                .maybe_scope(values.scope.map(str::to_owned))
                .maybe_audience(values.audience.map(str::to_owned))
                .maybe_resource(values.resource.map(str::to_owned))
                .build(),
        )
        .map(Some)
        .map_err(Into::into),
    }
}

async fn async_query_config(ctx: &QueryContext) -> anyhow::Result<AsyncQueryConfig> {
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
                let table = ctx.async_table().ok_or_else(|| {
                    anyhow::anyhow!("async DynamoDB authority was not validated before connect")
                })?;
                let dynamo_endpoint = std::env::var("LAKE_DYNAMODB_ENDPOINT").ok();
                let dynamo = DynamoMeta::connect(dynamo_endpoint.as_deref(), table).await?;
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
    let (outstanding_per_tenant, result_bytes) = async_resource_limits_from_env()?;
    AsyncQueryConfig::new(state, results)
        .try_with_scheduler_limits(workers, workers_per_tenant, execution_time)
        .and_then(|config| config.try_with_resource_limits(outstanding_per_tenant, result_bytes))
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
    use lake_common::ManagedStageDescriptor;
    use lake_engine_lance::LanceMaintenancePolicy;
    use lake_flight::ClientSecurity;
    use lake_query::QueryResources;

    use super::{
        IcebergRestAuthValues, iceberg_catalog_config_from_all_values,
        iceberg_catalog_config_from_values, query_engine_for_server,
        read_capability_issuer_for_stage,
    };
    use crate::commands::QueryContext;

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

    #[test]
    fn query_server_capability_issuer_is_s3_only() {
        let local = ManagedStageDescriptor::local("/var/lib/lake/managed-objects");
        assert!(
            read_capability_issuer_for_stage(&local, None)
                .expect("local Query does not need an S3 client")
                .is_none()
        );

        let s3 = ManagedStageDescriptor::s3(
            "lake-managed",
            "managed-objects",
            Some("us-east-1".to_owned()),
            None,
            true,
        );
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .build(),
        );
        assert!(
            read_capability_issuer_for_stage(&s3, Some(client))
                .expect("S3 Query owns the issuer")
                .is_some()
        );
        assert!(read_capability_issuer_for_stage(&s3, None).is_err());
    }

    #[test]
    fn iceberg_configuration_is_all_or_nothing_before_listener_bind() {
        assert!(
            iceberg_catalog_config_from_values(None, None, None)
                .expect("Iceberg is optional")
                .is_none()
        );

        let config = iceberg_catalog_config_from_values(
            Some("https://catalog.example.test/"),
            Some("s3://warehouse"),
            Some("analytics,models"),
        )
        .expect("complete configuration is valid")
        .expect("external catalog is configured");
        assert_eq!(config.endpoint().as_str(), "https://catalog.example.test/");
        assert_eq!(config.warehouse(), "s3://warehouse");
        assert_eq!(config.namespaces(), ["analytics", "models"]);

        for partial in [
            (
                Some("https://catalog.example.test"),
                None,
                Some("analytics"),
            ),
            (None, Some("s3://warehouse"), Some("analytics")),
            (
                Some("https://catalog.example.test"),
                Some("s3://warehouse"),
                None,
            ),
        ] {
            let error = iceberg_catalog_config_from_values(partial.0, partial.1, partial.2)
                .expect_err("partial configuration must fail before server bind");
            assert!(error.to_string().contains("all be set"));
        }
    }

    #[test]
    fn iceberg_rest_auth_configuration_is_validated_before_listener_bind() {
        const TOKEN: &str = "lake-rest-static-token";
        const CREDENTIAL: &str = "lake-query:lake-rest-oauth-secret";
        const SECRET: &str = "lake-rest-oauth-secret";
        let endpoint = Some("https://catalog.example.test");
        let warehouse = Some("s3://warehouse");
        let namespaces = Some("analytics");

        let static_auth = iceberg_catalog_config_from_all_values(
            endpoint,
            warehouse,
            namespaces,
            IcebergRestAuthValues {
                token:             Some(TOKEN),
                credential:        None,
                oauth2_server_uri: None,
                scope:             None,
                audience:          None,
                resource:          None,
            },
        )
        .expect("static bearer configuration is valid")
        .expect("Iceberg catalog is configured");
        assert!(
            !format!("{static_auth:?}").contains(TOKEN),
            "static bearer token must be redacted from Query configuration"
        );

        let oauth = iceberg_catalog_config_from_all_values(
            endpoint,
            warehouse,
            namespaces,
            IcebergRestAuthValues {
                token:             None,
                credential:        Some(CREDENTIAL),
                oauth2_server_uri: Some("https://identity.example.test/oauth/token"),
                scope:             Some("lake-catalog"),
                audience:          Some("lake"),
                resource:          Some("catalog"),
            },
        )
        .expect("OAuth client configuration is valid")
        .expect("Iceberg catalog is configured");
        assert!(
            !format!("{oauth:?}").contains(SECRET),
            "OAuth client secret must be redacted from Query configuration"
        );

        for invalid in [
            (Some(TOKEN), Some(CREDENTIAL), None, None, None, None),
            (None, None, None, Some("lake-catalog"), None, None),
            (
                None,
                Some(CREDENTIAL),
                Some("https://client:secret@identity.example.test/oauth/token"),
                None,
                None,
                None,
            ),
        ] {
            let error = iceberg_catalog_config_from_all_values(
                endpoint,
                warehouse,
                namespaces,
                IcebergRestAuthValues {
                    token:             invalid.0,
                    credential:        invalid.1,
                    oauth2_server_uri: invalid.2,
                    scope:             invalid.3,
                    audience:          invalid.4,
                    resource:          invalid.5,
                },
            )
            .expect_err("contradictory or partial REST authentication must fail before bind");
            assert!(
                !error.to_string().contains(TOKEN) && !error.to_string().contains(SECRET),
                "configuration error must not echo REST credentials"
            );
        }

        assert!(
            iceberg_catalog_config_from_all_values(
                None,
                None,
                None,
                IcebergRestAuthValues {
                    token:             Some(TOKEN),
                    credential:        None,
                    oauth2_server_uri: None,
                    scope:             None,
                    audience:          None,
                    resource:          None,
                },
            )
            .is_err(),
            "REST authentication without an enabled Iceberg catalog must fail"
        );
    }

    #[tokio::test]
    async fn query_catalog_wiring_requires_remote_metadata_source() {
        let root = tempfile::tempdir().unwrap();
        let ctx = QueryContext::open_local(
            root.path().to_str().unwrap(),
            LanceMaintenancePolicy::default(),
            false,
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
