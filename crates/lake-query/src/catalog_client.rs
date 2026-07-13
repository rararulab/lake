// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! Authenticated Metasrv-backed catalog source for served Query replicas.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow_flight::{Action, Result as FlightResult, flight_service_client::FlightServiceClient};
use async_trait::async_trait;
use lake_catalog::{
    CatalogDirectoryRequest, CatalogDirectoryResponse, CatalogSource, CatalogSourceError,
    CatalogSourceRef, CatalogSourceResult,
};
use lake_common::TableRef;
use lake_flight::{ClientSecurity, DELEGATED_NAMESPACE_HEADER};
use lake_meta::registry::TableRegistration;
use serde::Serialize;
use tonic::{Code, Request, transport::Channel};

const MAX_DIRECTORY_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_RESOLVE_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_FLIGHT_RESULT_MESSAGE_BYTES: usize = MAX_DIRECTORY_RESPONSE_BYTES + 1024;

#[derive(Clone)]
pub(crate) struct RemoteCatalogSource {
    client:   FlightServiceClient<Channel>,
    security: ClientSecurity,
    requests: Arc<AtomicUsize>,
}

impl std::fmt::Debug for RemoteCatalogSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteCatalogSource")
            .field("security", &self.security)
            .finish_non_exhaustive()
    }
}

impl RemoteCatalogSource {
    pub(crate) async fn connect(
        endpoint: impl Into<String>,
        security: ClientSecurity,
    ) -> CatalogSourceResult<Self> {
        let channel = security
            .connect(endpoint)
            .await
            .map_err(CatalogSourceError::remote)?;
        Ok(Self {
            client: FlightServiceClient::new(channel)
                .max_decoding_message_size(MAX_FLIGHT_RESULT_MESSAGE_BYTES),
            security,
            requests: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(crate) async fn connect_ref(
        endpoint: impl Into<String>,
        security: ClientSecurity,
    ) -> CatalogSourceResult<CatalogSourceRef> {
        Self::connect(endpoint, security)
            .await
            .map(|source| Arc::new(source) as CatalogSourceRef)
    }

    async fn action<T: Serialize>(
        &self,
        action_type: &str,
        body: &T,
        delegated_namespace: Option<&str>,
        response_limit: usize,
    ) -> CatalogSourceResult<FlightResult> {
        let body = serde_json::to_vec(body).map_err(CatalogSourceError::remote)?;
        let action = Action {
            r#type: action_type.to_owned(),
            body:   body.into(),
        };
        let mut request = self.security.authorize_request(Request::new(action));
        if let Some(namespace) = delegated_namespace {
            request.metadata_mut().insert(
                DELEGATED_NAMESPACE_HEADER,
                namespace
                    .parse()
                    .map_err(|_| CatalogSourceError::InvalidResponse)?,
            );
        }
        self.requests.fetch_add(1, Ordering::Relaxed);
        let mut client = self.client.clone();
        let mut stream = client
            .do_action(request)
            .await
            .map_err(CatalogSourceError::remote)?
            .into_inner();
        let result = stream
            .message()
            .await
            .map_err(CatalogSourceError::remote)?
            .ok_or(CatalogSourceError::InvalidResponse)?;
        if result.body.len() > response_limit
            || stream
                .message()
                .await
                .map_err(CatalogSourceError::remote)?
                .is_some()
        {
            return Err(CatalogSourceError::InvalidResponse);
        }
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn request_count(&self) -> usize { self.requests.load(Ordering::Relaxed) }
}

#[derive(Serialize)]
struct ResolveRequest<'a> {
    namespace: &'a str,
    name:      &'a str,
}

#[async_trait]
impl CatalogSource for RemoteCatalogSource {
    async fn resolve(&self, table: &TableRef) -> CatalogSourceResult<Option<TableRegistration>> {
        let request = ResolveRequest {
            namespace: &table.namespace.0,
            name:      &table.name.0,
        };
        match self
            .action(
                "resolve",
                &request,
                Some(&table.namespace.0),
                MAX_RESOLVE_RESPONSE_BYTES,
            )
            .await
        {
            Ok(result) => serde_json::from_slice(&result.body)
                .map(Some)
                .map_err(CatalogSourceError::remote),
            Err(CatalogSourceError::Remote { source }) => {
                if source
                    .downcast_ref::<tonic::Status>()
                    .is_some_and(|status| status.code() == Code::NotFound)
                {
                    Ok(None)
                } else {
                    Err(CatalogSourceError::Remote { source })
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn directory(
        &self,
        request: CatalogDirectoryRequest,
    ) -> CatalogSourceResult<CatalogDirectoryResponse> {
        let result = self
            .action(
                "catalog_snapshot",
                &request,
                None,
                MAX_DIRECTORY_RESPONSE_BYTES,
            )
            .await?;
        serde_json::from_slice(&result.body).map_err(CatalogSourceError::remote)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use arrow_flight::{IpcMessage, SchemaAsIpc};
    use datafusion::arrow::{
        datatypes::{DataType, Field, Schema},
        ipc::writer::IpcWriteOptions,
    };
    use lake_catalog::{LakeCatalog, LocalCatalogSource};
    use lake_common::{
        Principal, PrincipalId, PrincipalRole, TableLocation, TableRef, TenantId, Version,
    };
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_flight::{BearerPrincipalBinding, ClientSecurity, ServerSecurity};
    use lake_meta::{MetaStoreRef, RocksMeta, registry, registry::TableRegistration};
    use lake_metasrv::{Metasrv, MetasrvServerConfig, serve_with_config_and_shutdown};
    use tokio_util::sync::CancellationToken;

    use super::RemoteCatalogSource;

    struct RemoteFixture {
        _root:  tempfile::TempDir,
        source: RemoteCatalogSource,
        local:  Arc<LocalCatalogSource>,
        meta:   MetaStoreRef,
        engine: TableEngineRef,
        table:  TableRef,
        stop:   CancellationToken,
        server: tokio::task::JoinHandle<lake_metasrv::Result<()>>,
    }

    impl RemoteFixture {
        async fn shutdown(self) {
            self.stop.cancel();
            tokio::time::timeout(Duration::from_secs(2), self.server)
                .await
                .expect("Metasrv shutdown is bounded")
                .expect("Metasrv task joins")
                .expect("Metasrv exits cleanly");
        }
    }

    async fn remote_fixture() -> RemoteFixture {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        let IpcMessage(schema_ipc) = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .unwrap();
        registry::register(
            meta.as_ref(),
            &table,
            &TableRegistration::new(
                TableLocation::new("mem://episodes"),
                "lance",
                Version(7),
                schema_ipc.to_vec(),
            ),
        )
        .await
        .unwrap();
        registry::finalize_directory_generation(meta.as_ref())
            .await
            .unwrap();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let principal = Principal::try_new(
            PrincipalId::try_new("query-service").unwrap(),
            TenantId::try_new("service").unwrap(),
            PrincipalRole::QueryService,
            std::iter::empty::<&str>(),
        )
        .unwrap();
        let token = "query-catalog-token";
        let security =
            ServerSecurity::with_bearer_principals([
                BearerPrincipalBinding::new(token, principal).unwrap()
            ])
            .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let stop = CancellationToken::new();
        let server_stop = stop.clone();
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let server_addr = addr.to_string();
        let server = tokio::spawn(async move {
            serve_with_config_and_shutdown(
                metasrv,
                &server_addr,
                MetasrvServerConfig::new()
                    .with_server_security(security)
                    .with_shutdown_grace(Duration::from_millis(500)),
                server_stop.cancelled_owned(),
            )
            .await
        });
        let client_security = ClientSecurity::new().with_bearer_token(token).unwrap();
        let endpoint = format!("http://{addr}");
        let source = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match RemoteCatalogSource::connect(&endpoint, client_security.clone()).await {
                    Ok(source) => break source,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("secured Metasrv becomes reachable");
        RemoteFixture {
            _root: root,
            source,
            local: Arc::new(LocalCatalogSource::new(meta.clone())),
            meta,
            engine,
            table,
            stop,
            server,
        }
    }

    #[tokio::test]
    async fn remote_catalog_source_matches_local_catalog_resolution() {
        let fixture = remote_fixture().await;
        let local = LakeCatalog::with_source(fixture.local.clone(), fixture.engine.clone());
        let remote =
            LakeCatalog::with_source(Arc::new(fixture.source.clone()), fixture.engine.clone());

        local.refresh().await.unwrap();
        remote.refresh().await.unwrap();
        assert_eq!(
            local.cached_generation().listings(),
            remote.cached_generation().listings()
        );
        assert_eq!(
            local
                .cached_generation()
                .table_schema(&fixture.table)
                .unwrap(),
            remote
                .cached_generation()
                .table_schema(&fixture.table)
                .unwrap()
        );
        assert_eq!(
            local.resolve_snapshot(&fixture.table).await.unwrap(),
            remote.resolve_snapshot(&fixture.table).await.unwrap()
        );
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn remote_catalog_cache_hit_uses_zero_metadata_rpcs() {
        let fixture = remote_fixture().await;
        let catalog =
            LakeCatalog::with_source(Arc::new(fixture.source.clone()), fixture.engine.clone());
        catalog.refresh().await.unwrap();
        assert_eq!(fixture.source.request_count(), 1);
        catalog.resolve_snapshot(&fixture.table).await.unwrap();
        assert_eq!(fixture.source.request_count(), 2);

        for _ in 0..16 {
            catalog.resolve_snapshot(&fixture.table).await.unwrap();
            catalog
                .refresh_if_stale(Duration::from_secs(5))
                .await
                .unwrap();
        }
        assert_eq!(
            fixture.source.request_count(),
            2,
            "warm listing and registration cache hits issue no metadata RPCs"
        );
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn remote_catalog_outage_serves_last_good_generation() {
        let fixture = remote_fixture().await;
        let source = fixture.source.clone();
        let catalog = LakeCatalog::with_source(Arc::new(source.clone()), fixture.engine.clone());
        catalog.refresh().await.unwrap();
        let last_good = catalog.cached_generation();
        let requests_before_outage = source.request_count();
        fixture.shutdown().await;

        for _ in 0..16 {
            catalog.refresh_if_stale(Duration::ZERO).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while catalog.refresh_health().consecutive_failures() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one failed revalidation is observed");
        assert!(Arc::ptr_eq(&last_good, &catalog.cached_generation()));
        assert_eq!(
            source.request_count(),
            requests_before_outage + 1,
            "concurrent stale callers coalesce to one failed metadata RPC"
        );
    }

    #[tokio::test]
    async fn remote_catalog_append_invalidation_observes_committed_version() {
        let fixture = remote_fixture().await;
        let catalog =
            LakeCatalog::with_source(Arc::new(fixture.source.clone()), fixture.engine.clone());
        catalog.refresh().await.unwrap();
        let old = catalog
            .resolve_snapshot(&fixture.table)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.version(), Version(7));
        let registration = registry::get(fixture.meta.as_ref(), &fixture.table)
            .await
            .unwrap()
            .unwrap();
        registry::set_version(
            fixture.meta.as_ref(),
            &fixture.table,
            &registration,
            Version(8),
        )
        .await
        .unwrap();

        catalog.invalidate_registration(&fixture.table).await;
        let current = catalog
            .resolve_snapshot(&fixture.table)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.version(), Version(8));
        assert_eq!(
            fixture.source.request_count(),
            3,
            "append invalidation uses one point resolve without a directory refresh"
        );
        fixture.shutdown().await;
    }
}
