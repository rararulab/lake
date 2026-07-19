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

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    routing::{any, get, head, post},
};
use datafusion::{
    arrow::{array::Int64Array, record_batch::RecordBatch},
    prelude::SessionContext,
};
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent,
    io::LocalFsStorageFactory,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder},
    spec::{NestedField, PrimitiveType, Schema, Type},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use lake_iceberg::{
    IcebergCatalog, IcebergCatalogConfig, IcebergError, IcebergOAuthOptions, IcebergRestAuth,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::Notify;

#[derive(Debug)]
struct RecordingCatalog {
    inner:           MemoryCatalog,
    namespace_lists: AtomicUsize,
    table_lists:     AtomicUsize,
    table_loads:     AtomicUsize,
    fail_load:       AtomicBool,
    load_gate:       Option<Arc<tokio::sync::Semaphore>>,
}

impl RecordingCatalog {
    fn namespace_lists(&self) -> usize { self.namespace_lists.load(Ordering::Relaxed) }

    fn table_lists(&self) -> usize { self.table_lists.load(Ordering::Relaxed) }

    fn table_loads(&self) -> usize { self.table_loads.load(Ordering::Relaxed) }
}

#[derive(Clone)]
struct RestRequests {
    config:     Arc<AtomicUsize>,
    namespace:  Arc<AtomicUsize>,
    unexpected: Arc<tokio::sync::Mutex<Vec<String>>>,
}

async fn rest_config(
    State(requests): State<RestRequests>,
) -> ([(&'static str, &'static str); 1], &'static str) {
    requests.config.fetch_add(1, Ordering::Relaxed);
    (
        [("content-type", "application/json")],
        r#"{"defaults":{},"overrides":{}}"#,
    )
}

async fn rest_namespace(State(requests): State<RestRequests>) -> StatusCode {
    requests.namespace.fetch_add(1, Ordering::Relaxed);
    StatusCode::NO_CONTENT
}

async fn rest_table() -> ([(&'static str, &'static str); 1], &'static str) {
    (
        [("content-type", "application/json")],
        include_str!("fixtures/rest_load_table_response.json"),
    )
}

#[derive(Clone)]
struct RestDataTable {
    table: Table,
}

async fn rest_data_config() -> ([(&'static str, &'static str); 1], &'static str) {
    (
        [("content-type", "application/json")],
        r#"{"defaults":{},"overrides":{}}"#,
    )
}

async fn rest_data_namespace() -> StatusCode { StatusCode::NO_CONTENT }

async fn rest_data_table(
    State(state): State<RestDataTable>,
) -> ([(&'static str, &'static str); 1], String) {
    let response = serde_json::json!({
        "metadata-location": state.table.metadata_location(),
        "metadata": state.table.metadata(),
        "config": {},
    });
    (
        [("content-type", "application/json")],
        serde_json::to_string(&response).expect("encode REST table response"),
    )
}

#[derive(Clone)]
struct BearerRestDataTable {
    table:      Table,
    bearer:     String,
    rejections: Arc<AtomicUsize>,
    oauth:      Option<OAuthRequest>,
}

#[derive(Clone)]
struct OAuthRequest {
    expected_form_fields: Arc<[String]>,
    exchanges:            Arc<AtomicUsize>,
}

#[derive(Clone)]
struct RefreshingOAuthRestDataTable {
    table:            Table,
    exchanges:        Arc<AtomicUsize>,
    table_rejections: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct FailedOAuthRenewalRestCatalog {
    exchanges:           Arc<AtomicUsize>,
    renewal_requests:    Arc<AtomicUsize>,
    table_requests:      Arc<AtomicUsize>,
    renewal_started:     Arc<tokio::sync::Notify>,
    release_first_retry: Arc<tokio::sync::Notify>,
}

impl RefreshingOAuthRestDataTable {
    fn authorized(&self, headers: &HeaderMap) -> bool {
        let expected = match self.exchanges.load(Ordering::Relaxed) {
            1 => "Bearer initial-oauth-access-token",
            2 => "Bearer refreshed-oauth-access-token",
            _ => return false,
        };
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == expected)
    }
}

impl FailedOAuthRenewalRestCatalog {
    fn initial_token_authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == "Bearer initial-oauth-access-token")
    }

    fn startup_authorized(&self, headers: &HeaderMap) -> bool {
        self.exchanges.load(Ordering::Relaxed) == 1 && self.initial_token_authorized(headers)
    }
}

impl BearerRestDataTable {
    fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == self.bearer)
    }
}

async fn bearer_rest_config(
    State(state): State<BearerRestDataTable>,
    headers: HeaderMap,
) -> Result<([(&'static str, &'static str); 1], &'static str), StatusCode> {
    if !state.authorized(&headers) {
        state.rejections.fetch_add(1, Ordering::Relaxed);
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok((
        [("content-type", "application/json")],
        r#"{"defaults":{},"overrides":{}}"#,
    ))
}

async fn bearer_rest_namespace(
    State(state): State<BearerRestDataTable>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if state.authorized(&headers) {
        Ok(StatusCode::NO_CONTENT)
    } else {
        state.rejections.fetch_add(1, Ordering::Relaxed);
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn bearer_rest_table(
    State(state): State<BearerRestDataTable>,
    headers: HeaderMap,
) -> Result<([(&'static str, &'static str); 1], String), StatusCode> {
    if !state.authorized(&headers) {
        state.rejections.fetch_add(1, Ordering::Relaxed);
        return Err(StatusCode::UNAUTHORIZED);
    }
    let response = serde_json::json!({
        "metadata-location": state.table.metadata_location(),
        "metadata": state.table.metadata(),
        "config": {},
    });
    Ok((
        [("content-type", "application/json")],
        serde_json::to_string(&response).expect("encode REST table response"),
    ))
}

async fn bearer_rest_oauth_token(
    State(state): State<BearerRestDataTable>,
    body: String,
) -> Result<([(&'static str, &'static str); 1], String), StatusCode> {
    let oauth = state.oauth.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    if !oauth
        .expected_form_fields
        .iter()
        .all(|field| body.split('&').any(|actual| actual == field))
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    oauth.exchanges.fetch_add(1, Ordering::Relaxed);
    let token = state
        .bearer
        .strip_prefix("Bearer ")
        .expect("test bearer has prefix");
    Ok((
        [("content-type", "application/json")],
        serde_json::json!({
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": 3600,
        })
        .to_string(),
    ))
}

async fn refreshing_oauth_config(
    State(state): State<RefreshingOAuthRestDataTable>,
    headers: HeaderMap,
) -> Result<([(&'static str, &'static str); 1], &'static str), StatusCode> {
    if !state.authorized(&headers) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok((
        [("content-type", "application/json")],
        r#"{"defaults":{},"overrides":{}}"#,
    ))
}

async fn refreshing_oauth_namespace(
    State(state): State<RefreshingOAuthRestDataTable>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    state
        .authorized(&headers)
        .then_some(StatusCode::NO_CONTENT)
        .ok_or(StatusCode::UNAUTHORIZED)
}

async fn refreshing_oauth_table(
    State(state): State<RefreshingOAuthRestDataTable>,
    headers: HeaderMap,
) -> Result<([(&'static str, &'static str); 1], String), StatusCode> {
    if !state.authorized(&headers) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if state.exchanges.load(Ordering::Relaxed) == 1 {
        state.table_rejections.fetch_add(1, Ordering::Relaxed);
        return Err(StatusCode::UNAUTHORIZED);
    }
    let response = serde_json::json!({
        "metadata-location": state.table.metadata_location(),
        "metadata": state.table.metadata(),
        "config": {},
    });
    Ok((
        [("content-type", "application/json")],
        serde_json::to_string(&response).expect("encode REST table response"),
    ))
}

async fn refreshing_oauth_token(
    State(state): State<RefreshingOAuthRestDataTable>,
) -> Result<([(&'static str, &'static str); 1], String), StatusCode> {
    let token = match state.exchanges.fetch_add(1, Ordering::Relaxed) {
        0 => "initial-oauth-access-token",
        1 => "refreshed-oauth-access-token",
        _ => return Err(StatusCode::TOO_MANY_REQUESTS),
    };
    Ok((
        [("content-type", "application/json")],
        serde_json::json!({
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": 60,
        })
        .to_string(),
    ))
}

async fn failed_oauth_renewal_config(
    State(state): State<FailedOAuthRenewalRestCatalog>,
    headers: HeaderMap,
) -> Result<([(&'static str, &'static str); 1], &'static str), StatusCode> {
    if !state.initial_token_authorized(&headers) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok((
        [("content-type", "application/json")],
        r#"{"defaults":{},"overrides":{}}"#,
    ))
}

async fn failed_oauth_renewal_namespace(
    State(state): State<FailedOAuthRenewalRestCatalog>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    state
        .startup_authorized(&headers)
        .then_some(StatusCode::NO_CONTENT)
        .ok_or(StatusCode::UNAUTHORIZED)
}

async fn failed_oauth_renewal_table(
    State(state): State<FailedOAuthRenewalRestCatalog>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if !state.initial_token_authorized(&headers) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state.table_requests.fetch_add(1, Ordering::Relaxed);
    Err(StatusCode::UNAUTHORIZED)
}

async fn failed_oauth_renewal_token(
    State(state): State<FailedOAuthRenewalRestCatalog>,
) -> Result<([(&'static str, &'static str); 1], String), StatusCode> {
    match state.exchanges.fetch_add(1, Ordering::Relaxed) {
        0 => Ok((
            [("content-type", "application/json")],
            serde_json::json!({
                "access_token": "initial-oauth-access-token",
                "token_type": "Bearer",
                "expires_in": 60,
            })
            .to_string(),
        )),
        _ => {
            if state.renewal_requests.fetch_add(1, Ordering::Relaxed) == 0 {
                state.renewal_started.notify_one();
                state.release_first_retry.notified().await;
            }
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

async fn start_failed_oauth_renewal_catalog(
    state: FailedOAuthRenewalRestCatalog,
) -> (Arc<IcebergCatalog>, tokio::task::JoinHandle<()>) {
    const CREDENTIAL: &str = "lake-query:lake-rest-oauth-secret";

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind failed-renewal OAuth REST catalog");
    let address = listener
        .local_addr()
        .expect("read failed-renewal OAuth REST catalog address");
    let app = Router::new()
        .route("/v1/config", get(failed_oauth_renewal_config))
        .route(
            "/v1/namespaces/analytics",
            head(failed_oauth_renewal_namespace),
        )
        .route(
            "/v1/namespaces/analytics/tables/{table}",
            get(failed_oauth_renewal_table),
        )
        .route("/oauth/token", post(failed_oauth_renewal_token))
        .with_state(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve failed-renewal OAuth REST catalog");
    });
    let auth = IcebergRestAuth::oauth_client_credentials(
        CREDENTIAL,
        IcebergOAuthOptions::builder()
            .oauth2_server_uri(format!("http://{address}/oauth/token"))
            .build(),
    )
    .expect("validate OAuth client credential");
    let catalog = Arc::new(
        IcebergCatalog::connect(
            IcebergCatalogConfig::try_new(
                &format!("http://{address}"),
                "file:///warehouse",
                ["analytics"],
            )
            .expect("build OAuth REST configuration")
            .with_rest_auth(auth),
        )
        .await
        .expect("connect OAuth REST catalog"),
    );
    (catalog, server)
}

async fn bearer_rest_echo_authorization_error(headers: HeaderMap) -> (StatusCode, String) {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("missing authorization");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({
            "error": {
                "message": authorization,
                "type": "test.error",
                "code": 500,
            }
        })
        .to_string(),
    )
}

async fn rest_unexpected(State(requests): State<RestRequests>, request: Request) -> StatusCode {
    requests
        .unexpected
        .lock()
        .await
        .push(format!("{} {}", request.method(), request.uri()));
    StatusCode::NOT_FOUND
}

#[async_trait]
impl Catalog for RecordingCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.namespace_lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.table_lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.table_loads.fetch_add(1, Ordering::Relaxed);
        if let Some(load_gate) = &self.load_gate {
            load_gate
                .acquire()
                .await
                .expect("test load gate must stay open")
                .forget();
        }
        if self.fail_load.load(Ordering::Relaxed) {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "test catalog is temporarily unavailable",
            ));
        }
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(
        &self,
        source: &TableIdent,
        destination: &TableIdent,
    ) -> IcebergResult<()> {
        self.inner.rename_table(source, destination).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await
    }
}

#[tokio::test]
async fn configured_namespace_cache_never_enumerates_unconfigured_catalog_state() {
    let (_warehouse, catalog) = recording_catalog_with_episodes(None).await;
    let config =
        IcebergCatalogConfig::try_new("https://catalog.example", "s3://warehouse", ["analytics"])
            .expect("build config")
            .with_cache_policy(Duration::ZERO, Duration::from_mins(1))
            .expect("configure immediate refresh for test");
    let federation = IcebergCatalog::from_catalog(config, catalog.clone());

    let snapshot = federation
        .resolve_snapshot("analytics", "episodes")
        .await
        .expect("resolve configured table");

    assert_eq!(snapshot.namespace(), "analytics");
    assert_eq!(snapshot.table(), "episodes");
    assert_eq!(catalog.namespace_lists(), 0);
    assert_eq!(catalog.table_lists(), 0);
    assert_eq!(catalog.table_loads(), 1);
    assert!(
        federation
            .resolve_snapshot("private", "secrets")
            .await
            .is_err()
    );
    assert_eq!(catalog.table_loads(), 1);

    let context = SessionContext::new();
    context
        .register_table(
            "episodes",
            snapshot
                .table_provider()
                .await
                .expect("build static provider"),
        )
        .expect("register static provider");
    let batches = context
        .sql("SELECT * FROM episodes")
        .await
        .expect("plan static provider")
        .collect()
        .await
        .expect("read static provider");
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));

    context.register_catalog("iceberg", federation.datafusion_catalog());
    let batches = context
        .sql("SELECT * FROM iceberg.analytics.episodes")
        .await
        .expect("plan external catalog table")
        .collect()
        .await
        .expect("read external catalog table");
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
    assert_eq!(catalog.namespace_lists(), 0);
    assert_eq!(catalog.table_lists(), 0);
    assert_eq!(catalog.table_loads(), 2);
    assert!(matches!(
        federation
            .resolve_snapshot_at("analytics", "episodes", 9_331)
            .await,
        Err(lake_iceberg::IcebergError::SnapshotUnavailable)
    ));
    catalog.fail_load.store(true, Ordering::Relaxed);
    let recovered = federation
        .resolve_snapshot("analytics", "episodes")
        .await
        .expect("keep the last successful snapshot when refresh fails");
    assert_eq!(recovered.snapshot_id(), snapshot.snapshot_id());
    assert_eq!(catalog.table_loads(), 4);
}

async fn recording_catalog_with_episodes(
    load_gate: Option<Arc<tokio::sync::Semaphore>>,
) -> (tempfile::TempDir, Arc<RecordingCatalog>) {
    let warehouse = tempfile::tempdir().expect("create warehouse");
    let inner = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_owned(),
                warehouse.path().display().to_string(),
            )]),
        )
        .await
        .expect("open memory catalog");
    let namespace = NamespaceIdent::new("analytics".to_owned());
    inner
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    inner
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("episodes".to_owned())
                .location(format!("{}/episodes", warehouse.path().display()))
                .schema(
                    Schema::builder()
                        .with_schema_id(0)
                        .with_fields(vec![
                            NestedField::required(
                                1,
                                "episode_id",
                                Type::Primitive(PrimitiveType::Long),
                            )
                            .into(),
                        ])
                        .build()
                        .expect("build schema"),
                )
                .properties(HashMap::new())
                .build(),
        )
        .await
        .expect("create table");
    let catalog = Arc::new(RecordingCatalog {
        inner,
        namespace_lists: AtomicUsize::new(0),
        table_lists: AtomicUsize::new(0),
        table_loads: AtomicUsize::new(0),
        fail_load: AtomicBool::new(false),
        load_gate,
    });
    (warehouse, catalog)
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_metrics_report_load_and_cache_hit_without_identity_labels() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    lake_iceberg::describe_metrics();

    let (_warehouse, catalog) = recording_catalog_with_episodes(None).await;
    let federation = IcebergCatalog::from_catalog(
        IcebergCatalogConfig::try_new("https://catalog.example", "s3://warehouse", ["analytics"])
            .expect("build catalog config"),
        catalog,
    );

    federation
        .resolve_snapshot("analytics", "episodes")
        .await
        .expect("load external snapshot");
    federation
        .resolve_snapshot("analytics", "episodes")
        .await
        .expect("reuse cached snapshot");

    let rendered = handle.render();
    for expected in [
        "lake_iceberg_snapshot_resolution_total{outcome=\"loaded\"} 1",
        "lake_iceberg_snapshot_resolution_total{outcome=\"cache_hit\"} 1",
        "lake_iceberg_catalog_operation_total{operation=\"table_load\",outcome=\"success\"} 1",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected}:\n{rendered}"
        );
    }
    for forbidden in ["analytics", "episodes", "catalog.example", "warehouse"] {
        assert!(
            !rendered.contains(forbidden),
            "identity must not appear in metric output: {forbidden}\n{rendered}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_metrics_report_external_load_failure() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    lake_iceberg::describe_metrics();

    let (_warehouse, catalog) = recording_catalog_with_episodes(None).await;
    catalog.fail_load.store(true, Ordering::Relaxed);
    let federation = IcebergCatalog::from_catalog(
        IcebergCatalogConfig::try_new("https://catalog.example", "s3://warehouse", ["analytics"])
            .expect("build catalog config"),
        catalog.clone(),
    );

    assert!(matches!(
        federation.resolve_snapshot("analytics", "episodes").await,
        Err(IcebergError::Catalog)
    ));
    assert_eq!(catalog.table_loads(), 1, "metrics must not add a retry");

    let rendered = handle.render();
    for expected in [
        "lake_iceberg_snapshot_resolution_total{outcome=\"error\"} 1",
        "lake_iceberg_catalog_operation_total{operation=\"table_load\",outcome=\"error\"} 1",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected}:\n{rendered}"
        );
    }
}

#[tokio::test]
async fn distinct_snapshot_loads_are_bounded_and_release_after_cancellation() {
    const MAX_DISTINCT_PENDING_LOADS: usize = 64;

    let load_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (_warehouse, catalog) = recording_catalog_with_episodes(Some(load_gate.clone())).await;
    let config =
        IcebergCatalogConfig::try_new("https://catalog.example", "s3://warehouse", ["analytics"])
            .expect("build config")
            .with_cache_policy(Duration::ZERO, Duration::from_mins(1))
            .expect("configure immediate refresh for test");
    let federation = Arc::new(IcebergCatalog::from_catalog(config, catalog.clone()));
    let mut leaders = tokio::task::JoinSet::new();
    for index in 0..MAX_DISTINCT_PENDING_LOADS {
        let federation = federation.clone();
        let table = if index == 0 {
            "episodes".to_owned()
        } else {
            format!("adversary_{index}")
        };
        leaders.spawn(async move { federation.resolve_snapshot("analytics", &table).await });
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while catalog.table_loads() < MAX_DISTINCT_PENDING_LOADS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("every admitted distinct key must begin its external load");

    let follower_started = Arc::new(Notify::new());
    let follower_ready = follower_started.clone();
    let follower_catalog = federation.clone();
    let follower = tokio::spawn(async move {
        follower_ready.notify_one();
        follower_catalog
            .resolve_snapshot("analytics", "episodes")
            .await
    });
    follower_started.notified().await;
    tokio::task::yield_now().await;
    assert!(
        !follower.is_finished(),
        "a matching key must remain a follower when distinct-key admission is full"
    );

    let rejected = tokio::time::timeout(
        Duration::from_millis(100),
        federation.resolve_snapshot("analytics", "one_too_many"),
    )
    .await
    .expect("a full distinct-key admission limit must reject without waiting");
    assert!(matches!(rejected, Err(IcebergError::Catalog)));
    assert_eq!(
        catalog.table_loads(),
        MAX_DISTINCT_PENDING_LOADS,
        "a rejected distinct key must not start external catalog I/O"
    );

    leaders.abort_all();
    while let Some(result) = leaders.join_next().await {
        assert!(result.is_err(), "blocked leader must be cancelled");
    }

    load_gate.add_permits(1);
    let recovered = tokio::time::timeout(Duration::from_secs(1), follower)
        .await
        .expect("cancelling leaders must release the distinct-key admission")
        .expect("matching follower task must not panic")
        .expect("a released admission slot must load the configured table");
    assert_eq!(recovered.table(), "episodes");
}

#[tokio::test]
async fn concurrent_snapshot_refreshes_share_one_external_load() {
    const CONCURRENT_LOADS: usize = 8;

    let load_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (_warehouse, catalog) = recording_catalog_with_episodes(Some(load_gate.clone())).await;
    let config =
        IcebergCatalogConfig::try_new("https://catalog.example", "s3://warehouse", ["analytics"])
            .expect("build config")
            .with_cache_policy(Duration::ZERO, Duration::from_mins(1))
            .expect("configure immediate refresh for test");
    let federation = Arc::new(IcebergCatalog::from_catalog(config, catalog.clone()));
    let start = Arc::new(tokio::sync::Barrier::new(CONCURRENT_LOADS + 1));
    let mut loads = tokio::task::JoinSet::new();
    for _ in 0..CONCURRENT_LOADS {
        let federation = federation.clone();
        let start = start.clone();
        loads.spawn(async move {
            start.wait().await;
            federation.resolve_snapshot("analytics", "episodes").await
        });
    }
    start.wait().await;
    for _ in 0..CONCURRENT_LOADS {
        tokio::task::yield_now().await;
    }
    let observed_loads = catalog.table_loads();
    load_gate.add_permits(CONCURRENT_LOADS);

    while let Some(result) = loads.join_next().await {
        let snapshot = result
            .expect("concurrent snapshot task must not panic")
            .expect("concurrent snapshot resolution must succeed");
        assert_eq!(snapshot.table(), "episodes");
    }
    assert_eq!(
        observed_loads, 1,
        "one cache miss must issue only one external table load"
    );
}

#[tokio::test]
async fn cancelled_snapshot_leader_allows_a_new_load() {
    let load_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (_warehouse, catalog) = recording_catalog_with_episodes(Some(load_gate.clone())).await;
    let config =
        IcebergCatalogConfig::try_new("https://catalog.example", "s3://warehouse", ["analytics"])
            .expect("build config")
            .with_cache_policy(Duration::ZERO, Duration::from_mins(1))
            .expect("configure immediate refresh for test");
    let federation = Arc::new(IcebergCatalog::from_catalog(config, catalog.clone()));
    let leader_catalog = federation.clone();
    let leader = tokio::spawn(async move {
        leader_catalog
            .resolve_snapshot("analytics", "episodes")
            .await
    });
    while catalog.table_loads() == 0 {
        tokio::task::yield_now().await;
    }

    leader.abort();
    assert!(leader.await.is_err(), "blocked leader must be cancelled");

    load_gate.add_permits(1);
    let snapshot = tokio::time::timeout(
        Duration::from_secs(1),
        federation.resolve_snapshot("analytics", "episodes"),
    )
    .await
    .expect("a cancelled leader must not strand the next caller")
    .expect("replacement snapshot load must succeed");
    assert_eq!(snapshot.table(), "episodes");
    assert_eq!(catalog.table_loads(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_snapshot_leader_metrics_preserve_handoff_visibility() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    lake_iceberg::describe_metrics();

    let load_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (_warehouse, catalog) = recording_catalog_with_episodes(Some(load_gate.clone())).await;
    let config = IcebergCatalogConfig::try_new(
        "https://catalog.example",
        "s3://warehouse/lake-iceberg-test-secret",
        ["analytics"],
    )
    .expect("build config")
    .with_cache_policy(Duration::ZERO, Duration::from_mins(1))
    .expect("configure immediate refresh for test");
    let federation = Arc::new(IcebergCatalog::from_catalog(config, catalog.clone()));
    let leader_catalog = federation.clone();
    let leader = tokio::spawn(async move {
        leader_catalog
            .resolve_snapshot("analytics", "episodes")
            .await
    });
    while catalog.table_loads() == 0 {
        tokio::task::yield_now().await;
    }

    let (follower_started, follower_joined) = tokio::sync::oneshot::channel();
    let follower_catalog = federation.clone();
    let follower = tokio::spawn(async move {
        follower_started
            .send(())
            .expect("test must observe the waiting follower");
        follower_catalog
            .resolve_snapshot("analytics", "episodes")
            .await
    });
    follower_joined
        .await
        .expect("follower task must start while the leader load is pending");

    leader.abort();
    assert!(leader.await.is_err(), "blocked leader must be cancelled");

    load_gate.add_permits(1);
    let snapshot = tokio::time::timeout(Duration::from_secs(1), follower)
        .await
        .expect("an existing follower must take over after the leader is cancelled")
        .expect("follower task must not panic")
        .expect("follower replacement snapshot load must succeed");
    assert_eq!(snapshot.table(), "episodes");
    assert_eq!(catalog.table_loads(), 2);

    let rendered = handle.render();
    for expected in [
        "lake_iceberg_snapshot_resolution_total{outcome=\"cancelled\"} 1",
        "lake_iceberg_snapshot_resolution_total{outcome=\"loaded\"} 1",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected}:\n{rendered}"
        );
    }
    for forbidden in [
        "analytics",
        "episodes",
        "catalog.example",
        "warehouse",
        "lake-iceberg-test-secret",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "identity must not appear in metric output: {forbidden}\n{rendered}"
        );
    }
}

#[tokio::test]
async fn cancelled_snapshot_leader_releases_existing_follower() {
    let load_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (_warehouse, catalog) = recording_catalog_with_episodes(Some(load_gate.clone())).await;
    let config =
        IcebergCatalogConfig::try_new("https://catalog.example", "s3://warehouse", ["analytics"])
            .expect("build config")
            .with_cache_policy(Duration::ZERO, Duration::from_mins(1))
            .expect("configure immediate refresh for test");
    let federation = Arc::new(IcebergCatalog::from_catalog(config, catalog.clone()));

    let leader_catalog = federation.clone();
    let leader = tokio::spawn(async move {
        leader_catalog
            .resolve_snapshot("analytics", "episodes")
            .await
    });
    while catalog.table_loads() == 0 {
        tokio::task::yield_now().await;
    }

    let (follower_started, follower_joined) = tokio::sync::oneshot::channel();
    let follower_catalog = federation.clone();
    let follower = tokio::spawn(async move {
        follower_started
            .send(())
            .expect("test must observe the waiting follower");
        follower_catalog
            .resolve_snapshot("analytics", "episodes")
            .await
    });
    follower_joined
        .await
        .expect("follower task must start while the leader load is pending");

    leader.abort();
    assert!(leader.await.is_err(), "blocked leader must be cancelled");
    load_gate.add_permits(1);
    let snapshot = tokio::time::timeout(Duration::from_secs(1), follower)
        .await
        .expect("an existing follower must take over after the leader is cancelled")
        .expect("follower task must not panic")
        .expect("follower replacement snapshot load must succeed");
    assert_eq!(snapshot.table(), "episodes");
    assert_eq!(
        catalog.table_loads(),
        2,
        "the follower must replace the cancelled leader with one exact table load"
    );
}

#[tokio::test]
async fn rest_catalog_connect_warms_each_configured_namespace() {
    let requests = RestRequests {
        config:     Arc::new(AtomicUsize::new(0)),
        namespace:  Arc::new(AtomicUsize::new(0)),
        unexpected: Arc::new(tokio::sync::Mutex::new(Vec::new())),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind REST catalog");
    let address = listener.local_addr().expect("read REST catalog address");
    let app = Router::new()
        .route("/v1/config", get(rest_config))
        .route("/v1/namespaces/analytics", head(rest_namespace))
        .route("/v1/namespaces/analytics/tables/episodes", get(rest_table))
        .fallback(any(rest_unexpected))
        .with_state(requests.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve REST catalog");
    });

    let config = IcebergCatalogConfig::try_new(
        &format!("http://{address}"),
        "s3://warehouse",
        ["analytics"],
    )
    .expect("build config");
    let catalog = IcebergCatalog::connect(config).await;

    assert!(
        catalog.is_ok(),
        "connect REST catalog: {catalog:?}; unexpected requests: {:?}",
        requests.unexpected.lock().await,
    );
    let catalog = catalog.expect("checked above");

    assert_eq!(requests.config.load(Ordering::Relaxed), 1);
    assert_eq!(requests.namespace.load(Ordering::Relaxed), 1);
    assert!(format!("{catalog:?}").contains("IcebergCatalog"));
    let snapshot = catalog
        .resolve_snapshot("analytics", "episodes")
        .await
        .expect("load REST catalog table with a storage factory");
    assert_eq!(snapshot.snapshot_id(), Some(3_497_810_964_824_022_504));
    server.abort();
}

#[tokio::test]
async fn rest_catalog_timeout_bounds_unresponsive_startup() {
    #[derive(Clone)]
    struct DelayedConfig {
        request_started: Arc<Notify>,
    }

    async fn delayed_config(State(state): State<DelayedConfig>) -> StatusCode {
        state.request_started.notify_one();
        std::future::pending().await
    }

    let request_started = Arc::new(Notify::new());
    let delayed_config_state = DelayedConfig {
        request_started: request_started.clone(),
    };
    let wait_for_request = request_started.notified();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind delayed REST catalog");
    let address = listener
        .local_addr()
        .expect("read delayed REST catalog address");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/v1/config", get(delayed_config))
                .with_state(delayed_config_state),
        )
        .await
        .expect("serve delayed REST catalog");
    });

    let config = IcebergCatalogConfig::try_new(
        &format!("http://{address}"),
        "s3://warehouse",
        ["analytics"],
    )
    .expect("build config")
    .with_rest_timeout(Duration::from_millis(25))
    .expect("short REST timeout is valid");
    let connect = tokio::spawn(IcebergCatalog::connect(config));

    wait_for_request.await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(26)).await;
    let error = connect
        .await
        .expect("catalog startup task must not panic")
        .expect_err("unresponsive REST config must time out");

    assert!(matches!(error, lake_iceberg::IcebergError::Catalog));
    server.abort();
}

#[test]
fn rest_timeout_rejects_zero_and_excessive_values() {
    let config = IcebergCatalogConfig::try_new(
        "https://catalog.example.test",
        "s3://warehouse",
        ["analytics"],
    )
    .expect("build config");

    for timeout in [Duration::ZERO, Duration::from_secs(61)] {
        assert!(matches!(
            config.clone().with_rest_timeout(timeout),
            Err(lake_iceberg::IcebergError::InvalidRestTimeout)
        ));
    }
}

async fn populated_local_table() -> (tempfile::TempDir, Table) {
    let warehouse = tempfile::tempdir().expect("create Iceberg warehouse");
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    warehouse.path().display().to_string(),
                )]),
            )
            .await
            .expect("open Iceberg memory catalog"),
    );
    let namespace = NamespaceIdent::new("analytics".to_owned());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create Iceberg namespace");
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("episodes".to_owned())
                .location(format!("file://{}/episodes", warehouse.path().display()))
                .schema(
                    Schema::builder()
                        .with_schema_id(0)
                        .with_fields(vec![
                            NestedField::required(
                                1,
                                "episode_id",
                                Type::Primitive(PrimitiveType::Long),
                            )
                            .into(),
                        ])
                        .build()
                        .expect("build schema"),
                )
                .properties(HashMap::new())
                .build(),
        )
        .await
        .expect("create Iceberg table");
    let table = catalog
        .load_table(&TableIdent::new(namespace.clone(), "episodes".to_owned()))
        .await
        .expect("load Iceberg table");
    let arrow_schema = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .expect("convert Iceberg schema to Arrow"),
    );
    let location_generator =
        DefaultLocationGenerator::new(table.metadata()).expect("create data location generator");
    let parquet_writer = ParquetWriterBuilder::new(
        datafusion::parquet::file::properties::WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer,
        table.file_io().clone(),
        location_generator,
        DefaultFileNameGenerator::new(
            "rest-query".to_owned(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        ),
    );
    let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer)
        .build(None)
        .await
        .expect("build Iceberg data writer");
    data_file_writer
        .write(
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![42_i64]))])
                .expect("build Iceberg data batch"),
        )
        .await
        .expect("write Iceberg data batch");
    let data_files = data_file_writer
        .close()
        .await
        .expect("close Iceberg data writer");
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .fast_append()
        .add_data_files(data_files)
        .apply(transaction)
        .expect("apply Iceberg append");
    transaction
        .commit(catalog.as_ref())
        .await
        .expect("commit Iceberg append");
    let table = catalog
        .load_table(&TableIdent::new(namespace, "episodes".to_owned()))
        .await
        .expect("load populated Iceberg table");
    (warehouse, table)
}

#[tokio::test]
async fn rest_catalog_table_is_queryable_through_iceberg_catalog() {
    let (_warehouse, table) = populated_local_table().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind REST catalog");
    let address = listener.local_addr().expect("read REST catalog address");
    let app = Router::new()
        .route("/v1/config", get(rest_data_config))
        .route("/v1/namespaces/analytics", head(rest_data_namespace))
        .route(
            "/v1/namespaces/analytics/tables/episodes",
            get(rest_data_table),
        )
        .with_state(RestDataTable { table });
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve REST catalog");
    });
    let catalog = IcebergCatalog::connect(
        IcebergCatalogConfig::try_new(
            &format!("http://{address}"),
            "file:///warehouse",
            ["analytics"],
        )
        .expect("build REST configuration"),
    )
    .await
    .expect("connect REST catalog");
    let context = SessionContext::new();
    context.register_catalog("iceberg", catalog.datafusion_catalog());
    let batches = context
        .sql("SELECT episode_id FROM iceberg.analytics.episodes")
        .await
        .expect("plan Iceberg REST table")
        .collect()
        .await
        .expect("read Iceberg REST table");
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Iceberg episode ID column")
                .iter()
                .flatten()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![42]);
    server.abort();
}

#[tokio::test]
async fn rest_catalog_static_bearer_auth_is_runtime_only() {
    const TOKEN: &str = "lake-rest-static-token";

    let (_warehouse, table) = populated_local_table().await;
    let state = BearerRestDataTable {
        table,
        bearer: format!("Bearer {TOKEN}"),
        rejections: Arc::new(AtomicUsize::new(0)),
        oauth: None,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind protected REST catalog");
    let address = listener
        .local_addr()
        .expect("read protected REST catalog address");
    let app = Router::new()
        .route("/v1/config", get(bearer_rest_config))
        .route("/v1/namespaces/analytics", head(bearer_rest_namespace))
        .route(
            "/v1/namespaces/analytics/tables/episodes",
            get(bearer_rest_table),
        )
        .with_state(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve protected REST catalog");
    });
    let config = IcebergCatalogConfig::try_new(
        &format!("http://{address}"),
        "file:///warehouse",
        ["analytics"],
    )
    .expect("build protected REST configuration");

    assert!(
        IcebergCatalog::connect(config.clone()).await.is_err(),
        "unauthenticated REST catalog startup must fail"
    );
    assert_eq!(state.rejections.load(Ordering::Relaxed), 1);

    let config = config
        .with_rest_auth(IcebergRestAuth::bearer_token(TOKEN).expect("validate static REST token"));
    assert!(
        !format!("{config:?}").contains(TOKEN),
        "REST token must be redacted from Debug output"
    );
    let catalog = IcebergCatalog::connect(config)
        .await
        .expect("connect authenticated REST catalog");
    let context = SessionContext::new();
    context.register_catalog("iceberg", catalog.datafusion_catalog());
    let batches = context
        .sql("SELECT episode_id FROM iceberg.analytics.episodes")
        .await
        .expect("plan protected Iceberg REST table")
        .collect()
        .await
        .expect("read protected Iceberg REST table");
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Iceberg episode ID column")
                .iter()
                .flatten()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![42]);
    server.abort();
}

#[tokio::test]
async fn rest_catalog_failures_redact_runtime_bearer_tokens() {
    const TOKEN: &str = "lake-rest-static-token";

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind failing REST catalog");
    let address = listener
        .local_addr()
        .expect("read failing REST catalog address");
    let app = Router::new().route("/v1/config", get(bearer_rest_echo_authorization_error));
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve failing REST catalog");
    });
    let config = IcebergCatalogConfig::try_new(
        &format!("http://{address}"),
        "file:///warehouse",
        ["analytics"],
    )
    .expect("build failing REST configuration")
    .with_rest_auth(IcebergRestAuth::bearer_token(TOKEN).expect("validate static REST token"));

    let error = IcebergCatalog::connect(config)
        .await
        .expect_err("catalog error must be returned");
    assert!(
        !error.to_string().contains(TOKEN) && !format!("{error:?}").contains(TOKEN),
        "external REST error payload must not leak the bearer token"
    );
    server.abort();
}

#[tokio::test]
async fn rest_catalog_oauth_client_credentials_are_runtime_only() {
    const CREDENTIAL: &str = "lake-query:lake-rest-oauth-secret";
    const SECRET: &str = "lake-rest-oauth-secret";
    const TOKEN: &str = "lake-rest-oauth-access-token";

    let (_warehouse, table) = populated_local_table().await;
    let state = BearerRestDataTable {
        table,
        bearer: format!("Bearer {TOKEN}"),
        rejections: Arc::new(AtomicUsize::new(0)),
        oauth: Some(OAuthRequest {
            expected_form_fields: Arc::from([
                "grant_type=client_credentials".to_owned(),
                "client_id=lake-query".to_owned(),
                format!("client_secret={SECRET}"),
                "scope=lake-catalog".to_owned(),
            ]),
            exchanges:            Arc::new(AtomicUsize::new(0)),
        }),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind OAuth REST catalog");
    let address = listener
        .local_addr()
        .expect("read OAuth REST catalog address");
    let app = Router::new()
        .route("/v1/config", get(bearer_rest_config))
        .route("/v1/namespaces/analytics", head(bearer_rest_namespace))
        .route(
            "/v1/namespaces/analytics/tables/episodes",
            get(bearer_rest_table),
        )
        .route("/oauth/token", post(bearer_rest_oauth_token))
        .with_state(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve OAuth REST catalog");
    });
    let auth = IcebergRestAuth::oauth_client_credentials(
        CREDENTIAL,
        IcebergOAuthOptions::builder()
            .oauth2_server_uri(format!("http://{address}/oauth/token"))
            .scope("lake-catalog")
            .build(),
    )
    .expect("validate OAuth client credential");
    let config = IcebergCatalogConfig::try_new(
        &format!("http://{address}"),
        "file:///warehouse",
        ["analytics"],
    )
    .expect("build OAuth REST configuration")
    .with_rest_auth(auth);
    assert!(
        !format!("{config:?}").contains(SECRET),
        "OAuth client secret must be redacted from Debug output"
    );

    let catalog = IcebergCatalog::connect(config)
        .await
        .expect("connect OAuth REST catalog");
    let context = SessionContext::new();
    context.register_catalog("iceberg", catalog.datafusion_catalog());
    let batches = context
        .sql("SELECT episode_id FROM iceberg.analytics.episodes")
        .await
        .expect("plan OAuth-protected Iceberg REST table")
        .collect()
        .await
        .expect("read OAuth-protected Iceberg REST table");
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Iceberg episode ID column")
                .iter()
                .flatten()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![42]);
    assert_eq!(
        state
            .oauth
            .as_ref()
            .expect("OAuth test state")
            .exchanges
            .load(Ordering::Relaxed),
        1,
        "client credentials should create one cached REST session token"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn rest_catalog_oauth_expiry_regenerates_once_for_exact_table_load() {
    const CREDENTIAL: &str = "lake-query:lake-rest-oauth-secret";

    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    lake_iceberg::describe_metrics();

    let (_warehouse, table) = populated_local_table().await;
    let state = RefreshingOAuthRestDataTable {
        table,
        exchanges: Arc::new(AtomicUsize::new(0)),
        table_rejections: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind refreshable OAuth REST catalog");
    let address = listener
        .local_addr()
        .expect("read refreshable OAuth REST catalog address");
    let app = Router::new()
        .route("/v1/config", get(refreshing_oauth_config))
        .route("/v1/namespaces/analytics", head(refreshing_oauth_namespace))
        .route(
            "/v1/namespaces/analytics/tables/episodes",
            get(refreshing_oauth_table),
        )
        .route("/oauth/token", post(refreshing_oauth_token))
        .with_state(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve refreshable OAuth REST catalog");
    });
    let auth = IcebergRestAuth::oauth_client_credentials(
        CREDENTIAL,
        IcebergOAuthOptions::builder()
            .oauth2_server_uri(format!("http://{address}/oauth/token"))
            .build(),
    )
    .expect("validate OAuth client credential");
    let catalog = IcebergCatalog::connect(
        IcebergCatalogConfig::try_new(
            &format!("http://{address}"),
            "file:///warehouse",
            ["analytics"],
        )
        .expect("build OAuth REST configuration")
        .with_rest_auth(auth),
    )
    .await
    .expect("connect OAuth REST catalog");

    let snapshot = catalog
        .resolve_snapshot("analytics", "episodes")
        .await
        .expect("expired OAuth session must refresh once and retry its exact table lookup");
    assert_eq!(snapshot.table(), "episodes");
    assert_eq!(state.table_rejections.load(Ordering::Relaxed), 1);
    assert_eq!(
        state.exchanges.load(Ordering::Relaxed),
        2,
        "one initial exchange plus one bounded renewal is expected"
    );
    let rendered = handle.render();
    for expected in [
        "lake_iceberg_oauth_refresh_total{outcome=\"started\"} 1",
        "lake_iceberg_oauth_refresh_total{outcome=\"success\"} 1",
        "lake_iceberg_catalog_operation_total{operation=\"table_load\",outcome=\"error\"} 1",
        "lake_iceberg_catalog_operation_total{operation=\"table_load\",outcome=\"success\"} 1",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected}:\n{rendered}"
        );
    }
    for forbidden in ["analytics", "episodes", "initial-oauth-access-token"] {
        assert!(
            !rendered.contains(forbidden),
            "identity must not appear in metric output: {forbidden}\n{rendered}"
        );
    }
    server.abort();
}

#[tokio::test]
async fn concurrent_oauth_expiry_single_flights_table_load_and_shares_session_renewal() {
    const CREDENTIAL: &str = "lake-query:lake-rest-oauth-secret";
    const CONCURRENT_LOADS: usize = 8;

    let (_warehouse, table) = populated_local_table().await;
    let state = RefreshingOAuthRestDataTable {
        table,
        exchanges: Arc::new(AtomicUsize::new(0)),
        table_rejections: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind concurrent refreshable OAuth REST catalog");
    let address = listener
        .local_addr()
        .expect("read concurrent refreshable OAuth REST catalog address");
    let app = Router::new()
        .route("/v1/config", get(refreshing_oauth_config))
        .route("/v1/namespaces/analytics", head(refreshing_oauth_namespace))
        .route(
            "/v1/namespaces/analytics/tables/episodes",
            get(refreshing_oauth_table),
        )
        .route("/oauth/token", post(refreshing_oauth_token))
        .with_state(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve concurrent refreshable OAuth REST catalog");
    });
    let auth = IcebergRestAuth::oauth_client_credentials(
        CREDENTIAL,
        IcebergOAuthOptions::builder()
            .oauth2_server_uri(format!("http://{address}/oauth/token"))
            .build(),
    )
    .expect("validate OAuth client credential");
    let catalog = Arc::new(
        IcebergCatalog::connect(
            IcebergCatalogConfig::try_new(
                &format!("http://{address}"),
                "file:///warehouse",
                ["analytics"],
            )
            .expect("build OAuth REST configuration")
            .with_rest_auth(auth),
        )
        .await
        .expect("connect OAuth REST catalog"),
    );

    let mut loads = tokio::task::JoinSet::new();
    let start = Arc::new(tokio::sync::Barrier::new(CONCURRENT_LOADS + 1));
    for _ in 0..CONCURRENT_LOADS {
        let catalog = catalog.clone();
        let start = start.clone();
        loads.spawn(async move {
            start.wait().await;
            catalog.resolve_snapshot("analytics", "episodes").await
        });
    }
    start.wait().await;
    while let Some(result) = loads.join_next().await {
        result
            .expect("concurrent load task must not panic")
            .expect("each concurrent expired OAuth load must succeed after the shared renewal");
    }

    assert_eq!(
        state.table_rejections.load(Ordering::Relaxed),
        1,
        "one single-flight table load should trigger one expired-session response"
    );
    assert_eq!(
        state.exchanges.load(Ordering::Relaxed),
        2,
        "one initial exchange plus one shared renewal is expected"
    );
    server.abort();
}

#[tokio::test]
async fn concurrent_oauth_refresh_failure_is_single_flight() {
    const CONCURRENT_READS: usize = 8;

    let state = FailedOAuthRenewalRestCatalog {
        exchanges:           Arc::new(AtomicUsize::new(0)),
        renewal_requests:    Arc::new(AtomicUsize::new(0)),
        table_requests:      Arc::new(AtomicUsize::new(0)),
        renewal_started:     Arc::new(tokio::sync::Notify::new()),
        release_first_retry: Arc::new(tokio::sync::Notify::new()),
    };
    let (catalog, server) = start_failed_oauth_renewal_catalog(state.clone()).await;

    let wait_for_renewal = state.renewal_started.notified();
    let first_catalog = catalog.clone();
    let first =
        tokio::spawn(async move { first_catalog.resolve_snapshot("analytics", "first").await });
    wait_for_renewal.await;

    let mut followers = tokio::task::JoinSet::new();
    for table in [
        "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth",
    ] {
        let catalog = catalog.clone();
        followers.spawn(async move { catalog.resolve_snapshot("analytics", table).await });
    }
    let all_table_requests = tokio::time::timeout(Duration::from_secs(1), async {
        while state.table_requests.load(Ordering::Relaxed) < CONCURRENT_READS {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        all_table_requests.is_ok(),
        "all readers must reach their bounded table loads before the renewal is released; \
         observed {}",
        state.table_requests.load(Ordering::Relaxed)
    );
    for _ in 0..CONCURRENT_READS {
        tokio::task::yield_now().await;
    }
    state.release_first_retry.notify_one();

    assert!(matches!(
        first.await.expect("first reader must not panic"),
        Err(IcebergError::Catalog)
    ));
    while let Some(result) = followers.join_next().await {
        assert!(matches!(
            result.expect("follower must not panic"),
            Err(IcebergError::Catalog)
        ));
    }
    assert_eq!(
        state.renewal_requests.load(Ordering::Relaxed),
        1,
        "concurrent readers must share the failed renewal result"
    );
    assert_eq!(
        state.exchanges.load(Ordering::Relaxed),
        2,
        "one startup exchange and one failed renewal are expected"
    );
    server.abort();
}

#[tokio::test]
async fn cancelled_oauth_renewal_leader_releases_follower() {
    let state = FailedOAuthRenewalRestCatalog {
        exchanges:           Arc::new(AtomicUsize::new(0)),
        renewal_requests:    Arc::new(AtomicUsize::new(0)),
        table_requests:      Arc::new(AtomicUsize::new(0)),
        renewal_started:     Arc::new(tokio::sync::Notify::new()),
        release_first_retry: Arc::new(tokio::sync::Notify::new()),
    };
    let (catalog, server) = start_failed_oauth_renewal_catalog(state.clone()).await;

    let wait_for_renewal = state.renewal_started.notified();
    let leader_catalog = catalog.clone();
    let leader =
        tokio::spawn(async move { leader_catalog.resolve_snapshot("analytics", "leader").await });
    wait_for_renewal.await;

    let follower_catalog = catalog.clone();
    let follower = tokio::spawn(async move {
        follower_catalog
            .resolve_snapshot("analytics", "follower")
            .await
    });
    let follower_reached_failed_read = tokio::time::timeout(Duration::from_secs(1), async {
        while state.table_requests.load(Ordering::Relaxed) < 2 {
            tokio::task::yield_now().await;
        }
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        follower_reached_failed_read.is_ok(),
        "the follower must join while the leader renewal is pending; observed {} table reads",
        state.table_requests.load(Ordering::Relaxed)
    );

    leader.abort();
    assert!(
        leader
            .await
            .expect_err("cancelled leader must not complete")
            .is_cancelled()
    );
    let follower_result = tokio::time::timeout(Duration::from_secs(1), follower)
        .await
        .expect("follower must be released after the renewal leader is cancelled")
        .expect("follower task must not panic");
    assert!(matches!(follower_result, Err(IcebergError::Catalog)));
    assert_eq!(
        state.renewal_requests.load(Ordering::Relaxed),
        1,
        "cancelling the leader must release the existing follower, not start another renewal"
    );
    assert_eq!(
        state.exchanges.load(Ordering::Relaxed),
        2,
        "one startup exchange and one cancelled renewal are expected"
    );
    server.abort();
}
