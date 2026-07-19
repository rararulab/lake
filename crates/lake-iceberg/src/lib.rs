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

//! Read-only, snapshot-pinned federation for external Apache Iceberg tables.

use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent, table::Table};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};
use iceberg_datafusion::IcebergStaticTableProvider;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use snafu::Snafu;
use tokio::sync::Mutex as AsyncMutex;
use url::{Host, Url};

const DEFAULT_CACHE_FRESHNESS: Duration = Duration::from_secs(5);
const DEFAULT_CACHE_STALE_IF_ERROR: Duration = Duration::from_mins(1);
const DEFAULT_REST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REST_TIMEOUT: Duration = Duration::from_mins(1);
const SNAPSHOT_CACHE_CAPACITY: usize = 10_000;
const MAX_REST_SECRET_BYTES: usize = 8 * 1024;

/// Errors returned while validating external Iceberg catalog configuration.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum IcebergError {
    /// The REST endpoint is not a credential-free TLS or loopback-development
    /// URL.
    #[snafu(display("Iceberg REST endpoint is invalid"))]
    InvalidEndpoint,
    /// The configured warehouse is blank.
    #[snafu(display("Iceberg warehouse is invalid"))]
    InvalidWarehouse,
    /// A configured namespace is not a single SQL identifier segment.
    #[snafu(display("Iceberg namespace is invalid"))]
    InvalidNamespace,
    /// A configured namespace appears more than once.
    #[snafu(display("Iceberg namespaces contain a duplicate"))]
    DuplicateNamespace,
    /// External snapshot cache freshness and stale-if-error bounds are invalid.
    #[snafu(display("Iceberg snapshot cache policy is invalid"))]
    InvalidCachePolicy,
    /// The external REST request deadline is zero or exceeds Lake's bound.
    #[snafu(display("Iceberg REST request timeout is invalid"))]
    InvalidRestTimeout,
    /// REST authentication configuration is malformed.
    #[snafu(display("Iceberg REST authentication is invalid"))]
    InvalidRestAuth,
    /// A table is outside the configured namespace allowlist.
    #[snafu(display("Iceberg table is not configured"))]
    TableNotConfigured,
    /// A statement ticket refers to a snapshot no longer retained upstream.
    #[snafu(display("Iceberg snapshot is unavailable"))]
    SnapshotUnavailable,
    /// A configured namespace could not be verified in the external catalog.
    #[snafu(display("Iceberg namespace is unavailable"))]
    NamespaceUnavailable,
    /// An external catalog operation failed without exposing endpoint details.
    #[snafu(display("Iceberg catalog operation failed"))]
    Catalog,
}

/// Result type for the read-only Iceberg federation adapter.
pub type Result<T> = std::result::Result<T, IcebergError>;

/// Process-local authentication for an external Iceberg REST catalog.
///
/// The value is supplied by the Query deployment and is never serialized into
/// Lake metadata or statement tickets. Its `Debug` implementation deliberately
/// omits secret material.
#[derive(Clone, Eq, PartialEq)]
pub struct IcebergRestAuth {
    kind: RestAuthKind,
}

#[derive(Clone, Eq, PartialEq)]
enum RestAuthKind {
    BearerToken(String),
    OAuthClientCredentials {
        credential: String,
        options:    IcebergOAuthOptions,
    },
}

/// Optional standard properties for OAuth client-credential REST sessions.
///
/// These values identify the token request but contain no client secret. The
/// client credential itself is accepted separately by
/// [`IcebergRestAuth::oauth_client_credentials`].
#[derive(bon::Builder, Clone, Debug, Eq, PartialEq)]
pub struct IcebergOAuthOptions {
    #[builder(into)]
    oauth2_server_uri: Option<String>,
    #[builder(into)]
    scope:             Option<String>,
    #[builder(into)]
    audience:          Option<String>,
    #[builder(into)]
    resource:          Option<String>,
}

impl IcebergRestAuth {
    /// Validate a static bearer token supplied by the Query runtime.
    pub fn bearer_token(token: impl AsRef<str>) -> Result<Self> {
        let token = token.as_ref();
        if !valid_rest_secret(token) {
            return Err(IcebergError::InvalidRestAuth);
        }
        Ok(Self {
            kind: RestAuthKind::BearerToken(token.to_owned()),
        })
    }

    /// Validate OAuth client credentials supplied by the Query runtime.
    pub fn oauth_client_credentials(
        credential: impl AsRef<str>,
        options: IcebergOAuthOptions,
    ) -> Result<Self> {
        let credential = credential.as_ref();
        if !valid_rest_secret(credential) || !options.valid() {
            return Err(IcebergError::InvalidRestAuth);
        }
        Ok(Self {
            kind: RestAuthKind::OAuthClientCredentials {
                credential: credential.to_owned(),
                options,
            },
        })
    }

    fn catalog_properties(&self) -> HashMap<String, String> {
        match &self.kind {
            RestAuthKind::BearerToken(token) => {
                HashMap::from([("token".to_owned(), token.clone())])
            }
            RestAuthKind::OAuthClientCredentials {
                credential,
                options,
            } => options.catalog_properties(credential),
        }
    }

    fn uses_oauth_client_credentials(&self) -> bool {
        matches!(self.kind, RestAuthKind::OAuthClientCredentials { .. })
    }
}

impl std::fmt::Debug for IcebergRestAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergRestAuth")
            .field(
                "kind",
                &match &self.kind {
                    RestAuthKind::BearerToken(_) => "bearer_token",
                    RestAuthKind::OAuthClientCredentials { .. } => "oauth_client_credentials",
                },
            )
            .finish_non_exhaustive()
    }
}

impl IcebergOAuthOptions {
    fn valid(&self) -> bool {
        self.oauth2_server_uri.as_deref().is_none_or(|value| {
            Url::parse(value)
                .is_ok_and(|endpoint| valid_credential_free_external_rest_url(&endpoint))
        }) && [
            self.scope.as_deref(),
            self.audience.as_deref(),
            self.resource.as_deref(),
        ]
        .into_iter()
        .flatten()
        .all(valid_rest_secret)
    }

    fn catalog_properties(&self, credential: &str) -> HashMap<String, String> {
        let mut properties = HashMap::from([("credential".to_owned(), credential.to_owned())]);
        for (name, value) in [
            ("oauth2-server-uri", self.oauth2_server_uri.as_ref()),
            ("scope", self.scope.as_ref()),
            ("audience", self.audience.as_ref()),
            ("resource", self.resource.as_ref()),
        ] {
            if let Some(value) = value {
                properties.insert(name.to_owned(), value.clone());
            }
        }
        properties
    }
}

/// Immutable deployment configuration for one external Iceberg REST catalog.
#[derive(Clone, Eq, PartialEq)]
pub struct IcebergCatalogConfig {
    endpoint:             Url,
    warehouse:            String,
    namespaces:           Arc<[String]>,
    cache_freshness:      Duration,
    cache_stale_if_error: Duration,
    rest_timeout:         Duration,
    rest_auth:            Option<IcebergRestAuth>,
}

impl IcebergCatalogConfig {
    /// Validate one REST endpoint, warehouse, and finite namespace allowlist.
    ///
    /// The endpoint intentionally rejects embedded credentials, queries, and
    /// fragments so deployment credentials stay outside Lake configuration and
    /// ticket payloads. Namespace names map to one DataFusion schema segment.
    pub fn try_new<I, S>(endpoint: &str, warehouse: &str, namespaces: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut endpoint = Url::parse(endpoint).map_err(|_| IcebergError::InvalidEndpoint)?;
        if !valid_credential_free_external_rest_url(&endpoint) {
            return Err(IcebergError::InvalidEndpoint);
        }
        let normalized_path = endpoint.path().trim_end_matches('/').to_owned();
        endpoint.set_path(&normalized_path);
        let warehouse = warehouse.trim();
        if warehouse.is_empty() {
            return Err(IcebergError::InvalidWarehouse);
        }
        let namespaces = namespaces
            .into_iter()
            .map(|namespace| namespace.as_ref().trim().to_owned())
            .collect::<Vec<_>>();
        if namespaces.is_empty()
            || namespaces
                .iter()
                .any(|namespace| !valid_namespace(namespace))
        {
            return Err(IcebergError::InvalidNamespace);
        }
        let unique = namespaces.iter().collect::<std::collections::BTreeSet<_>>();
        if unique.len() != namespaces.len() {
            return Err(IcebergError::DuplicateNamespace);
        }
        Ok(Self {
            endpoint,
            warehouse: warehouse.to_owned(),
            namespaces: namespaces.into(),
            cache_freshness: DEFAULT_CACHE_FRESHNESS,
            cache_stale_if_error: DEFAULT_CACHE_STALE_IF_ERROR,
            rest_timeout: DEFAULT_REST_TIMEOUT,
            rest_auth: None,
        })
    }

    /// Attach process-local REST authentication to this deployment config.
    #[must_use]
    pub fn with_rest_auth(mut self, auth: IcebergRestAuth) -> Self {
        self.rest_auth = Some(auth);
        self
    }

    /// Set bounded external-snapshot cache freshness and stale-if-error time.
    ///
    /// A zero freshness forces each lookup to refresh; it is useful when the
    /// catalog's own cache is the only desired freshness boundary. A stale
    /// value must be non-zero and no shorter than fresh data.
    pub fn with_cache_policy(
        mut self,
        freshness: Duration,
        stale_if_error: Duration,
    ) -> Result<Self> {
        if stale_if_error.is_zero() || stale_if_error < freshness {
            return Err(IcebergError::InvalidCachePolicy);
        }
        self.cache_freshness = freshness;
        self.cache_stale_if_error = stale_if_error;
        Ok(self)
    }

    /// Set the total and connect deadline for each external REST request.
    ///
    /// The bound applies to the REST configuration handshake, namespace point
    /// checks, exact table loads, and OAuth token exchanges. It deliberately
    /// does not add retries or change Query's end-to-end execution deadline.
    pub fn with_rest_timeout(mut self, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() || timeout > MAX_REST_TIMEOUT {
            return Err(IcebergError::InvalidRestTimeout);
        }
        self.rest_timeout = timeout;
        Ok(self)
    }

    /// Return the validated credential-free TLS or loopback REST endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &Url { &self.endpoint }

    /// Return the configured Iceberg warehouse identifier.
    #[must_use]
    pub fn warehouse(&self) -> &str { &self.warehouse }

    /// Return the finite allowlist of external SQL namespaces.
    #[must_use]
    pub fn namespaces(&self) -> &[String] { &self.namespaces }

    /// Return the bounded HTTP deadline applied to each external REST request.
    #[must_use]
    pub const fn rest_timeout(&self) -> Duration { self.rest_timeout }

    fn cache_freshness(&self) -> Duration { self.cache_freshness }

    fn cache_stale_if_error(&self) -> Duration { self.cache_stale_if_error }

    fn rest_properties(&self) -> HashMap<String, String> {
        self.rest_auth
            .as_ref()
            .map_or_else(HashMap::new, IcebergRestAuth::catalog_properties)
    }

    fn uses_oauth_client_credentials(&self) -> bool {
        self.rest_auth
            .as_ref()
            .is_some_and(IcebergRestAuth::uses_oauth_client_credentials)
    }
}

impl std::fmt::Debug for IcebergCatalogConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergCatalogConfig")
            .field("endpoint", &self.endpoint)
            .field("warehouse", &self.warehouse)
            .field("namespaces", &self.namespaces)
            .field("cache_freshness", &self.cache_freshness)
            .field("cache_stale_if_error", &self.cache_stale_if_error)
            .field("rest_timeout", &self.rest_timeout)
            .field("rest_auth", &self.rest_auth.as_ref().map(|_| "configured"))
            .finish()
    }
}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_rest_secret(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REST_SECRET_BYTES
        && value.trim() == value
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_credential_free_external_rest_url(endpoint: &Url) -> bool {
    if endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return false;
    }

    match (endpoint.scheme(), endpoint.host()) {
        ("https", _) => true,
        ("http", Some(Host::Ipv4(address))) => address.is_loopback(),
        ("http", Some(Host::Ipv6(address))) => address.is_loopback(),
        _ => false,
    }
}

fn catalog_error(_: iceberg::Error) -> IcebergError { IcebergError::Catalog }

/// Read-only access to one configured external Iceberg catalog.
#[derive(Clone)]
pub struct IcebergCatalog {
    config:             IcebergCatalogConfig,
    catalog:            Arc<dyn Catalog>,
    oauth_rest_session: Option<Arc<OAuthRestSession>>,
    cache:              Arc<Mutex<HashMap<TableCacheKey, CachedSnapshot>>>,
}

struct OAuthRestSession {
    catalog:    Arc<RestCatalog>,
    generation: AtomicU64,
    refresh:    AsyncMutex<()>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct TableCacheKey {
    namespace: String,
    table:     String,
}

#[derive(Clone)]
struct CachedSnapshot {
    snapshot:     IcebergTableSnapshot,
    refreshed_at: Instant,
}

impl IcebergCatalog {
    /// Connect to and validate every configured REST catalog namespace.
    ///
    /// Startup performs only bounded point checks over the deployment
    /// allowlist. It never lists external namespaces or tables.
    pub async fn connect(config: IcebergCatalogConfig) -> Result<Self> {
        let mut properties = HashMap::from([
            (
                REST_CATALOG_PROP_URI.to_owned(),
                config.endpoint().as_str().trim_end_matches('/').to_owned(),
            ),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_owned(),
                config.warehouse().to_owned(),
            ),
        ]);
        properties.extend(config.rest_properties());
        let rest_timeout = config.rest_timeout();
        let http_client = iceberg_reqwest::Client::builder()
            .connect_timeout(rest_timeout)
            .timeout(rest_timeout)
            .build()
            .map_err(|_| IcebergError::Catalog)?;
        let rest_catalog = Arc::new(
            RestCatalogBuilder::default()
                .with_client(http_client)
                .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
                .load("lake-iceberg", properties)
                .await
                .map_err(catalog_error)?,
        );
        let oauth_rest_session = config.uses_oauth_client_credentials().then(|| {
            Arc::new(OAuthRestSession {
                catalog:    rest_catalog.clone(),
                generation: AtomicU64::new(0),
                refresh:    AsyncMutex::new(()),
            })
        });
        let catalog: Arc<dyn Catalog> = rest_catalog;
        let catalog =
            Self::from_catalog_with_oauth_rest_session(config, catalog, oauth_rest_session);
        for namespace in catalog.config.namespaces() {
            let exists = catalog
                .namespace_exists(NamespaceIdent::new(namespace.clone()))
                .await?;
            if !exists {
                return Err(IcebergError::NamespaceUnavailable);
            }
        }
        Ok(catalog)
    }

    /// Build the adapter around an already connected catalog.
    ///
    /// This constructor exists for local integration tests and embeds no
    /// deployment credentials in the resulting adapter.
    #[must_use]
    pub fn from_catalog(config: IcebergCatalogConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self::from_catalog_with_oauth_rest_session(config, catalog, None)
    }

    fn from_catalog_with_oauth_rest_session(
        config: IcebergCatalogConfig,
        catalog: Arc<dyn Catalog>,
        oauth_rest_session: Option<Arc<OAuthRestSession>>,
    ) -> Self {
        Self {
            config,
            catalog,
            oauth_rest_session,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Expose configured namespaces as a read-only DataFusion catalog.
    ///
    /// The provider owns only the finite deployment allowlist. Its asynchronous
    /// table lookup loads one exact table on demand and never enumerates the
    /// external catalog.
    #[must_use]
    pub fn datafusion_catalog(&self) -> Arc<dyn CatalogProvider> {
        Arc::new(IcebergDataFusionCatalog::new(self.clone()))
    }

    /// Resolve one configured table into its current immutable Iceberg
    /// snapshot.
    pub async fn resolve_snapshot(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<IcebergTableSnapshot> {
        let key = self.cache_key(namespace, table)?;
        let cached = self
            .cache
            .lock()
            .expect("Iceberg snapshot cache lock poisoned")
            .get(&key)
            .cloned();
        if cached
            .as_ref()
            .is_some_and(|cached| cached.refreshed_at.elapsed() < self.config.cache_freshness())
        {
            return Ok(cached.expect("checked above").snapshot);
        }
        match self.load_current_snapshot(&key).await {
            Ok(snapshot) => {
                self.insert_cached(key, snapshot.clone());
                Ok(snapshot)
            }
            Err(_error)
                if cached.as_ref().is_some_and(|cached| {
                    cached.refreshed_at.elapsed() < self.config.cache_stale_if_error()
                }) =>
            {
                Ok(cached.expect("checked above").snapshot)
            }
            Err(error) => Err(error),
        }
    }

    /// Resolve one configured table at the immutable snapshot named in a
    /// statement ticket. This is a point lookup and never adopts the table's
    /// newer current snapshot.
    pub async fn resolve_snapshot_at(
        &self,
        namespace: &str,
        table: &str,
        snapshot_id: i64,
    ) -> Result<IcebergTableSnapshot> {
        let key = self.cache_key(namespace, table)?;
        let table = self
            .load_table(TableIdent::new(
                NamespaceIdent::new(key.namespace.clone()),
                key.table.clone(),
            ))
            .await?;
        if table.metadata().snapshot_by_id(snapshot_id).is_none() {
            return Err(IcebergError::SnapshotUnavailable);
        }
        Ok(IcebergTableSnapshot {
            namespace: key.namespace,
            table:     table.identifier().name().to_owned(),
            snapshot:  Some(snapshot_id),
            inner:     table,
        })
    }

    fn cache_key(&self, namespace: &str, table: &str) -> Result<TableCacheKey> {
        if !self
            .config
            .namespaces()
            .iter()
            .any(|configured| configured == namespace)
            || !valid_namespace(table)
        {
            return Err(IcebergError::TableNotConfigured);
        }
        Ok(TableCacheKey {
            namespace: namespace.to_owned(),
            table:     table.to_owned(),
        })
    }

    async fn load_current_snapshot(&self, key: &TableCacheKey) -> Result<IcebergTableSnapshot> {
        let table = self
            .load_table(TableIdent::new(
                NamespaceIdent::new(key.namespace.clone()),
                key.table.clone(),
            ))
            .await?;
        Ok(IcebergTableSnapshot {
            namespace: key.namespace.clone(),
            table:     table.identifier().name().to_owned(),
            snapshot:  table
                .metadata()
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            inner:     table,
        })
    }

    async fn namespace_exists(&self, namespace: NamespaceIdent) -> Result<bool> {
        let generation = self.oauth_generation();
        match self.catalog.namespace_exists(&namespace).await {
            Ok(exists) => Ok(exists),
            Err(_) if self.refresh_oauth_after_failed_read(generation).await? => self
                .catalog
                .namespace_exists(&namespace)
                .await
                .map_err(catalog_error),
            Err(_) => Err(IcebergError::Catalog),
        }
    }

    async fn load_table(&self, table: TableIdent) -> Result<Table> {
        let generation = self.oauth_generation();
        match self.catalog.load_table(&table).await {
            Ok(table) => Ok(table),
            Err(_) if self.refresh_oauth_after_failed_read(generation).await? => {
                self.catalog.load_table(&table).await.map_err(catalog_error)
            }
            Err(_) => Err(IcebergError::Catalog),
        }
    }

    fn oauth_generation(&self) -> Option<u64> {
        self.oauth_rest_session
            .as_ref()
            .map(|session| session.generation.load(Ordering::Acquire))
    }

    async fn refresh_oauth_after_failed_read(&self, generation: Option<u64>) -> Result<bool> {
        let (Some(session), Some(generation)) = (&self.oauth_rest_session, generation) else {
            return Ok(false);
        };
        // The lock deliberately spans the external exchange so a token expiry
        // cannot fan out into one client-credential request per reader.
        let _refresh = session.refresh.lock().await;
        if session.generation.load(Ordering::Acquire) != generation {
            return Ok(true);
        }
        session
            .catalog
            .regenerate_token()
            .await
            .map_err(catalog_error)?;
        session.generation.fetch_add(1, Ordering::Release);
        Ok(true)
    }

    fn insert_cached(&self, key: TableCacheKey, snapshot: IcebergTableSnapshot) {
        let mut cache = self
            .cache
            .lock()
            .expect("Iceberg snapshot cache lock poisoned");
        if cache.len() >= SNAPSHOT_CACHE_CAPACITY && !cache.contains_key(&key) {
            let evicted = cache.keys().next().cloned();
            if let Some(evicted) = evicted {
                cache.remove(&evicted);
            }
        }
        cache.insert(
            key,
            CachedSnapshot {
                snapshot,
                refreshed_at: Instant::now(),
            },
        );
    }
}

impl std::fmt::Debug for IcebergCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergCatalog")
            .field("namespaces", &self.config.namespaces())
            .finish_non_exhaustive()
    }
}

struct IcebergDataFusionCatalog {
    schemas: BTreeMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergDataFusionCatalog {
    fn new(catalog: IcebergCatalog) -> Self {
        let schemas = catalog
            .config
            .namespaces()
            .iter()
            .map(|namespace| {
                (
                    namespace.clone(),
                    Arc::new(IcebergSchemaProvider {
                        catalog:   catalog.clone(),
                        namespace: namespace.clone(),
                    }) as Arc<dyn SchemaProvider>,
                )
            })
            .collect();
        Self { schemas }
    }
}

impl std::fmt::Debug for IcebergDataFusionCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergDataFusionCatalog")
            .field("schemas", &self.schemas.keys())
            .finish()
    }
}

impl CatalogProvider for IcebergDataFusionCatalog {
    fn as_any(&self) -> &dyn Any { self }

    fn schema_names(&self) -> Vec<String> { self.schemas.keys().cloned().collect() }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

struct IcebergSchemaProvider {
    catalog:   IcebergCatalog,
    namespace: String,
}

impl std::fmt::Debug for IcebergSchemaProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergSchemaProvider")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn table_names(&self) -> Vec<String> {
        // External table enumeration is intentionally forbidden. Discovery is
        // bounded to tables whose immutable snapshots were already selected.
        Vec::new()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        if !valid_namespace(name) {
            return Ok(None);
        }
        self.catalog
            .resolve_snapshot(&self.namespace, name)
            .await
            .map_err(|source| DataFusionError::External(Box::new(source)))?
            .table_provider()
            .await
            .map(Some)
            .map_err(|source| DataFusionError::External(Box::new(source)))
    }

    fn table_exist(&self, _name: &str) -> bool {
        // This synchronous hook must not perform external I/O. `table` above
        // performs the bounded point lookup when planning refers to a table.
        false
    }
}

/// One immutable external table snapshot selected while planning a statement.
#[derive(Clone)]
pub struct IcebergTableSnapshot {
    namespace: String,
    table:     String,
    snapshot:  Option<i64>,
    inner:     Table,
}

impl IcebergTableSnapshot {
    /// Return the external SQL namespace.
    #[must_use]
    pub fn namespace(&self) -> &str { &self.namespace }

    /// Return the external table name.
    #[must_use]
    pub fn table(&self) -> &str { &self.table }

    /// Return the pinned Iceberg snapshot ID, if the table has committed data.
    #[must_use]
    pub const fn snapshot_id(&self) -> Option<i64> { self.snapshot }

    /// Build a DataFusion provider that can read only this pinned snapshot.
    pub async fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let provider = match self.snapshot {
            Some(snapshot) => {
                IcebergStaticTableProvider::try_new_from_table_snapshot(
                    self.inner.clone(),
                    snapshot,
                )
                .await
            }
            None => IcebergStaticTableProvider::try_new_from_table(self.inner.clone()).await,
        }
        .map_err(catalog_error)?;
        Ok(Arc::new(provider))
    }
}

impl std::fmt::Debug for IcebergTableSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergTableSnapshot")
            .field("namespace", &self.namespace)
            .field("table", &self.table)
            .field("snapshot", &self.snapshot)
            .finish_non_exhaustive()
    }
}
