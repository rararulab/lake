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
    sync::{Arc, Mutex},
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
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_datafusion::IcebergStaticTableProvider;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use snafu::Snafu;
use url::Url;

const DEFAULT_CACHE_FRESHNESS: Duration = Duration::from_secs(5);
const DEFAULT_CACHE_STALE_IF_ERROR: Duration = Duration::from_mins(1);
const SNAPSHOT_CACHE_CAPACITY: usize = 10_000;

/// Errors returned while validating external Iceberg catalog configuration.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum IcebergError {
    /// The REST endpoint is not a credential-free HTTP(S) URL.
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
    Catalog { source: iceberg::Error },
}

/// Result type for the read-only Iceberg federation adapter.
pub type Result<T> = std::result::Result<T, IcebergError>;

/// Immutable deployment configuration for one external Iceberg REST catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergCatalogConfig {
    endpoint:             Url,
    warehouse:            String,
    namespaces:           Arc<[String]>,
    cache_freshness:      Duration,
    cache_stale_if_error: Duration,
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
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
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
        })
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

    /// Return the validated credential-free REST endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &Url { &self.endpoint }

    /// Return the configured Iceberg warehouse identifier.
    #[must_use]
    pub fn warehouse(&self) -> &str { &self.warehouse }

    /// Return the finite allowlist of external SQL namespaces.
    #[must_use]
    pub fn namespaces(&self) -> &[String] { &self.namespaces }

    fn cache_freshness(&self) -> Duration { self.cache_freshness }

    fn cache_stale_if_error(&self) -> Duration { self.cache_stale_if_error }
}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// Read-only access to one configured external Iceberg catalog.
#[derive(Clone)]
pub struct IcebergCatalog {
    config:  IcebergCatalogConfig,
    catalog: Arc<dyn Catalog>,
    cache:   Arc<Mutex<HashMap<TableCacheKey, CachedSnapshot>>>,
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
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
            .load(
                "lake-iceberg",
                HashMap::from([
                    (
                        REST_CATALOG_PROP_URI.to_owned(),
                        config.endpoint().as_str().trim_end_matches('/').to_owned(),
                    ),
                    (
                        REST_CATALOG_PROP_WAREHOUSE.to_owned(),
                        config.warehouse().to_owned(),
                    ),
                ]),
            )
            .await
            .map_err(|source| IcebergError::Catalog { source })?;
        let catalog = Self::from_catalog(config, Arc::new(catalog));
        for namespace in catalog.config.namespaces() {
            let exists = catalog
                .catalog
                .namespace_exists(&NamespaceIdent::new(namespace.clone()))
                .await
                .map_err(|source| IcebergError::Catalog { source })?;
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
        Self {
            config,
            catalog,
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
            .catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(key.namespace.clone()),
                key.table.clone(),
            ))
            .await
            .map_err(|source| IcebergError::Catalog { source })?;
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
            .catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(key.namespace.clone()),
                key.table.clone(),
            ))
            .await
            .map_err(|source| IcebergError::Catalog { source })?;
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
        .map_err(|source| IcebergError::Catalog { source })?;
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
