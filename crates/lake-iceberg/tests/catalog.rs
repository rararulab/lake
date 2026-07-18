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
    http::StatusCode,
    routing::{any, get, head},
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
use lake_iceberg::{IcebergCatalog, IcebergCatalogConfig};

#[derive(Debug)]
struct RecordingCatalog {
    inner:           MemoryCatalog,
    namespace_lists: AtomicUsize,
    table_lists:     AtomicUsize,
    table_loads:     AtomicUsize,
    fail_load:       AtomicBool,
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
    });
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
