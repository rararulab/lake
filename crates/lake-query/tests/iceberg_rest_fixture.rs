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

//! Apache REST catalog interoperability coverage.
//!
//! Run with `mise run test-iceberg-integration`. The test intentionally uses
//! an independent Apache REST catalog and MinIO rather than Lake's in-process
//! Axum protocol fixture.

use std::{collections::HashMap, env, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{array::Int64Array, record_batch::RecordBatch},
    parquet::file::properties::WriterProperties,
};
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    io::{
        S3_ACCESS_KEY_ID, S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY,
    },
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
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use lake_engine_lance::LanceEngine;
use lake_iceberg::{IcebergCatalog, IcebergCatalogConfig, IcebergS3Config};
use lake_meta::{MetaStore, MetaStoreRef};
use lake_query::QueryEngine;
use uuid::Uuid;

#[derive(Default)]
struct RejectingLakeMeta;

#[async_trait]
impl MetaStore for RejectingLakeMeta {
    async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
        unreachable!("an Iceberg-only query must not read Lake metadata")
    }

    async fn cas(
        &self,
        _key: &str,
        _expected: Option<&[u8]>,
        _new: &[u8],
    ) -> lake_meta::Result<bool> {
        unreachable!("an Iceberg-only query must not mutate Lake metadata")
    }

    async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
        unreachable!("an Iceberg-only query must not list Lake metadata")
    }

    async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> {
        unreachable!("an Iceberg-only query must not delete Lake metadata")
    }
}

fn required_env(name: &str) -> String {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required; run mise run test-iceberg-integration"))
}

#[test]
fn apache_rest_catalog_fixture_runner_selects_real_interoperability_test() {
    let runner = include_str!("../../../scripts/test-iceberg-integration.ts");
    for required in [
        "test-iceberg-env.ts up",
        "apache_rest_catalog_with_minio_is_queryable",
        "test-iceberg-env.ts down",
    ] {
        assert!(
            runner.contains(required),
            "Iceberg integration runner must contain {required:?}"
        );
    }
}

fn s3_properties(endpoint: &str) -> HashMap<String, String> {
    HashMap::from([
        (S3_ENDPOINT.to_owned(), endpoint.to_owned()),
        (S3_ACCESS_KEY_ID.to_owned(), "admin".to_owned()),
        (S3_SECRET_ACCESS_KEY.to_owned(), "password".to_owned()),
        (S3_REGION.to_owned(), "us-east-1".to_owned()),
        (S3_PATH_STYLE_ACCESS.to_owned(), "true".to_owned()),
        (S3_DISABLE_EC2_METADATA.to_owned(), "true".to_owned()),
    ])
}

async fn create_populated_table(
    endpoint: &str,
    warehouse: &str,
    s3_endpoint: &str,
    namespace: &NamespaceIdent,
) -> Table {
    let mut properties = s3_properties(s3_endpoint);
    properties.insert(REST_CATALOG_PROP_URI.to_owned(), endpoint.to_owned());
    properties.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse.to_owned());
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load("lake-rest-fixture-writer", properties)
        .await
        .expect("connect independent Apache REST catalog writer");
    catalog
        .create_namespace(namespace, HashMap::new())
        .await
        .expect("create isolated external namespace");
    let table = catalog
        .create_table(
            namespace,
            TableCreation::builder()
                .name("episodes".to_owned())
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
                        .expect("build external Iceberg schema"),
                )
                .properties(HashMap::new())
                .build(),
        )
        .await
        .expect("create external Iceberg table");
    let arrow_schema = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .expect("convert external Iceberg schema to Arrow"),
    );
    let location_generator =
        DefaultLocationGenerator::new(table.metadata()).expect("create Iceberg location generator");
    let parquet_writer = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer,
        table.file_io().clone(),
        location_generator,
        DefaultFileNameGenerator::new(
            "apache-rest-fixture".to_owned(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        ),
    );
    let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer)
        .build(None)
        .await
        .expect("build external Iceberg data writer");
    data_file_writer
        .write(
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![42_i64]))])
                .expect("build external Iceberg batch"),
        )
        .await
        .expect("write external Iceberg Parquet data");
    let data_files = data_file_writer
        .close()
        .await
        .expect("close external Iceberg data writer");
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .fast_append()
        .add_data_files(data_files)
        .apply(transaction)
        .expect("apply external Iceberg append");
    transaction
        .commit(&catalog)
        .await
        .expect("commit external Iceberg snapshot");
    catalog
        .load_table(&TableIdent::new(namespace.clone(), "episodes".to_owned()))
        .await
        .expect("reload committed external table")
}

#[tokio::test]
#[ignore = "requires `mise run test-iceberg-integration`"]
async fn apache_rest_catalog_with_minio_is_queryable() {
    let rest_endpoint = required_env("LAKE_ICEBERG_TEST_REST_ENDPOINT");
    let warehouse = required_env("LAKE_ICEBERG_TEST_WAREHOUSE");
    let s3_endpoint = required_env("LAKE_ICEBERG_TEST_S3_ENDPOINT");
    let namespace_name = format!("lake_rest_{}", Uuid::now_v7().simple());
    let namespace = NamespaceIdent::new(namespace_name.clone());
    create_populated_table(&rest_endpoint, &warehouse, &s3_endpoint, &namespace).await;

    let storage = IcebergS3Config::try_new(&s3_endpoint)
        .expect("configure loopback MinIO endpoint")
        .with_region("us-east-1")
        .expect("configure MinIO region")
        .with_path_style_access()
        .with_anonymous_access();
    let iceberg = IcebergCatalog::connect(
        IcebergCatalogConfig::try_new(&rest_endpoint, &warehouse, [&namespace_name])
            .expect("configure external Apache REST catalog")
            .with_s3_config(storage),
    )
    .await
    .expect("connect Lake to external Apache REST catalog");
    let meta: MetaStoreRef = Arc::new(RejectingLakeMeta);
    let query = QueryEngine::new(meta, Arc::new(LanceEngine::new())).with_iceberg_catalog(iceberg);
    let sql = format!("SELECT episode_id FROM iceberg.{namespace_name}.episodes");
    let batches = query
        .execute_sql(&sql)
        .await
        .expect("plan external Apache REST Iceberg table")
        .try_collect::<Vec<_>>()
        .await
        .expect("read external Apache REST Iceberg table directly from MinIO");
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("external Iceberg episode ID column")
                .iter()
                .flatten()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![42]);
}
