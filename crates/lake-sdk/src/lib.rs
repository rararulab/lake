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

//! Rust SDK for parameterized SQL inserts containing managed `FILE` values.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, SchemaRef},
        error::ArrowError,
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use lake_common::{DataLocation, TableRef, Version};
use lake_metasrv::Metasrv;
use lake_objects::{LocalObjectStore, data_location_array, data_location_field};
use snafu::{ResultExt, Snafu};
use tokio::{fs::File, io::AsyncRead};

/// Errors raised by the typed Rust SDK.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum SdkError {
    #[snafu(display("unsupported INSERT SQL: {message}"))]
    InvalidSql { message: String },

    #[snafu(display("INSERT binds {actual} values but SQL declares {expected} placeholders"))]
    ParameterCount { expected: usize, actual: usize },

    #[snafu(display("INSERT column '{column}' is missing from table schema"))]
    UnknownColumn { column: String },

    #[snafu(display("INSERT must bind every table column; '{column}' is missing"))]
    MissingColumn { column: String },

    #[snafu(display("INSERT value for '{column}' does not match its table type"))]
    TypeMismatch { column: String },

    #[snafu(display("table '{table}' not found"))]
    NotFound { table: String },

    #[snafu(display("metasrv operation failed"))]
    Metasrv { source: lake_metasrv::MetasrvError },

    #[snafu(display("storage engine operation failed"))]
    Engine { source: lake_engine::EngineError },

    #[snafu(display("managed object operation failed"))]
    Object { source: lake_objects::ObjectError },

    #[snafu(display("could not build INSERT record batch"))]
    Arrow { source: ArrowError },
}

/// The result type for Rust SDK operations.
pub type Result<T> = std::result::Result<T, SdkError>;

/// An SDK upload source bound to a SQL `FILE` value.
///
/// The SDK streams this source directly into a Lake-managed stage. It never
/// sends the file bytes through SQL, Flight, or the metadata service.
pub struct FileUpload {
    source:       ObjectSource,
    content_type: String,
}

enum ObjectSource {
    Path(PathBuf),
    Reader(Box<dyn AsyncRead + Send + Unpin>),
}

impl fmt::Debug for FileUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match &self.source {
            ObjectSource::Path(path) => path.display().to_string(),
            ObjectSource::Reader(_) => "reader".to_owned(),
        };
        formatter
            .debug_struct("FileUpload")
            .field("source", &source)
            .field("content_type", &self.content_type)
            .finish()
    }
}

impl FileUpload {
    /// Bind `path` as a SQL `FILE` with the supplied IANA media type.
    #[must_use]
    pub fn from_path(path: impl AsRef<Path>, content_type: impl Into<String>) -> Self {
        Self {
            source:       ObjectSource::Path(path.as_ref().to_path_buf()),
            content_type: content_type.into(),
        }
    }

    /// Bind an async source that the SDK streams directly to the managed stage.
    #[must_use]
    pub fn from_reader<R>(reader: R, content_type: impl Into<String>) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        Self {
            source:       ObjectSource::Reader(Box::new(reader)),
            content_type: content_type.into(),
        }
    }
}

/// A typed parameter accepted by the SDK's narrow INSERT binding.
#[derive(Debug)]
pub enum InsertValue {
    /// UTF-8 scalar value.
    Utf8(String),
    /// SQL `FILE` streamed directly to the Lake-managed stage.
    File(FileUpload),
}

/// An in-process Rust SDK client for managed-object inserts and direct reads.
#[derive(Clone)]
pub struct LakeClient {
    metasrv: Metasrv,
    objects: LocalObjectStore,
}

impl LakeClient {
    /// Construct a client over the existing metadata authority and object
    /// prefix.
    #[must_use]
    pub fn new(metasrv: Arc<Metasrv>, objects: LocalObjectStore) -> Self {
        Self {
            metasrv: (*metasrv).clone(),
            objects,
        }
    }

    /// Execute a parameterized, single-row INSERT with typed scalar/`FILE`
    /// values.
    pub async fn insert(&self, sql: &str, values: Vec<InsertValue>) -> Result<Version> {
        let insert = parse_insert(sql)?;
        if insert.columns.len() != values.len() {
            return Err(SdkError::ParameterCount {
                expected: insert.columns.len(),
                actual:   values.len(),
            });
        }
        let schema = self.table_schema(&insert.table).await?;
        validate_bindings(&schema, &insert.columns, &values)?;

        let mut bindings = insert
            .columns
            .into_iter()
            .zip(values)
            .collect::<BTreeMap<_, _>>();
        let mut arrays = Vec::<ArrayRef>::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let value = bindings
                .remove(field.name())
                .ok_or_else(|| SdkError::MissingColumn {
                    column: field.name().to_owned(),
                })?;
            arrays.push(self.upload_and_encode(field.data_type(), value).await?);
        }
        let batch = RecordBatch::try_new(schema, arrays).context(ArrowSnafu)?;
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
        ));
        self.metasrv
            .append(&insert.table, stream)
            .await
            .context(MetasrvSnafu)
    }

    /// Open a direct local reader for an immutable `DataLocation`.
    pub async fn open(&self, location: &DataLocation) -> Result<File> {
        self.objects
            .open_reader(location)
            .await
            .context(ObjectSnafu)
    }

    async fn table_schema(&self, table: &TableRef) -> Result<SchemaRef> {
        let registration = self
            .metasrv
            .resolve(table)
            .await
            .context(MetasrvSnafu)?
            .ok_or_else(|| SdkError::NotFound {
                table: table.to_string(),
            })?;
        let handle = self
            .metasrv
            .engine()
            .open(&registration.location)
            .await
            .context(EngineSnafu)?
            .ok_or_else(|| SdkError::NotFound {
                table: table.to_string(),
            })?;
        Ok(handle.schema())
    }

    async fn upload_and_encode(
        &self,
        data_type: &DataType,
        value: InsertValue,
    ) -> Result<ArrayRef> {
        match (data_type, value) {
            (DataType::Utf8, InsertValue::Utf8(value)) => {
                Ok(Arc::new(StringArray::from(vec![value])))
            }
            (data_type, InsertValue::File(file))
                if data_type == data_location_field("ignored", false).data_type() =>
            {
                let location = match file.source {
                    ObjectSource::Path(path) => {
                        self.objects.put_file(path, file.content_type).await
                    }
                    ObjectSource::Reader(reader) => {
                        self.objects.put_reader(reader, file.content_type).await
                    }
                }
                .context(ObjectSnafu)?;
                Ok(Arc::new(data_location_array(&[location])))
            }
            (..) => Err(SdkError::TypeMismatch {
                column: "bound value".to_owned(),
            }),
        }
    }
}

#[derive(Debug)]
struct Insert {
    table:   TableRef,
    columns: Vec<String>,
}

fn parse_insert(sql: &str) -> Result<Insert> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let rest = sql
        .strip_prefix("INSERT INTO ")
        .ok_or_else(|| SdkError::InvalidSql {
            message: "statement must begin with INSERT INTO".to_owned(),
        })?;
    let (target, rest) = rest.split_once('(').ok_or_else(|| SdkError::InvalidSql {
        message: "missing column list".to_owned(),
    })?;
    let (columns, values) = rest
        .split_once(") VALUES (")
        .ok_or_else(|| SdkError::InvalidSql {
            message: "expected ) VALUES (".to_owned(),
        })?;
    let values = values
        .strip_suffix(')')
        .ok_or_else(|| SdkError::InvalidSql {
            message: "missing closing value list".to_owned(),
        })?;
    let (namespace, name) = target
        .trim()
        .split_once('.')
        .ok_or_else(|| SdkError::InvalidSql {
            message: "table must be <namespace>.<name>".to_owned(),
        })?;
    if !identifier(namespace) || !identifier(name) {
        return Err(SdkError::InvalidSql {
            message: "table identifiers may contain only ASCII letters, digits, and underscores"
                .to_owned(),
        });
    }
    let columns = columns
        .split(',')
        .map(str::trim)
        .map(|column| {
            if !identifier(column) {
                return Err(SdkError::InvalidSql {
                    message: format!("invalid column identifier '{column}'"),
                });
            }
            Ok(column.to_owned())
        })
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() || values.split(',').any(|value| value.trim() != "?") {
        return Err(SdkError::InvalidSql {
            message: "VALUES must contain one ? placeholder per column".to_owned(),
        });
    }
    if values.split(',').count() != columns.len() || duplicate(&columns) {
        return Err(SdkError::InvalidSql {
            message: "column names and placeholders must be one-to-one".to_owned(),
        });
    }
    Ok(Insert {
        table: TableRef::new(namespace, name),
        columns,
    })
}

fn validate_bindings(schema: &SchemaRef, columns: &[String], values: &[InsertValue]) -> Result<()> {
    for (column, value) in columns.iter().zip(values) {
        let field = schema
            .fields()
            .iter()
            .find(|field| field.name() == column)
            .ok_or_else(|| SdkError::UnknownColumn {
                column: column.clone(),
            })?;
        let matches = match value {
            InsertValue::Utf8(_) => field.data_type() == &DataType::Utf8,
            InsertValue::File(_) => {
                field.data_type() == data_location_field("ignored", false).data_type()
            }
        };
        if !matches {
            return Err(SdkError::TypeMismatch {
                column: column.clone(),
            });
        }
    }
    for field in schema.fields() {
        if !columns.iter().any(|column| column == field.name()) {
            return Err(SdkError::MissingColumn {
                column: field.name().to_owned(),
            });
        }
    }
    Ok(())
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn duplicate(values: &[String]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].iter().any(|prior| prior == value))
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    use datafusion::arrow::{
        array::{Array, StringArray, StructArray},
        datatypes::{DataType, Field, Schema},
    };
    use lake_common::{TableLocation, TableRef};
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaStoreRef, RocksMeta};
    use lake_metasrv::Metasrv;
    use lake_objects::{LocalObjectStore, data_location_field, data_location_from_array};
    use lake_query::QueryEngine;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

    use crate::{FileUpload, InsertValue, LakeClient};

    #[tokio::test]
    async fn insert_sql_file_uploads_and_queries_datalocation() {
        let root = tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![
            Field::new("episode_id", DataType::Utf8, false),
            data_location_field("video", false),
        ]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("tables/episodes.lance").to_string_lossy()),
                schema,
            )
            .await
            .unwrap();

        let source = root.path().join("episode.mp4");
        let expected = b"large video bytes streamed by the sdk";
        tokio::fs::write(&source, expected).await.unwrap();
        let client = LakeClient::new(
            metasrv,
            LocalObjectStore::open(root.path().join("objects"))
                .await
                .unwrap(),
        );

        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-42".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();

        let query = QueryEngine::new(meta, engine);
        let batches = query
            .execute_sql("SELECT episode_id, video FROM lake.robots.episodes")
            .await
            .unwrap();
        let episode_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(episode_ids.value(0), "episode-42");
        let locations = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let location = data_location_from_array(locations, 0).unwrap();
        assert_eq!(location.content_type, "video/mp4");
        assert_eq!(location.size_bytes, expected.len() as u64);
        assert_eq!(location.sha256, format!("{:x}", Sha256::digest(expected)));
        let mut reader = client.open(&location).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn failed_upload_does_not_publish_a_table_version() {
        let root = tempdir().unwrap();
        let (client, metasrv, table) = setup_client(root.path()).await;
        let before = metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version;

        let error = client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-43".to_owned()),
                    InsertValue::File(FileUpload::from_reader(
                        FailingReader { emitted: false },
                        "video/mp4",
                    )),
                ],
            )
            .await;

        assert!(error.is_err(), "a missing source file must fail the INSERT");
        let after = metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version;
        assert_eq!(
            after, before,
            "a failed upload must not append a table version"
        );
        let mut objects = tokio::fs::read_dir(root.path().join("objects"))
            .await
            .unwrap();
        assert!(
            objects.next_entry().await.unwrap().is_none(),
            "a failed upload must remove its staging object"
        );
    }

    #[tokio::test]
    async fn unsupported_insert_syntax_never_starts_an_upload() {
        let root = tempdir().unwrap();
        let (client, _metasrv, _table) = setup_client(root.path()).await;
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"must not be uploaded")
            .await
            .unwrap();

        let error = client
            .insert(
                "INSERT INTO robots.episodes VALUES (?)",
                vec![InsertValue::File(FileUpload::from_path(
                    &source,
                    "video/mp4",
                ))],
            )
            .await;

        assert!(error.is_err(), "unsupported SQL must fail before upload");
        let mut objects = tokio::fs::read_dir(root.path().join("objects"))
            .await
            .unwrap();
        assert!(objects.next_entry().await.unwrap().is_none());
    }

    async fn setup_client(root: &std::path::Path) -> (LakeClient, Arc<Metasrv>, TableRef) {
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Arc::new(Metasrv::new(meta, engine));
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![
            Field::new("episode_id", DataType::Utf8, false),
            data_location_field("video", false),
        ]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.join("tables/episodes.lance").to_string_lossy()),
                schema,
            )
            .await
            .unwrap();
        let client = LakeClient::new(
            metasrv.clone(),
            LocalObjectStore::open(root.join("objects")).await.unwrap(),
        );
        (client, metasrv, table)
    }

    struct FailingReader {
        emitted: bool,
    }

    impl AsyncRead for FailingReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if !self.emitted {
                self.emitted = true;
                buf.put_slice(b"first chunk");
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Err(io::Error::other("source stream interrupted")))
        }
    }
}
