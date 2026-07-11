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

//! Per-namespace `SchemaProvider`.

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result as DfResult},
};
use lake_common::{Namespace, TableName, TableRef};

use crate::catalog::{CatalogState, ProviderLoadError};

/// A DataFusion schema = one lake namespace.
pub struct LakeSchema {
    namespace: Namespace,
    state:     Arc<CatalogState>,
}

impl LakeSchema {
    pub(crate) fn new(namespace: Namespace, state: Arc<CatalogState>) -> Self {
        Self { namespace, state }
    }

    fn table_ref(&self, name: &str) -> TableRef {
        TableRef {
            namespace: self.namespace.clone(),
            name:      TableName(name.to_string()),
        }
    }
}

impl std::fmt::Debug for LakeSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSchema")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for LakeSchema {
    fn as_any(&self) -> &dyn Any { self }

    fn table_names(&self) -> Vec<String> {
        // Reads the cached snapshot — never blocks on the metastore.
        let generation = self
            .state
            .snapshot
            .read()
            .expect("snapshot lock poisoned")
            .clone();
        generation
            .listings()
            .get(&self.namespace)
            .map(|tables| tables.iter().map(|t| t.0.clone()).collect())
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let table = self.table_ref(name);
        let Some(reg) = self
            .state
            .registration(&table)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        else {
            return Ok(None);
        };
        match self.state.provider(&table, &reg).await {
            Ok(provider) => Ok(Some(provider)),
            Err(error) if matches!(error.as_ref(), ProviderLoadError::Missing { .. }) => Ok(None),
            Err(error) => Err(DataFusionError::External(Box::new(error))),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let generation = self
            .state
            .snapshot
            .read()
            .expect("snapshot lock poisoned")
            .clone();
        generation
            .listings()
            .get(&self.namespace)
            .is_some_and(|tables| tables.iter().any(|t| t.0 == name))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use datafusion::{
        arrow::datatypes::{Schema, SchemaRef},
        catalog::CatalogProvider,
        datasource::empty::EmptyTable,
        execution::SendableRecordBatchStream,
    };
    use lake_common::{AppendOperation, TableLocation, Version};
    use lake_engine::{
        EngineError, ObjectReferencePage, ObjectReferenceRequest, TableEngine, TableEngineRef,
        TableHandle, TableHandleRef,
    };
    use lake_meta::{MetaStore, MetaStoreRef, RocksMeta, registry, registry::TableRegistration};
    use tokio::sync::Notify;

    use super::*;
    use crate::LakeCatalog;

    struct CountingEngine {
        opens:      AtomicUsize,
        providers:  Arc<AtomicUsize>,
        fail_next:  Arc<AtomicBool>,
        versions:   Arc<StdMutex<Vec<Version>>>,
        open_delay: std::time::Duration,
    }

    impl CountingEngine {
        fn new(open_delay: std::time::Duration) -> Self {
            Self {
                opens: AtomicUsize::new(0),
                providers: Arc::new(AtomicUsize::new(0)),
                fail_next: Arc::new(AtomicBool::new(false)),
                versions: Arc::new(StdMutex::new(Vec::new())),
                open_delay,
            }
        }
    }

    struct CountingHandle {
        providers: Arc<AtomicUsize>,
        fail_next: Arc<AtomicBool>,
        versions:  Arc<StdMutex<Vec<Version>>>,
        schema:    SchemaRef,
    }

    #[async_trait]
    impl TableEngine for CountingEngine {
        fn kind(&self) -> &'static str { "counting" }

        async fn create(
            &self,
            _location: &TableLocation,
            _schema: SchemaRef,
        ) -> lake_engine::Result<TableHandleRef> {
            unreachable!()
        }

        async fn open(
            &self,
            _location: &TableLocation,
        ) -> lake_engine::Result<Option<TableHandleRef>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.open_delay).await;
            Ok(Some(Arc::new(CountingHandle {
                providers: self.providers.clone(),
                fail_next: self.fail_next.clone(),
                versions:  self.versions.clone(),
                schema:    Arc::new(Schema::empty()),
            })))
        }

        async fn remove(&self, _location: &TableLocation) -> lake_engine::Result<()> {
            unreachable!()
        }

        async fn maintain(
            &self,
            _location: &TableLocation,
            _version: Version,
        ) -> lake_engine::Result<Option<Version>> {
            unreachable!()
        }

        async fn retained_object_references(
            &self,
            _location: &TableLocation,
            _request: ObjectReferenceRequest,
        ) -> lake_engine::Result<ObjectReferencePage> {
            unreachable!()
        }
    }

    #[async_trait]
    impl TableHandle for CountingHandle {
        fn schema(&self) -> SchemaRef { self.schema.clone() }

        fn current_version(&self) -> Version { Version(1) }

        async fn table_provider(
            &self,
            version: Version,
        ) -> lake_engine::Result<Arc<dyn TableProvider>> {
            self.providers.fetch_add(1, Ordering::SeqCst);
            self.versions.lock().unwrap().push(version);
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(EngineError::backend(std::io::Error::other(
                    "injected provider failure",
                )));
            }
            Ok(Arc::new(EmptyTable::new(self.schema.clone())))
        }

        async fn append(
            &self,
            _operation: &AppendOperation,
            _batches: SendableRecordBatchStream,
        ) -> lake_engine::Result<Version> {
            unreachable!()
        }

        async fn reconcile_append(
            &self,
            _operation: &AppendOperation,
        ) -> lake_engine::Result<Option<Version>> {
            unreachable!()
        }
    }

    struct PausingMeta {
        inner:   Arc<RocksMeta>,
        pause:   AtomicBool,
        entered: Notify,
        release: Notify,
    }

    #[async_trait]
    impl MetaStore for PausingMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            let captured = self.inner.get(key).await?;
            if key == "tbl/robots/episodes" && self.pause.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(captured)
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            self.inner.cas(key, expected, new).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    async fn registered_catalog_with_capacity(
        engine: Arc<CountingEngine>,
        capacity: Option<u64>,
    ) -> (tempfile::TempDir, Arc<RocksMeta>, TableRef, LakeCatalog) {
        let root = tempfile::tempdir().unwrap();
        let meta = Arc::new(RocksMeta::open(root.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        registry::register(
            meta.as_ref(),
            &table,
            &TableRegistration::new(
                TableLocation::new("mem://episodes"),
                engine.kind(),
                Version(1),
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        let meta_ref: MetaStoreRef = meta.clone();
        let engine_ref: TableEngineRef = engine;
        let catalog = match capacity {
            Some(capacity) => {
                LakeCatalog::with_test_provider_cache_capacity(meta_ref, engine_ref, capacity)
            }
            None => LakeCatalog::new(meta_ref, engine_ref),
        };
        (root, meta, table, catalog)
    }

    async fn registered_catalog(
        engine: Arc<CountingEngine>,
    ) -> (tempfile::TempDir, Arc<RocksMeta>, TableRef, LakeCatalog) {
        registered_catalog_with_capacity(engine, None).await
    }

    #[tokio::test]
    async fn concurrent_provider_loads_are_singleflighted() {
        let engine = Arc::new(CountingEngine::new(std::time::Duration::from_millis(25)));
        let (_root, _meta, _table, catalog) = registered_catalog(engine.clone()).await;
        let schema = catalog.schema("robots").unwrap();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let schema = schema.clone();
            tasks.push(tokio::spawn(async move {
                schema.table("episodes").await.unwrap().unwrap()
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(engine.opens.load(Ordering::SeqCst), 1);
        assert_eq!(engine.providers.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn provider_cache_separates_versions() {
        let engine = Arc::new(CountingEngine::new(std::time::Duration::ZERO));
        let (_root, meta, table, catalog) = registered_catalog(engine.clone()).await;
        let schema = catalog.schema("robots").unwrap();

        schema.table("episodes").await.unwrap().unwrap();
        let registration = registry::get(meta.as_ref(), &table).await.unwrap().unwrap();
        registry::set_version(meta.as_ref(), &table, &registration, Version(2))
            .await
            .unwrap();
        catalog.invalidate_registration(&table).await;
        schema.table("episodes").await.unwrap().unwrap();
        schema.table("episodes").await.unwrap().unwrap();

        assert_eq!(engine.opens.load(Ordering::SeqCst), 2);
        assert_eq!(engine.providers.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn provider_cache_separates_incarnations() {
        let engine = Arc::new(CountingEngine::new(std::time::Duration::ZERO));
        let (_root, meta, table, catalog) = registered_catalog(engine.clone()).await;
        let schema = catalog.schema("robots").unwrap();

        schema.table("episodes").await.unwrap().unwrap();
        let old = registry::get(meta.as_ref(), &table).await.unwrap().unwrap();
        registry::delete(meta.as_ref(), &table, &old).await.unwrap();
        let replacement = TableRegistration::new(
            old.location.clone(),
            engine.kind(),
            old.current_version,
            Vec::new(),
        );
        assert_ne!(old.incarnation_id(), replacement.incarnation_id());
        registry::register(meta.as_ref(), &table, &replacement)
            .await
            .unwrap();
        catalog.invalidate_registration(&table).await;
        schema.table("episodes").await.unwrap().unwrap();

        assert_eq!(engine.opens.load(Ordering::SeqCst), 2);
        assert_eq!(engine.providers.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_provider_load_is_not_cached() {
        let engine = Arc::new(CountingEngine::new(std::time::Duration::ZERO));
        engine.fail_next.store(true, Ordering::SeqCst);
        let (_root, _meta, _table, catalog) = registered_catalog(engine.clone()).await;
        let schema = catalog.schema("robots").unwrap();

        let error = schema.table("episodes").await.unwrap_err();
        assert!(error.to_string().contains("injected provider failure"));
        schema.table("episodes").await.unwrap().unwrap();
        schema.table("episodes").await.unwrap().unwrap();

        assert_eq!(engine.opens.load(Ordering::SeqCst), 2);
        assert_eq!(engine.providers.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn provider_cache_respects_capacity() {
        let engine = Arc::new(CountingEngine::new(std::time::Duration::ZERO));
        let (_root, meta, table, catalog) = registered_catalog_with_capacity(engine, Some(2)).await;
        let schema = catalog.schema("robots").unwrap();

        for version in 1..=5 {
            schema.table("episodes").await.unwrap().unwrap();
            if version < 5 {
                let registration = registry::get(meta.as_ref(), &table).await.unwrap().unwrap();
                registry::set_version(meta.as_ref(), &table, &registration, Version(version + 1))
                    .await
                    .unwrap();
                catalog.invalidate_registration(&table).await;
            }
        }
        catalog.maintain_test_provider_cache().await;

        assert!(catalog.test_provider_cache_entry_count() <= 2);
    }

    #[tokio::test]
    async fn invalidation_fences_an_inflight_stale_registration_fill() {
        let root = tempfile::tempdir().unwrap();
        let inner = Arc::new(RocksMeta::open(root.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        let initial = TableRegistration::new(
            TableLocation::new("mem://episodes"),
            "counting",
            Version(1),
            Vec::new(),
        );
        registry::register(inner.as_ref(), &table, &initial)
            .await
            .unwrap();
        let meta = Arc::new(PausingMeta {
            inner:   inner.clone(),
            pause:   AtomicBool::new(true),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let engine = Arc::new(CountingEngine::new(std::time::Duration::ZERO));
        let meta_ref: MetaStoreRef = meta.clone();
        let engine_ref: TableEngineRef = engine.clone();
        let catalog = LakeCatalog::new(meta_ref, engine_ref);
        let schema = catalog.schema("robots").unwrap();
        let entered = meta.entered.notified();

        let stale_lookup =
            tokio::spawn(async move { schema.table("episodes").await.unwrap().unwrap() });
        entered.await;
        registry::set_version(inner.as_ref(), &table, &initial, Version(2))
            .await
            .unwrap();
        catalog.invalidate_registration(&table).await;
        meta.release.notify_one();
        stale_lookup.await.unwrap();

        catalog
            .schema("robots")
            .unwrap()
            .table("episodes")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            engine.versions.lock().unwrap().as_slice(),
            &[Version(1), Version(2)]
        );
    }
}
