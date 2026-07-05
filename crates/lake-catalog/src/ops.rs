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

//! Catalog write operations (create table). Read paths live on the
//! DataFusion providers; writes are explicit calls the metadata layer makes.

use datafusion::arrow::datatypes::SchemaRef;
use lake_common::{TableLocation, TableRef};
use lake_engine::TableEngineRef;
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use snafu::Snafu;

/// Error from a catalog write op.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum CatalogError {
    #[snafu(display("metastore/registry error"))]
    Meta { source: lake_meta::MetaError },

    #[snafu(display("engine error"))]
    Engine { source: lake_engine::EngineError },
}

/// Create a table: create the empty dataset via the engine, then register it
/// (create the empty dataset first so a registry entry never points at a
/// missing dataset — the same manifest-first-then-pointer discipline as
/// commits).
pub async fn create_table(
    meta: &MetaStoreRef,
    engine: &TableEngineRef,
    table: &TableRef,
    location: TableLocation,
    schema: SchemaRef,
) -> Result<(), CatalogError> {
    use snafu::ResultExt;

    let handle = engine
        .create(&location, schema)
        .await
        .context(EngineSnafu)?;
    let reg = TableRegistration {
        location,
        engine: engine.kind().to_string(),
        current_version: handle.current_version(),
    };
    registry::register(meta.as_ref(), table, &reg)
        .await
        .context(MetaSnafu)?;
    Ok(())
}
