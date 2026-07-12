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

//! The db→table catalog: resolves table names to DataFusion tables.
//!
//! [`LakeCatalog`] maps a lake namespace to a DataFusion schema and a table
//! name to a [`TableProvider`](datafusion::catalog::TableProvider) obtained
//! from the storage engine. It is the cache shield in front of the metadata
//! authority: DataFusion's sync listing methods read an in-memory snapshot
//! (never blocking on I/O), and per-table lookups hit a moka cache before
//! the registry. Refresh the snapshot with [`LakeCatalog::refresh`].

mod catalog;
mod ops;
mod schema;

pub use catalog::{
    CatalogGeneration, CatalogRefreshHealth, CatalogState, LakeCatalog, ProviderLoadError,
    TableSnapshot,
};
pub use ops::{CatalogError, create_table};
