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

//! The HA KV metastore.
//!
//! [`MetaStore`] is the KV backend abstraction (a small `KvBackend`
//! analog): tiny mutable pointers only, mutated via compare-and-set. The
//! [`registry`] module layers lake's db→table registry on top of it. The
//! authoritative durable state of the whole metadata tier lives here.
//!
//! Backends: [`RocksMeta`] for dev. `DynamoMeta` (prod, multi-AZ HA via
//! conditional puts) is v1 — see `docs/architecture.md`; the trait is the
//! seam it will slot into.

mod dynamo;
mod error;
pub mod registry;
mod rocks;
mod store;

pub use dynamo::DynamoMeta;
pub use error::{MetaError, Result};
pub use rocks::RocksMeta;
pub use store::{MetaStore, MetaStoreRef};
