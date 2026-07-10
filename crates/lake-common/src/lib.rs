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

//! Shared identifiers used across every lake tier.
//!
//! These are deliberately thin newtypes over `String` / `u64`: they cost
//! nothing at runtime but stop a `TableName` from being passed where a
//! `Namespace` is expected. Nothing here does I/O or pulls in a tier's
//! dependencies, so every crate can depend on `lake-common` freely.

mod data_location;
mod ids;
mod location;

pub use data_location::DataLocation;
pub use ids::{Namespace, TableName, TableRef, Version};
pub use location::TableLocation;
