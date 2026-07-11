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

//! Bounded inventory pages over one managed-object stage.

use async_trait::async_trait;

use crate::{ObjectCandidate, ObjectError, Result};

const MAX_INVENTORY_PAGE_SIZE: usize = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryRequest {
    cursor:    Option<String>,
    max_items: usize,
}

impl InventoryRequest {
    pub fn try_new(cursor: Option<String>, max_items: usize) -> Result<Self> {
        if max_items == 0 || max_items > MAX_INVENTORY_PAGE_SIZE {
            return Err(ObjectError::InvalidGcConfig {
                message: format!(
                    "inventory page size must be within 1..={MAX_INVENTORY_PAGE_SIZE}"
                ),
            });
        }
        Ok(Self { cursor, max_items })
    }

    pub(crate) fn into_parts(self) -> (Option<String>, usize) { (self.cursor, self.max_items) }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryPage {
    candidates:  Vec<ObjectCandidate>,
    next_cursor: Option<String>,
}

impl InventoryPage {
    pub(crate) fn new(candidates: Vec<ObjectCandidate>, next_cursor: Option<String>) -> Self {
        Self {
            candidates,
            next_cursor,
        }
    }

    #[must_use]
    pub fn candidates(&self) -> &[ObjectCandidate] { &self.candidates }

    #[must_use]
    pub fn next_cursor(&self) -> Option<&str> { self.next_cursor.as_deref() }
}

#[async_trait]
pub trait ManagedObjectInventory: Send + Sync {
    /// URI prefix containing only immutable objects owned by this stage.
    fn managed_uri_prefix(&self) -> String;

    /// Read one bounded, strictly URI-sorted inventory page.
    async fn inventory_page(&self, request: InventoryRequest) -> Result<InventoryPage>;
}
