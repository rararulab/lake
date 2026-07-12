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

//! Durable progress returned by the DynamoDB v1-to-v2 migrator.

use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DynamoMigrationPage {
    pub scanned:      usize,
    pub copied:       usize,
    pub already_live: usize,
    pub complete:     bool,
    pub continuation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DynamoMigrationVerification {
    pub generation:   u64,
    pub legacy_items: usize,
    pub v2_items:     usize,
    pub finalized:    bool,
}
