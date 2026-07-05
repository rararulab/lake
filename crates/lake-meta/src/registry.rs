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

//! Lake's db→table registry, layered on the [`MetaStore`](crate::MetaStore).
//!
//! This is the metadata authority's durable state: which tables exist, where
//! they live, which engine backs them, and their current version. Entries
//! are keyed `tbl/<namespace>/<name>` so a namespace's tables are a prefix
//! scan. The registry is small (~10⁴ entries) and fully cacheable — see
//! `docs/architecture.md`.

use lake_common::{Namespace, TableLocation, TableName, TableRef, Version};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use crate::{
    error::{AlreadyRegisteredSnafu, ConflictSnafu, CorruptEntrySnafu, Result},
    store::MetaStore,
};

/// One registry entry: everything needed to route a table name to its data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRegistration {
    pub location:        TableLocation,
    /// Engine kind that backs this table (`TableEngine::kind`), so a reader
    /// routes to the right engine.
    pub engine:          String,
    pub current_version: Version,
}

fn key(table: &TableRef) -> String { format!("tbl/{}/{}", table.namespace.0, table.name.0) }

fn prefix(namespace: &Namespace) -> String { format!("tbl/{}/", namespace.0) }

/// Register a new table. Fails if one already exists (CAS on absence).
pub async fn register(
    meta: &dyn MetaStore,
    table: &TableRef,
    reg: &TableRegistration,
) -> Result<()> {
    let k = key(table);
    let bytes = serde_json::to_vec(reg).context(CorruptEntrySnafu { key: &k })?;
    let created = meta.cas(&k, None, &bytes).await?;
    ensure!(
        created,
        AlreadyRegisteredSnafu {
            table: table.to_string(),
        }
    );
    Ok(())
}

/// Look up a table's registration.
pub async fn get(meta: &dyn MetaStore, table: &TableRef) -> Result<Option<TableRegistration>> {
    let k = key(table);
    match meta.get(&k).await? {
        Some(bytes) => {
            let reg = serde_json::from_slice(&bytes).context(CorruptEntrySnafu { key: &k })?;
            Ok(Some(reg))
        }
        None => Ok(None),
    }
}

/// List the table names in a namespace (prefix scan, name stripped).
pub async fn list(meta: &dyn MetaStore, namespace: &Namespace) -> Result<Vec<TableName>> {
    let names = meta.list_prefix(&prefix(namespace)).await?;
    Ok(names.into_iter().map(TableName).collect())
}

/// List the distinct namespaces that have at least one table. Scans the
/// whole `tbl/` prefix and dedups the first path segment. Small at lake's
/// scale (~10⁴ tables); a namespace index goes here if it ever isn't.
pub async fn list_namespaces(meta: &dyn MetaStore) -> Result<Vec<Namespace>> {
    let mut seen = std::collections::BTreeSet::new();
    for rest in meta.list_prefix("tbl/").await? {
        if let Some((ns, _)) = rest.split_once('/') {
            seen.insert(ns.to_string());
        }
    }
    Ok(seen.into_iter().map(Namespace).collect())
}

/// Advance a table's current-version pointer, CAS-guarded on the expected
/// prior registration. Losers of the race get [`MetaError::Conflict`].
pub async fn set_version(
    meta: &dyn MetaStore,
    table: &TableRef,
    expected: &TableRegistration,
    new_version: Version,
) -> Result<()> {
    let k = key(table);
    let expected_bytes = serde_json::to_vec(expected).context(CorruptEntrySnafu { key: &k })?;
    let updated = TableRegistration {
        current_version: new_version,
        ..expected.clone()
    };
    let new_bytes = serde_json::to_vec(&updated).context(CorruptEntrySnafu { key: &k })?;
    let swapped = meta.cas(&k, Some(&expected_bytes), &new_bytes).await?;
    ensure!(
        swapped,
        ConflictSnafu {
            table: table.to_string(),
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rocks::RocksMeta;

    fn reg(v: u64) -> TableRegistration {
        TableRegistration {
            location:        TableLocation::new("mem://t"),
            engine:          "lance".to_string(),
            current_version: Version(v),
        }
    }

    #[tokio::test]
    async fn register_get_list_setversion() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path()).unwrap();
        let t = TableRef::new("robots", "arm_left");

        register(&meta, &t, &reg(1)).await.unwrap();
        assert!(
            register(&meta, &t, &reg(1)).await.is_err(),
            "double register must fail"
        );

        assert_eq!(
            get(&meta, &t).await.unwrap().unwrap().current_version,
            Version(1)
        );
        assert_eq!(
            list(&meta, &t.namespace).await.unwrap(),
            vec![TableName("arm_left".into())]
        );

        set_version(&meta, &t, &reg(1), Version(2)).await.unwrap();
        assert_eq!(
            get(&meta, &t).await.unwrap().unwrap().current_version,
            Version(2)
        );
        assert!(
            set_version(&meta, &t, &reg(1), Version(3)).await.is_err(),
            "stale expected must conflict"
        );
    }
}
