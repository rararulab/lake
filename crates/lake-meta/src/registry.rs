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

//! Lake's db→table registry, layered on the [`MetaStore`].
//!
//! This is the metadata authority's durable state: which tables exist, where
//! they live, which engine backs them, and their current version. Entries
//! are keyed `tbl/<namespace>/<name>` so a namespace's tables are a prefix
//! scan. The registry is small (~10⁴ entries) and fully cacheable — see
//! `docs/architecture.md`.

use lake_common::{Namespace, TableLocation, TableName, TableRef, Version};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};

use crate::{
    error::{
        AlreadyRegisteredSnafu, ConflictSnafu, CorruptEntrySnafu, InvalidScanLimitSnafu, Result,
    },
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
    /// Stable identity of this create/drop lifecycle, independent of its name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    incarnation_id:      Option<String>,
    /// Arrow IPC schema bytes owned and interpreted by the catalog layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    schema_ipc:          Option<Vec<u8>>,
}

impl TableRegistration {
    #[must_use]
    pub fn new(
        location: TableLocation,
        engine: impl Into<String>,
        current_version: Version,
        schema_ipc: Vec<u8>,
    ) -> Self {
        Self {
            location,
            engine: engine.into(),
            current_version,
            incarnation_id: Some(uuid::Uuid::now_v7().to_string()),
            schema_ipc: Some(schema_ipc),
        }
    }

    #[must_use]
    pub fn schema_ipc(&self) -> Option<&[u8]> { self.schema_ipc.as_deref() }

    /// Return the immutable identity of this table lifecycle, when migrated.
    #[must_use]
    pub fn incarnation_id(&self) -> Option<&str> { self.incarnation_id.as_deref() }
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

/// Remove the exact registration previously resolved by the caller.
/// A replacement generation produces [`crate::MetaError::Conflict`] rather than
/// being removed by a stale drop.
pub async fn delete(
    meta: &dyn MetaStore,
    table: &TableRef,
    expected: &TableRegistration,
) -> Result<()> {
    let k = key(table);
    let expected_bytes = serde_json::to_vec(expected).context(CorruptEntrySnafu { key: &k })?;
    let deleted = meta.delete(&k, &expected_bytes).await?;
    ensure!(
        deleted,
        ConflictSnafu {
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

/// Atomically add an incarnation identity to a legacy registration.
///
/// New registrations already contain one. The migration is CAS-guarded so a
/// concurrent version advance or drop/recreate cannot be overwritten.
pub async fn ensure_incarnation(
    meta: &dyn MetaStore,
    table: &TableRef,
    expected: &TableRegistration,
) -> Result<TableRegistration> {
    if expected.incarnation_id.is_some() {
        return Ok(expected.clone());
    }
    let k = key(table);
    let identity = uuid::Uuid::now_v7().to_string();
    let mut current = expected.clone();
    loop {
        if current.incarnation_id.is_some() {
            return Ok(current);
        }
        let expected_bytes = serde_json::to_vec(&current).context(CorruptEntrySnafu { key: &k })?;
        let mut migrated = current.clone();
        migrated.incarnation_id = Some(identity.clone());
        let migrated_bytes =
            serde_json::to_vec(&migrated).context(CorruptEntrySnafu { key: &k })?;
        if meta.cas(&k, Some(&expected_bytes), &migrated_bytes).await? {
            return Ok(migrated);
        }
        current = get(meta, table).await?.context(ConflictSnafu {
            table: table.to_string(),
        })?;
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

/// Scan every table registration in one metastore prefix operation.
pub async fn scan_tables(meta: &dyn MetaStore) -> Result<Vec<(TableRef, TableRegistration)>> {
    let mut tables = Vec::new();
    for (rest, bytes) in meta.scan_prefix("tbl/").await? {
        let Some((namespace, name)) = rest.split_once('/') else {
            continue;
        };
        let registration = serde_json::from_slice(&bytes).context(CorruptEntrySnafu {
            key: format!("tbl/{rest}"),
        })?;
        tables.push((TableRef::new(namespace, name), registration));
    }
    tables.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    Ok(tables)
}

/// One decoded, bounded page of table registrations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableRegistrationPage {
    tables:       Vec<(TableRef, TableRegistration)>,
    continuation: Option<String>,
}

impl TableRegistrationPage {
    /// Borrow the registrations returned by this page.
    #[must_use]
    pub fn tables(&self) -> &[(TableRef, TableRegistration)] { &self.tables }

    /// Borrow the backend-opaque token for the next page.
    #[must_use]
    pub fn continuation(&self) -> Option<&str> { self.continuation.as_deref() }

    /// Consume the page into registrations and continuation state.
    #[must_use]
    pub fn into_parts(self) -> (Vec<(TableRef, TableRegistration)>, Option<String>) {
        (self.tables, self.continuation)
    }
}

/// Scan at most `limit` table registrations and return an opaque continuation.
pub async fn scan_tables_page(
    meta: &dyn MetaStore,
    continuation: Option<&str>,
    limit: usize,
) -> Result<TableRegistrationPage> {
    ensure!(limit > 0, InvalidScanLimitSnafu);
    let page = meta.scan_prefix_page("tbl/", continuation, limit).await?;
    let (entries, continuation) = page.into_parts();
    let mut tables = Vec::with_capacity(entries.len());
    for (rest, bytes) in entries {
        let Some((namespace, name)) = rest.split_once('/') else {
            continue;
        };
        let registration = serde_json::from_slice(&bytes).context(CorruptEntrySnafu {
            key: format!("tbl/{rest}"),
        })?;
        tables.push((TableRef::new(namespace, name), registration));
    }
    tables.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    Ok(TableRegistrationPage {
        tables,
        continuation,
    })
}

/// Advance a table's current-version pointer, CAS-guarded on the expected
/// prior registration. Losers of the race get [`crate::MetaError::Conflict`].
pub async fn set_version(
    meta: &dyn MetaStore,
    table: &TableRef,
    expected: &TableRegistration,
    new_version: Version,
) -> Result<()> {
    let k = key(table);
    let expected_bytes = serde_json::to_vec(expected).context(CorruptEntrySnafu { key: &k })?;
    let mut updated = expected.clone();
    updated.current_version = new_version;
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
        TableRegistration::new(
            TableLocation::new("mem://t"),
            "lance",
            Version(v),
            vec![1, 2, 3],
        )
    }

    #[test]
    fn registration_schema_payload_is_backward_compatible() {
        let legacy: TableRegistration = serde_json::from_str(
            r#"{"location":"mem://legacy","engine":"lance","current_version":1}"#,
        )
        .unwrap();
        assert_eq!(legacy.schema_ipc(), None);

        let current = reg(7);
        let wire = serde_json::to_vec(&current).unwrap();
        let decoded: TableRegistration = serde_json::from_slice(&wire).unwrap();
        assert_eq!(decoded.schema_ipc(), Some(&[1, 2, 3][..]));
    }

    #[tokio::test]
    async fn scan_table_pages_are_bounded_and_resumable() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path()).unwrap();
        let expected = (0..5)
            .map(|index| TableRef::new("robots", format!("episodes-{index}")))
            .collect::<std::collections::BTreeSet<_>>();
        for table in &expected {
            register(&meta, table, &reg(1)).await.unwrap();
        }

        let mut cursor = None;
        let mut actual = std::collections::BTreeSet::new();
        let mut page_count = 0;
        loop {
            let page = scan_tables_page(&meta, cursor.as_deref(), 2).await.unwrap();
            assert!(page.tables().len() <= 2);
            page_count += 1;
            for (table, _) in page.tables() {
                assert!(
                    actual.insert(table.clone()),
                    "table returned twice: {table}"
                );
            }
            cursor = page.continuation().map(ToOwned::to_owned);
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(page_count, 3);
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn legacy_incarnation_migration_preserves_registration() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path()).unwrap();
        let table = TableRef::new("robots", "legacy");
        let legacy: TableRegistration = serde_json::from_str(
            r#"{"location":"mem://legacy","engine":"lance","current_version":7,"schema_ipc":[1,2,3]}"#,
        )
        .unwrap();
        register(&meta, &table, &legacy).await.unwrap();

        let migrated = ensure_incarnation(&meta, &table, &legacy).await.unwrap();

        assert!(migrated.incarnation_id().is_some());
        assert_eq!(migrated.location, legacy.location);
        assert_eq!(migrated.engine, legacy.engine);
        assert_eq!(migrated.current_version, legacy.current_version);
        assert_eq!(migrated.schema_ipc(), legacy.schema_ipc());
        assert_eq!(get(&meta, &table).await.unwrap(), Some(migrated));
    }

    #[tokio::test]
    async fn legacy_incarnation_migration_reloads_after_cas_races() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path()).unwrap();
        let table = TableRef::new("robots", "legacy");
        let legacy: TableRegistration = serde_json::from_str(
            r#"{"location":"mem://legacy","engine":"lance","current_version":1,"schema_ipc":[1]}"#,
        )
        .unwrap();
        register(&meta, &table, &legacy).await.unwrap();
        let stale = get(&meta, &table).await.unwrap().unwrap();
        set_version(&meta, &table, &legacy, Version(2))
            .await
            .unwrap();

        let migrated = ensure_incarnation(&meta, &table, &stale).await.unwrap();
        assert_eq!(migrated.current_version, Version(2));
        assert_eq!(migrated.location, TableLocation::new("mem://legacy"));
        assert!(migrated.incarnation_id().is_some());

        let recreated_table = TableRef::new("robots", "recreated_legacy");
        register(&meta, &recreated_table, &legacy).await.unwrap();
        let before_recreate = get(&meta, &recreated_table).await.unwrap().unwrap();
        delete(&meta, &recreated_table, &before_recreate)
            .await
            .unwrap();
        let replacement = reg(9);
        register(&meta, &recreated_table, &replacement)
            .await
            .unwrap();

        let observed = ensure_incarnation(&meta, &recreated_table, &before_recreate)
            .await
            .unwrap();
        assert_eq!(observed, replacement);
        assert_ne!(observed.incarnation_id(), before_recreate.incarnation_id());
    }

    #[tokio::test]
    async fn register_get_list_setversion() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path()).unwrap();
        let t = TableRef::new("robots", "arm_left");
        let original = reg(1);

        register(&meta, &t, &original).await.unwrap();
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

        set_version(&meta, &t, &original, Version(2)).await.unwrap();
        assert_eq!(
            get(&meta, &t).await.unwrap().unwrap().current_version,
            Version(2)
        );
        assert!(
            set_version(&meta, &t, &original, Version(3)).await.is_err(),
            "stale expected must conflict"
        );
    }

    #[tokio::test]
    async fn stale_delete_cannot_remove_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path()).unwrap();
        let table = TableRef::new("robots", "arm_left");
        let original = reg(1);
        let replacement = TableRegistration {
            location: TableLocation::new("mem://replacement"),
            current_version: Version(7),
            ..original.clone()
        };

        register(&meta, &table, &original).await.unwrap();
        let stale_observation = get(&meta, &table).await.unwrap().unwrap();

        delete(&meta, &table, &original).await.unwrap();
        register(&meta, &table, &replacement).await.unwrap();

        // A delayed drop that resolved `original` before the replacement was
        // registered must not remove the new generation.
        assert!(delete(&meta, &table, &stale_observation).await.is_err());
        assert_eq!(stale_observation, original);
        assert_eq!(get(&meta, &table).await.unwrap(), Some(replacement));
    }
}
