// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! Durable, incarnation-bound table-drop intent records.

use lake_common::TableRef;
use lake_meta::{MetaStore, registry::TableRegistration};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu, ensure};

pub(crate) const DROP_PREFIX: &str = "drop/";
const MAX_TOMBSTONES_PER_TABLE: usize = 1;

#[derive(Debug, Snafu)]
pub(crate) enum DropTombstoneError {
    #[snafu(display("table {table} has no incarnation identity"))]
    MissingIncarnation { table: TableRef },

    #[snafu(display("drop tombstone {key} could not be encoded or decoded"))]
    Codec {
        key:    String,
        source: serde_json::Error,
    },

    #[snafu(display("drop tombstone {key} conflicts with durable state"))]
    Conflict { key: String },

    #[snafu(display("drop tombstone metastore operation failed"))]
    Store { source: lake_meta::MetaError },
}

type Result<T> = std::result::Result<T, DropTombstoneError>;

/// Immutable intent to detach one exact table incarnation and remove its old
/// dataset. The registration is deliberately complete but compact: cleanup
/// never needs the vanished registry pointer to recover its engine/location.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DropTombstone {
    pub(crate) table:        TableRef,
    pub(crate) registration: TableRegistration,
}

impl DropTombstone {
    pub(crate) fn new(table: TableRef, registration: TableRegistration) -> Result<Self> {
        ensure!(
            registration.incarnation_id().is_some(),
            MissingIncarnationSnafu {
                table: table.clone(),
            }
        );
        Ok(Self {
            table,
            registration,
        })
    }

    pub(crate) fn key(&self) -> String {
        format!(
            "{DROP_PREFIX}{}/{}/{}",
            self.table.namespace.0,
            self.table.name.0,
            self.registration
                .incarnation_id()
                .expect("constructor requires an incarnation")
        )
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let key = self.key();
        serde_json::to_vec(self).context(CodecSnafu { key })
    }

    fn decode(key: &str, bytes: &[u8]) -> Result<Self> {
        let tombstone: Self = serde_json::from_slice(bytes).context(CodecSnafu {
            key: key.to_owned(),
        })?;
        ensure!(
            tombstone.key() == key,
            ConflictSnafu {
                key: key.to_owned(),
            }
        );
        Ok(tombstone)
    }
}

fn table_prefix(table: &TableRef) -> String {
    format!("{DROP_PREFIX}{}/{}/", table.namespace.0, table.name.0)
}

/// Idempotently persist the exact immutable intent before object mutation.
pub(crate) async fn prepare(meta: &dyn MetaStore, tombstone: &DropTombstone) -> Result<()> {
    let key = tombstone.key();
    let encoded = tombstone.encode()?;
    if meta.cas(&key, None, &encoded).await.context(StoreSnafu)? {
        return Ok(());
    }
    let durable = meta.get(&key).await.context(StoreSnafu)?;
    ensure!(
        durable.as_deref() == Some(encoded.as_slice()),
        ConflictSnafu { key }
    );
    Ok(())
}

pub(crate) async fn exists(meta: &dyn MetaStore, tombstone: &DropTombstone) -> Result<bool> {
    let key = tombstone.key();
    let Some(bytes) = meta.get(&key).await.context(StoreSnafu)? else {
        return Ok(false);
    };
    ensure!(
        DropTombstone::decode(&key, &bytes)? == *tombstone,
        ConflictSnafu { key }
    );
    Ok(true)
}

pub(crate) async fn list_for_table(
    meta: &dyn MetaStore,
    table: &TableRef,
) -> Result<Vec<DropTombstone>> {
    let prefix = table_prefix(table);
    let page = meta
        .scan_prefix_page(&prefix, None, MAX_TOMBSTONES_PER_TABLE + 1)
        .await
        .context(StoreSnafu)?;
    let (entries, continuation) = page.into_parts();
    ensure!(
        continuation.is_none() && entries.len() <= MAX_TOMBSTONES_PER_TABLE,
        ConflictSnafu {
            key: prefix.clone(),
        }
    );
    let mut tombstones = Vec::new();
    for (suffix, bytes) in entries {
        tombstones.push(DropTombstone::decode(&format!("{prefix}{suffix}"), &bytes)?);
    }
    tombstones.sort_unstable_by_key(DropTombstone::key);
    Ok(tombstones)
}

pub(crate) async fn scan_page(
    meta: &dyn MetaStore,
    continuation: Option<&str>,
    limit: usize,
) -> Result<(Vec<DropTombstone>, Option<String>)> {
    let page = meta
        .scan_prefix_page(DROP_PREFIX, continuation, limit)
        .await
        .context(StoreSnafu)?;
    let (entries, continuation) = page.into_parts();
    let tombstones = entries
        .into_iter()
        .map(|(suffix, bytes)| DropTombstone::decode(&format!("{DROP_PREFIX}{suffix}"), &bytes))
        .collect::<Result<Vec<_>>>()?;
    Ok((tombstones, continuation))
}

/// Idempotently clear the exact tombstone only after object cleanup finishes.
pub(crate) async fn finish(meta: &dyn MetaStore, tombstone: &DropTombstone) -> Result<()> {
    let key = tombstone.key();
    let encoded = tombstone.encode()?;
    if meta.delete(&key, &encoded).await.context(StoreSnafu)? {
        return Ok(());
    }
    ensure!(
        meta.get(&key).await.context(StoreSnafu)?.is_none(),
        ConflictSnafu { key }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use lake_common::{TableLocation, TableRef, Version};
    use lake_meta::{MetaStore, RocksMeta, registry::TableRegistration};

    use super::{DropTombstone, list_for_table, prepare};

    fn registration(location: &str) -> TableRegistration {
        TableRegistration::new(
            TableLocation::new(location),
            "lance",
            Version(1),
            vec![1, 2, 3],
        )
    }

    #[tokio::test]
    async fn tombstone_prepare_is_idempotent_and_incarnation_bound() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path()).unwrap();
        let table = TableRef::new("robots", "episodes");
        let original = DropTombstone::new(table.clone(), registration("mem://old")).unwrap();

        prepare(&meta, &original).await.unwrap();
        prepare(&meta, &original).await.unwrap();
        assert_eq!(
            list_for_table(&meta, &table).await.unwrap(),
            vec![original.clone()]
        );

        let replacement = DropTombstone::new(table.clone(), registration("mem://new")).unwrap();
        assert_ne!(original.key(), replacement.key());
        assert_eq!(meta.list_prefix("drop/").await.unwrap().len(), 1);
    }
}
