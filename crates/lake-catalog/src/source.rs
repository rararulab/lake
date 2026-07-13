// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! Read-only catalog authority seam used by the Query cache.

use std::{error::Error, sync::Arc};

use async_trait::async_trait;
use lake_common::TableRef;
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

const DIRECTORY_REFRESH_ATTEMPTS: usize = 3;

pub const CATALOG_SOURCE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Snafu)]
pub enum CatalogSourceError {
    #[snafu(display("local catalog source failed: {source}"))]
    Local { source: lake_meta::MetaError },

    #[snafu(display("remote catalog source is unavailable"))]
    Remote {
        source: Box<dyn Error + Send + Sync>,
    },

    #[snafu(display("catalog directory changed while its snapshot was loading"))]
    DirectoryGenerationChanged,

    #[snafu(display("catalog source returned an invalid response"))]
    InvalidResponse,
}

impl CatalogSourceError {
    pub fn remote(source: impl Error + Send + Sync + 'static) -> Self {
        Self::Remote {
            source: Box::new(source),
        }
    }
}

pub type CatalogSourceResult<T> = Result<T, CatalogSourceError>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDirectoryRequest {
    pub schema_version:   u32,
    pub known_generation: Option<Vec<u8>>,
}

impl CatalogDirectoryRequest {
    #[must_use]
    pub fn new(known_generation: Option<Vec<u8>>) -> Self {
        Self {
            schema_version: CATALOG_SOURCE_SCHEMA_VERSION,
            known_generation,
        }
    }
}

impl Default for CatalogDirectoryRequest {
    fn default() -> Self { Self::new(None) }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogDirectoryResponse {
    NotModified {
        schema_version: u32,
        generation:     Vec<u8>,
    },
    Snapshot {
        schema_version: u32,
        generation:     Option<Vec<u8>>,
        registrations:  Vec<(TableRef, TableRegistration)>,
    },
}

#[async_trait]
pub trait CatalogSource: Send + Sync {
    async fn resolve(&self, table: &TableRef) -> CatalogSourceResult<Option<TableRegistration>>;

    async fn directory(
        &self,
        request: CatalogDirectoryRequest,
    ) -> CatalogSourceResult<CatalogDirectoryResponse>;
}

pub type CatalogSourceRef = Arc<dyn CatalogSource>;

#[derive(Clone)]
pub struct LocalCatalogSource {
    meta: MetaStoreRef,
}

impl LocalCatalogSource {
    #[must_use]
    pub fn new(meta: MetaStoreRef) -> Self { Self { meta } }
}

#[async_trait]
impl CatalogSource for LocalCatalogSource {
    async fn resolve(&self, table: &TableRef) -> CatalogSourceResult<Option<TableRegistration>> {
        registry::get(self.meta.as_ref(), table)
            .await
            .map_err(|source| CatalogSourceError::Local { source })
    }

    async fn directory(
        &self,
        request: CatalogDirectoryRequest,
    ) -> CatalogSourceResult<CatalogDirectoryResponse> {
        if request.schema_version != CATALOG_SOURCE_SCHEMA_VERSION {
            return Err(CatalogSourceError::InvalidResponse);
        }
        let state = registry::directory_state(self.meta.as_ref())
            .await
            .map_err(|source| CatalogSourceError::Local { source })?;
        let mut generation = state.generation().map(<[u8]>::to_vec);
        if state.authoritative() && request.known_generation.as_deref() == generation.as_deref() {
            return Ok(CatalogDirectoryResponse::NotModified {
                schema_version: CATALOG_SOURCE_SCHEMA_VERSION,
                generation:     generation.expect("authoritative state has a generation"),
            });
        }

        for attempt in 0..DIRECTORY_REFRESH_ATTEMPTS {
            let registrations = registry::scan_tables(self.meta.as_ref())
                .await
                .map_err(|source| CatalogSourceError::Local { source })?;
            if state.authoritative() {
                let after = registry::directory_generation(self.meta.as_ref())
                    .await
                    .map_err(|source| CatalogSourceError::Local { source })?;
                if generation.as_deref() != Some(after.as_slice()) {
                    generation = Some(after);
                    if attempt + 1 == DIRECTORY_REFRESH_ATTEMPTS {
                        return Err(CatalogSourceError::DirectoryGenerationChanged);
                    }
                    continue;
                }
            }
            return Ok(CatalogDirectoryResponse::Snapshot {
                schema_version: CATALOG_SOURCE_SCHEMA_VERSION,
                generation,
                registrations,
            });
        }
        unreachable!("directory refresh attempts are non-zero")
    }
}
