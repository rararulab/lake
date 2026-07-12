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

//! Prod backend: DynamoDB. CAS is a native conditional `PutItem`, so no
//! process-local lock is needed — the store is multi-AZ HA by construction.
//!
//! The legacy on-demand table uses HASH key `pk`. Current binaries atomically
//! mirror it into a companion table keyed by `(bucket, pk)`, where `bucket`
//! isolates and shards logical key families. A durable, verified marker moves
//! read authority to the companion layout without changing [`MetaStore`] keys.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use aws_sdk_dynamodb::{
    Client,
    client::Waiters as _,
    operation::transact_write_items::TransactWriteItemsError,
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, ConditionCheck, Delete, KeySchemaElement,
        KeyType, Put, ScalarAttributeType, TransactWriteItem, Update,
    },
};
use snafu::IntoError;

use crate::{
    dynamo_layout::{PrefixCursor, bucket_for_prefix, physical_key},
    dynamo_migration::{DynamoMigrationPage, DynamoMigrationVerification},
    error::{DynamoSnafu, MetaError, Result},
    store::{GuardedMutation, GuardedTarget, MetaScanPage, MetaStore},
};

const V2_TABLE_SUFFIX: &str = "_prefix_v2";
const V2_AUTHORITY_MARKER: &str = "__lake_internal/dynamo-prefix-v2-authority";
const V2_AUTHORITY_VALUE: &[u8] = b"complete-v1";
const V2_GENERATION_KEY: &str = "__lake_internal/dynamo-prefix-v2-generation";
const V2_BACKFILL_CURSOR_KEY: &str = "__lake_internal/dynamo-prefix-v2-backfill-cursor";
const MIGRATION_PAGE_LIMIT_MAX: usize = 10_000;

/// Wrap any AWS SDK error into a [`MetaError::Dynamo`] carrying `message`.
fn dynamo_err<E>(message: impl Into<String>) -> impl FnOnce(E) -> MetaError
where
    E: std::error::Error + Send + Sync + 'static,
{
    move |source| {
        DynamoSnafu {
            message: message.into(),
        }
        .into_error(Box::new(source))
    }
}

fn transaction_condition_mismatch(error: &TransactWriteItemsError) -> bool {
    let TransactWriteItemsError::TransactionCanceledException(cancelled) = error else {
        return false;
    };
    let mut mismatch = false;
    for reason in cancelled.cancellation_reasons() {
        match reason.code() {
            None | Some("None") => {}
            Some("ConditionalCheckFailed") => mismatch = true,
            Some(_) => return false,
        }
    }
    mismatch
}

pub struct DynamoMeta {
    client:           Client,
    table:            String,
    v2_table:         String,
    v2_authoritative: AtomicBool,
}

impl DynamoMeta {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        let table = table.into();
        Self {
            client,
            v2_table: format!("{table}{V2_TABLE_SUFFIX}"),
            table,
            v2_authoritative: AtomicBool::new(false),
        }
    }

    /// Build a client from the ambient AWS config. When `endpoint_url` is
    /// `Some` (e.g. localstack) it overrides the resolved endpoint.
    pub async fn connect(endpoint_url: Option<&str>, table: &str) -> Result<Self> {
        let shared = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let mut builder = aws_sdk_dynamodb::config::Builder::from(&shared);
        if let Some(url) = endpoint_url {
            // Localstack: override the endpoint and supply a region plus dummy
            // static credentials so the signer builds without ambient AWS env.
            // The prod path (`endpoint_url = None`) keeps pure `load_defaults`.
            builder = builder
                .endpoint_url(url)
                .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                    "test",
                    "test",
                    None,
                    None,
                    "localstack",
                ));
        }
        let client = Client::from_conf(builder.build());
        Ok(Self::new(client, table))
    }

    /// Create the backing table (on-demand billing, `pk` HASH key) if it is
    /// not already present. Idempotent: a concurrent creator's
    /// `ResourceInUseException` is treated as success.
    pub async fn ensure_table(&self) -> Result<()> {
        let attribute = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .expect("pk attribute definition is complete");
        let schema = KeySchemaElement::builder()
            .attribute_name("pk")
            .key_type(KeyType::Hash)
            .build()
            .expect("pk key schema element is complete");

        let created = self
            .client
            .create_table()
            .table_name(&self.table)
            .attribute_definitions(attribute)
            .key_schema(schema)
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        match created {
            Ok(_) => {}
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|e| e.is_resource_in_use_exception()) => {}
            Err(err) => return Err(dynamo_err("create_table")(err)),
        }

        let attributes = [
            AttributeDefinition::builder()
                .attribute_name("bucket")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("bucket attribute definition is complete"),
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("pk attribute definition is complete"),
        ];
        let schema = [
            KeySchemaElement::builder()
                .attribute_name("bucket")
                .key_type(KeyType::Hash)
                .build()
                .expect("bucket key schema element is complete"),
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Range)
                .build()
                .expect("pk key schema element is complete"),
        ];
        let created = self
            .client
            .create_table()
            .table_name(&self.v2_table)
            .set_attribute_definitions(Some(attributes.into()))
            .set_key_schema(Some(schema.into()))
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        match created {
            Ok(_) => {}
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|error| error.is_resource_in_use_exception()) => {}
            Err(err) => return Err(dynamo_err("create v2 prefix table")(err)),
        }
        self.client
            .wait_until_table_exists()
            .table_name(&self.table)
            .wait(Duration::from_mins(2))
            .await
            .map_err(dynamo_err("wait for legacy table to become active"))?;
        self.client
            .wait_until_table_exists()
            .table_name(&self.v2_table)
            .wait(Duration::from_mins(2))
            .await
            .map_err(dynamo_err("wait for v2 table to become active"))?;
        self.refresh_authority().await
    }

    /// Refresh the monotonic v2-authority marker from the legacy table.
    pub async fn refresh_authority(&self) -> Result<()> {
        if self.v2_authoritative.load(Ordering::Acquire) {
            return Ok(());
        }
        let response = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(V2_AUTHORITY_MARKER.to_owned()))
            .consistent_read(true)
            .send()
            .await
            .map_err(dynamo_err("read v2 authority marker"))?;
        let complete = response
            .item()
            .and_then(|item| item.get("val"))
            .is_some_and(|value| {
                matches!(value, AttributeValue::B(blob) if blob.as_ref() == V2_AUTHORITY_VALUE)
            });
        if complete {
            self.v2_authoritative.store(true, Ordering::Release);
        }
        Ok(())
    }

    #[must_use]
    pub fn is_v2_authoritative(&self) -> bool { self.v2_authoritative.load(Ordering::Acquire) }

    async fn get_v2(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let physical = physical_key(key);
        let response = self
            .client
            .get_item()
            .table_name(&self.v2_table)
            .key("bucket", AttributeValue::S(physical.bucket))
            .key("pk", AttributeValue::S(physical.logical_key))
            .consistent_read(true)
            .send()
            .await
            .map_err(dynamo_err(format!("v2 get_item on '{key}'")))?;
        Ok(response.item().and_then(|item| match item.get("val") {
            Some(AttributeValue::B(blob)) => Some(blob.as_ref().to_vec()),
            _ => None,
        }))
    }

    async fn scan_prefix_page_v2(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<MetaScanPage> {
        if limit == 0 {
            return Ok(MetaScanPage::new(Vec::new(), None));
        }
        let cursor = match continuation {
            Some(encoded) => PrefixCursor::decode(prefix, encoded)?,
            None => PrefixCursor::first(prefix),
        };
        let shard = cursor.shard();
        let bucket = bucket_for_prefix(prefix, shard)?;
        let start_key = cursor.last_key().map(|key| {
            HashMap::from([
                ("bucket".to_owned(), AttributeValue::S(bucket.clone())),
                ("pk".to_owned(), AttributeValue::S(key.to_owned())),
            ])
        });
        let response = self
            .client
            .query()
            .table_name(&self.v2_table)
            .consistent_read(true)
            .key_condition_expression("#bucket = :bucket AND begins_with(pk, :prefix)")
            .expression_attribute_names("#bucket", "bucket")
            .expression_attribute_values(":bucket", AttributeValue::S(bucket))
            .expression_attribute_values(":prefix", AttributeValue::S(prefix.to_owned()))
            .projection_expression("pk,val")
            .set_exclusive_start_key(start_key)
            .limit(i32::try_from(limit).unwrap_or(i32::MAX))
            .send()
            .await
            .map_err(dynamo_err(format!("v2 query page for prefix '{prefix}'")))?;

        let mut entries = Vec::with_capacity(response.items().len());
        for item in response.items() {
            let (Some(AttributeValue::S(pk)), Some(AttributeValue::B(value))) =
                (item.get("pk"), item.get("val"))
            else {
                continue;
            };
            if let Some(stripped) = pk.strip_prefix(prefix) {
                entries.push((stripped.to_owned(), value.as_ref().to_vec()));
            }
        }
        let continuation = if let Some(last_key) = response
            .last_evaluated_key()
            .and_then(|key| key.get("pk"))
            .and_then(|value| match value {
                AttributeValue::S(value) => Some(value.as_str()),
                _ => None,
            }) {
            Some(PrefixCursor::after_key(prefix, shard, last_key)?.encode()?)
        } else {
            PrefixCursor::next_shard(prefix, shard)
                .map(|cursor| cursor.encode())
                .transpose()?
        };
        Ok(MetaScanPage::new(entries, continuation))
    }

    async fn scan_prefix_v2(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let mut entries = Vec::new();
        let mut continuation = None;
        loop {
            let page = self
                .scan_prefix_page_v2(prefix, continuation.as_deref(), 1_000)
                .await?;
            let (page_entries, next) = page.into_parts();
            entries.extend(page_entries);
            continuation = next;
            if continuation.is_none() {
                break;
            }
        }
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    }

    async fn get_legacy_raw(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let response = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(key.to_owned()))
            .consistent_read(true)
            .send()
            .await
            .map_err(dynamo_err(format!("migration get legacy key '{key}'")))?;
        Ok(response.item().and_then(|item| match item.get("val") {
            Some(AttributeValue::B(value)) => Some(value.as_ref().to_vec()),
            _ => None,
        }))
    }

    fn backfill_transaction_items(&self, key: &str, expected: &[u8]) -> Vec<TransactWriteItem> {
        let physical = physical_key(key);
        let guard = ConditionCheck::builder()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(key.to_owned()))
            .condition_expression("val = :expected")
            .expression_attribute_values(
                ":expected",
                AttributeValue::B(Blob::new(expected.to_vec())),
            )
            .build()
            .expect("backfill legacy guard is complete");
        let put = Put::builder()
            .table_name(&self.v2_table)
            .item("bucket", AttributeValue::S(physical.bucket))
            .item("pk", AttributeValue::S(physical.logical_key))
            .item("val", AttributeValue::B(Blob::new(expected.to_vec())))
            .condition_expression("attribute_not_exists(pk)")
            .build()
            .expect("backfill v2 put is complete");
        vec![
            TransactWriteItem::builder().condition_check(guard).build(),
            TransactWriteItem::builder().put(put).build(),
        ]
    }

    async fn copy_v2_if_absent(&self, key: &str, value: &[u8]) -> Result<bool> {
        let mut expected = Some(value.to_vec());
        for _ in 0..4 {
            let Some(current) = expected.as_deref() else {
                return Ok(false);
            };
            let result = self
                .client
                .transact_write_items()
                .set_transact_items(Some(self.backfill_transaction_items(key, current)))
                .send()
                .await;
            match result {
                Ok(_) => return Ok(true),
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(transaction_condition_mismatch) =>
                {
                    let legacy = self.get_legacy_raw(key).await?;
                    let v2 = self.get_v2(key).await?;
                    if legacy == v2 {
                        return Ok(false);
                    }
                    if v2.is_some() {
                        return Err(MetaError::MigrationConflict {
                            message: format!("key '{key}' differs between v1 and v2"),
                        });
                    }
                    expected = legacy;
                }
                Err(error) => return Err(dynamo_err(format!("backfill v2 key '{key}'"))(error)),
            }
        }
        Err(MetaError::MigrationConflict {
            message: format!("key '{key}' kept changing during backfill"),
        })
    }

    fn validate_migration_page_size(limit: usize) -> Result<i32> {
        if limit == 0 || limit > MIGRATION_PAGE_LIMIT_MAX {
            return Err(MetaError::InvalidMigrationPageSize { limit });
        }
        Ok(i32::try_from(limit).expect("migration page maximum fits i32"))
    }

    /// Copy one bounded, durably resumable page from the legacy table.
    pub async fn migrate_v2_page(&self, limit: usize) -> Result<DynamoMigrationPage> {
        let limit = Self::validate_migration_page_size(limit)?;
        let checkpoint = self.get_legacy_raw(V2_BACKFILL_CURSOR_KEY).await?;
        let checkpoint = checkpoint
            .map(String::from_utf8)
            .transpose()
            .map_err(|error| MetaError::MigrationConflict {
                message: format!("backfill cursor is not UTF-8: {error}"),
            })?;
        let start_key = checkpoint
            .as_ref()
            .map(|key| HashMap::from([("pk".to_owned(), AttributeValue::S(key.clone()))]));
        let response = self
            .client
            .scan()
            .table_name(&self.table)
            .consistent_read(true)
            .projection_expression("pk,val")
            .set_exclusive_start_key(start_key)
            .limit(limit)
            .send()
            .await
            .map_err(dynamo_err("scan bounded legacy migration page"))?;

        let mut copied = 0;
        let mut already_live = 0;
        for item in response.items() {
            let (Some(AttributeValue::S(key)), Some(AttributeValue::B(value))) =
                (item.get("pk"), item.get("val"))
            else {
                continue;
            };
            if key.starts_with("__lake_internal/") {
                continue;
            }
            if self.copy_v2_if_absent(key, value.as_ref()).await? {
                copied += 1;
            } else {
                already_live += 1;
            }
        }
        let continuation = response
            .last_evaluated_key()
            .and_then(|key| key.get("pk"))
            .and_then(|value| match value {
                AttributeValue::S(value) => Some(value.clone()),
                _ => None,
            });
        if let Some(next) = &continuation {
            self.client
                .put_item()
                .table_name(&self.table)
                .item("pk", AttributeValue::S(V2_BACKFILL_CURSOR_KEY.to_owned()))
                .item("val", AttributeValue::B(Blob::new(next.as_bytes())))
                .send()
                .await
                .map_err(dynamo_err("persist v2 backfill cursor"))?;
        } else if let Some(previous) = checkpoint {
            let _ = self
                .client
                .delete_item()
                .table_name(&self.table)
                .key("pk", AttributeValue::S(V2_BACKFILL_CURSOR_KEY.to_owned()))
                .condition_expression("val = :previous")
                .expression_attribute_values(
                    ":previous",
                    AttributeValue::B(Blob::new(previous.into_bytes())),
                )
                .send()
                .await;
        }
        Ok(DynamoMigrationPage {
            scanned: response.scanned_count().max(0) as usize,
            copied,
            already_live,
            complete: continuation.is_none(),
            continuation,
        })
    }

    async fn migration_generation(&self) -> Result<u64> {
        let response = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(V2_GENERATION_KEY.to_owned()))
            .consistent_read(true)
            .send()
            .await
            .map_err(dynamo_err("read migration generation"))?;
        response
            .item()
            .and_then(|item| item.get("generation"))
            .map_or(Ok(0), |value| match value {
                AttributeValue::N(value) => {
                    value.parse().map_err(|error| MetaError::MigrationConflict {
                        message: format!("invalid migration generation: {error}"),
                    })
                }
                _ => Err(MetaError::MigrationConflict {
                    message: "migration generation is not numeric".to_owned(),
                }),
            })
    }

    /// Verify exact v1/v2 parity under a stable dual-write generation and
    /// publish the monotonic v2 authority marker.
    pub async fn verify_and_finalize_v2(
        &self,
        page_size: usize,
    ) -> Result<DynamoMigrationVerification> {
        let page_size = Self::validate_migration_page_size(page_size)?;
        let generation = self.migration_generation().await?;
        let mut legacy_items = 0;
        let mut start = None;
        loop {
            let response = self
                .client
                .scan()
                .table_name(&self.table)
                .consistent_read(true)
                .projection_expression("pk,val")
                .set_exclusive_start_key(start.take())
                .limit(page_size)
                .send()
                .await
                .map_err(dynamo_err("verify bounded legacy page"))?;
            for item in response.items() {
                let (Some(AttributeValue::S(key)), Some(AttributeValue::B(value))) =
                    (item.get("pk"), item.get("val"))
                else {
                    continue;
                };
                if key.starts_with("__lake_internal/") {
                    continue;
                }
                legacy_items += 1;
                if self.get_v2(key).await?.as_deref() != Some(value.as_ref()) {
                    return Err(MetaError::MigrationConflict {
                        message: format!("v2 is missing or stale for legacy key '{key}'"),
                    });
                }
            }
            match response.last_evaluated_key() {
                Some(key) if !key.is_empty() => start = Some(key.clone()),
                _ => break,
            }
        }

        let mut v2_items = 0;
        let mut start = None;
        loop {
            let response = self
                .client
                .scan()
                .table_name(&self.v2_table)
                .consistent_read(true)
                .projection_expression("pk,val")
                .set_exclusive_start_key(start.take())
                .limit(page_size)
                .send()
                .await
                .map_err(dynamo_err("verify bounded v2 page"))?;
            for item in response.items() {
                let (Some(AttributeValue::S(key)), Some(AttributeValue::B(value))) =
                    (item.get("pk"), item.get("val"))
                else {
                    return Err(MetaError::MigrationConflict {
                        message: "malformed item in v2 table".to_owned(),
                    });
                };
                v2_items += 1;
                if self.get_legacy_raw(key).await?.as_deref() != Some(value.as_ref()) {
                    return Err(MetaError::MigrationConflict {
                        message: format!("v2 has no exact legacy peer for key '{key}'"),
                    });
                }
            }
            match response.last_evaluated_key() {
                Some(key) if !key.is_empty() => start = Some(key.clone()),
                _ => break,
            }
        }
        if legacy_items != v2_items {
            return Err(MetaError::MigrationConflict {
                message: format!("item counts differ: v1={legacy_items}, v2={v2_items}"),
            });
        }
        let after = self.migration_generation().await?;
        if after != generation {
            return Err(MetaError::MigrationConflict {
                message: format!("dual-write generation moved from {generation} to {after}"),
            });
        }

        let generation_condition = ConditionCheck::builder()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(V2_GENERATION_KEY.to_owned()))
            .condition_expression(if generation == 0 {
                "attribute_not_exists(generation)"
            } else {
                "generation = :generation"
            })
            .set_expression_attribute_values((generation != 0).then(|| {
                HashMap::from([(
                    ":generation".to_owned(),
                    AttributeValue::N(generation.to_string()),
                )])
            }))
            .build()
            .expect("generation finalization condition is complete");
        let marker = Put::builder()
            .table_name(&self.table)
            .item("pk", AttributeValue::S(V2_AUTHORITY_MARKER.to_owned()))
            .item("val", AttributeValue::B(Blob::new(V2_AUTHORITY_VALUE)))
            .condition_expression("attribute_not_exists(pk) OR val = :marker")
            .expression_attribute_values(
                ":marker",
                AttributeValue::B(Blob::new(V2_AUTHORITY_VALUE)),
            )
            .build()
            .expect("v2 authority marker is complete");
        let result = self
            .client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .condition_check(generation_condition)
                    .build(),
            )
            .transact_items(TransactWriteItem::builder().put(marker).build())
            .send()
            .await;
        match result {
            Ok(_) => self.v2_authoritative.store(true, Ordering::Release),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(transaction_condition_mismatch) =>
            {
                return Err(MetaError::MigrationConflict {
                    message: "dual-write generation changed during finalization".to_owned(),
                });
            }
            Err(error) => return Err(dynamo_err("publish v2 authority marker")(error)),
        }
        Ok(DynamoMigrationVerification {
            generation,
            legacy_items,
            v2_items,
            finalized: true,
        })
    }

    fn guarded_transaction_items(&self, mutation: GuardedMutation<'_>) -> Vec<TransactWriteItem> {
        let legacy_guard = ConditionCheck::builder()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(mutation.guard_key.to_owned()))
            .condition_expression("val = :guard")
            .expression_attribute_values(
                ":guard",
                AttributeValue::B(Blob::new(mutation.guard_expected.to_vec())),
            )
            .build()
            .expect("guard condition check is complete");
        let guard_physical = physical_key(mutation.guard_key);
        let mut v2_guard = ConditionCheck::builder()
            .table_name(&self.v2_table)
            .key("bucket", AttributeValue::S(guard_physical.bucket))
            .key("pk", AttributeValue::S(guard_physical.logical_key))
            .expression_attribute_values(
                ":guard",
                AttributeValue::B(Blob::new(mutation.guard_expected.to_vec())),
            );
        v2_guard = if self.is_v2_authoritative() {
            v2_guard.condition_expression("val = :guard")
        } else {
            v2_guard.condition_expression("attribute_not_exists(pk) OR val = :guard")
        };
        let v2_guard = v2_guard.build().expect("v2 guard condition is complete");

        let target_physical = physical_key(mutation.target_key);
        let (legacy_target, v2_target) = match mutation.target {
            GuardedTarget::Put { expected, value } => {
                let mut legacy_put = Put::builder()
                    .table_name(&self.table)
                    .item("pk", AttributeValue::S(mutation.target_key.to_owned()))
                    .item("val", AttributeValue::B(Blob::new(value.to_vec())));
                let mut v2_put = Put::builder()
                    .table_name(&self.v2_table)
                    .item("bucket", AttributeValue::S(target_physical.bucket))
                    .item("pk", AttributeValue::S(target_physical.logical_key))
                    .item("val", AttributeValue::B(Blob::new(value.to_vec())));
                match expected {
                    None => {
                        legacy_put = legacy_put.condition_expression("attribute_not_exists(pk)");
                        v2_put = v2_put.condition_expression("attribute_not_exists(pk)");
                    }
                    Some(bytes) => {
                        legacy_put = legacy_put
                            .condition_expression("val = :target")
                            .expression_attribute_values(
                                ":target",
                                AttributeValue::B(Blob::new(bytes.to_vec())),
                            );
                        v2_put = v2_put
                            .condition_expression(if self.is_v2_authoritative() {
                                "val = :target"
                            } else {
                                "attribute_not_exists(pk) OR val = :target"
                            })
                            .expression_attribute_values(
                                ":target",
                                AttributeValue::B(Blob::new(bytes.to_vec())),
                            );
                    }
                }
                (
                    TransactWriteItem::builder().put(
                        legacy_put
                            .build()
                            .expect("guarded legacy target put is complete"),
                    ),
                    TransactWriteItem::builder()
                        .put(v2_put.build().expect("guarded v2 target put is complete")),
                )
            }
            GuardedTarget::Delete { expected } => {
                let legacy_delete = Delete::builder()
                    .table_name(&self.table)
                    .key("pk", AttributeValue::S(mutation.target_key.to_owned()))
                    .condition_expression("val = :target")
                    .expression_attribute_values(
                        ":target",
                        AttributeValue::B(Blob::new(expected.to_vec())),
                    )
                    .build()
                    .expect("guarded legacy target delete is complete");
                let v2_delete = Delete::builder()
                    .table_name(&self.v2_table)
                    .key("bucket", AttributeValue::S(target_physical.bucket))
                    .key("pk", AttributeValue::S(target_physical.logical_key))
                    .condition_expression(if self.is_v2_authoritative() {
                        "val = :target"
                    } else {
                        "attribute_not_exists(pk) OR val = :target"
                    })
                    .expression_attribute_values(
                        ":target",
                        AttributeValue::B(Blob::new(expected.to_vec())),
                    )
                    .build()
                    .expect("guarded v2 target delete is complete");
                (
                    TransactWriteItem::builder().delete(legacy_delete),
                    TransactWriteItem::builder().delete(v2_delete),
                )
            }
        };
        vec![
            TransactWriteItem::builder()
                .condition_check(legacy_guard)
                .build(),
            TransactWriteItem::builder()
                .condition_check(v2_guard)
                .build(),
            legacy_target.build(),
            v2_target.build(),
            self.generation_update(),
        ]
    }

    fn generation_update(&self) -> TransactWriteItem {
        let update = Update::builder()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(V2_GENERATION_KEY.to_owned()))
            .update_expression("ADD generation :one")
            .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
            .build()
            .expect("generation update is complete");
        TransactWriteItem::builder().update(update).build()
    }

    fn cas_transaction_items(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Vec<TransactWriteItem> {
        let physical = physical_key(key);
        let mut legacy = Put::builder()
            .table_name(&self.table)
            .item("pk", AttributeValue::S(key.to_owned()))
            .item("val", AttributeValue::B(Blob::new(new.to_vec())));
        let mut v2 = Put::builder()
            .table_name(&self.v2_table)
            .item("bucket", AttributeValue::S(physical.bucket))
            .item("pk", AttributeValue::S(physical.logical_key))
            .item("val", AttributeValue::B(Blob::new(new.to_vec())));
        match expected {
            None => {
                legacy = legacy.condition_expression("attribute_not_exists(pk)");
                v2 = v2.condition_expression("attribute_not_exists(pk)");
            }
            Some(bytes) => {
                legacy = legacy
                    .condition_expression("val = :target")
                    .expression_attribute_values(
                        ":target",
                        AttributeValue::B(Blob::new(bytes.to_vec())),
                    );
                v2 = v2
                    .condition_expression(if self.is_v2_authoritative() {
                        "val = :target"
                    } else {
                        "attribute_not_exists(pk) OR val = :target"
                    })
                    .expression_attribute_values(
                        ":target",
                        AttributeValue::B(Blob::new(bytes.to_vec())),
                    );
            }
        }
        vec![
            TransactWriteItem::builder()
                .put(legacy.build().expect("legacy CAS put is complete"))
                .build(),
            TransactWriteItem::builder()
                .put(v2.build().expect("v2 CAS put is complete"))
                .build(),
            self.generation_update(),
        ]
    }
}

#[async_trait]
impl MetaStore for DynamoMeta {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if self.is_v2_authoritative() {
            return self.get_v2(key).await;
        }
        let resp = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(key.to_owned()))
            .consistent_read(true)
            .send()
            .await
            .map_err(dynamo_err(format!("get_item on '{key}'")))?;
        let val = resp.item().and_then(|item| match item.get("val") {
            Some(AttributeValue::B(blob)) => Some(blob.as_ref().to_vec()),
            _ => None,
        });
        Ok(val)
    }

    async fn cas(&self, key: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<bool> {
        let result = self
            .client
            .transact_write_items()
            .set_transact_items(Some(self.cas_transaction_items(key, expected, new)))
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(transaction_condition_mismatch) =>
            {
                Ok(false)
            }
            Err(error) => Err(dynamo_err(format!("dual CAS transaction on '{key}'"))(
                error,
            )),
        }
    }

    async fn guarded_mutate(&self, mutation: GuardedMutation<'_>) -> Result<bool> {
        let mutation = mutation.validate()?;
        let target_key = mutation.target_key.to_owned();
        let result = self
            .client
            .transact_write_items()
            .set_transact_items(Some(self.guarded_transaction_items(mutation)))
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(transaction_condition_mismatch) =>
            {
                Ok(false)
            }
            Err(error) => Err(dynamo_err(format!(
                "transact_write_items guarding target '{target_key}'"
            ))(error)),
        }
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        if self.is_v2_authoritative() {
            return Ok(self
                .scan_prefix_v2(prefix)
                .await?
                .into_iter()
                .map(|(key, _)| key)
                .collect());
        }
        // ponytail: a full Scan is fine at lake's ~10^4-key scale. The upgrade
        // path is a GSI keyed to make prefixes a Query rather than a table scan.
        let mut out = Vec::new();
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let resp = self
                .client
                .scan()
                .table_name(&self.table)
                .consistent_read(true)
                .projection_expression("pk")
                .filter_expression("begins_with(pk, :prefix)")
                .expression_attribute_values(":prefix", AttributeValue::S(prefix.to_owned()))
                .set_exclusive_start_key(start_key.take())
                .send()
                .await
                .map_err(dynamo_err(format!("scan for prefix '{prefix}'")))?;

            for item in resp.items() {
                if let Some(AttributeValue::S(pk)) = item.get("pk") {
                    if let Some(stripped) = pk.strip_prefix(prefix) {
                        out.push(stripped.to_owned());
                    }
                }
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => start_key = Some(key.clone()),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        if self.is_v2_authoritative() {
            return self.scan_prefix_v2(prefix).await;
        }
        let mut out = Vec::new();
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let resp = self
                .client
                .scan()
                .table_name(&self.table)
                .consistent_read(true)
                .projection_expression("pk,val")
                .filter_expression("begins_with(pk, :prefix)")
                .expression_attribute_values(":prefix", AttributeValue::S(prefix.to_owned()))
                .set_exclusive_start_key(start_key.take())
                .send()
                .await
                .map_err(dynamo_err(format!("scan entries for prefix '{prefix}'")))?;

            for item in resp.items() {
                let (Some(AttributeValue::S(pk)), Some(AttributeValue::B(value))) =
                    (item.get("pk"), item.get("val"))
                else {
                    continue;
                };
                if let Some(stripped) = pk.strip_prefix(prefix) {
                    out.push((stripped.to_owned(), value.as_ref().to_vec()));
                }
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => start_key = Some(key.clone()),
                _ => break,
            }
        }
        out.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(out)
    }

    async fn scan_prefix_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<MetaScanPage> {
        if self.is_v2_authoritative() {
            return self.scan_prefix_page_v2(prefix, continuation, limit).await;
        }
        if limit == 0 {
            return Ok(MetaScanPage::new(Vec::new(), None));
        }
        let start_key = continuation
            .map(|cursor| HashMap::from([("pk".to_owned(), AttributeValue::S(cursor.to_owned()))]));
        let response = self
            .client
            .scan()
            .table_name(&self.table)
            .consistent_read(true)
            .projection_expression("pk,val")
            .filter_expression("begins_with(pk, :prefix)")
            .expression_attribute_values(":prefix", AttributeValue::S(prefix.to_owned()))
            .set_exclusive_start_key(start_key)
            .limit(i32::try_from(limit).unwrap_or(i32::MAX))
            .send()
            .await
            .map_err(dynamo_err(format!("scan page for prefix '{prefix}'")))?;
        let mut entries = Vec::new();
        for item in response.items() {
            let (Some(AttributeValue::S(pk)), Some(AttributeValue::B(value))) =
                (item.get("pk"), item.get("val"))
            else {
                continue;
            };
            if let Some(stripped) = pk.strip_prefix(prefix) {
                entries.push((stripped.to_owned(), value.as_ref().to_vec()));
            }
        }
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let continuation = response
            .last_evaluated_key()
            .and_then(|key| key.get("pk"))
            .and_then(|value| match value {
                AttributeValue::S(value) => Some(value.clone()),
                _ => None,
            });
        Ok(MetaScanPage::new(entries, continuation))
    }

    async fn delete(&self, key: &str, expected: &[u8]) -> Result<bool> {
        let physical = physical_key(key);
        let legacy = Delete::builder()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(key.to_owned()))
            .condition_expression("val = :expected")
            .expression_attribute_values(
                ":expected",
                AttributeValue::B(Blob::new(expected.to_vec())),
            )
            .build()
            .expect("legacy conditional delete is complete");
        let v2 = Delete::builder()
            .table_name(&self.v2_table)
            .key("bucket", AttributeValue::S(physical.bucket))
            .key("pk", AttributeValue::S(physical.logical_key))
            .condition_expression(if self.is_v2_authoritative() {
                "val = :expected"
            } else {
                "attribute_not_exists(pk) OR val = :expected"
            })
            .expression_attribute_values(
                ":expected",
                AttributeValue::B(Blob::new(expected.to_vec())),
            )
            .build()
            .expect("v2 conditional delete is complete");
        let result = self
            .client
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().delete(legacy).build())
            .transact_items(TransactWriteItem::builder().delete(v2).build())
            .transact_items(self.generation_update())
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(transaction_condition_mismatch) =>
            {
                Ok(false)
            }
            Err(error) => Err(dynamo_err(format!("dual delete transaction on '{key}'"))(
                error,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::{
        config::Region,
        types::{CancellationReason, error::TransactionCanceledException},
    };

    use super::*;

    fn test_meta() -> DynamoMeta {
        let config = aws_sdk_dynamodb::Config::builder()
            .region(Region::new("us-east-1"))
            .behavior_version_latest()
            .build();
        DynamoMeta::new(Client::from_conf(config), "meta")
    }

    #[test]
    fn dynamo_guarded_mutation_is_wired() {
        let meta = test_meta();
        let create = GuardedMutation::create("lease", b"epoch-1", "target", b"value");
        let items = meta.guarded_transaction_items(create);
        assert_eq!(items.len(), 5);
        assert!(items[0].condition_check().is_some());
        assert!(items[1].condition_check().is_some());
        assert!(items[2].put().is_some());
        assert!(items[3].put().is_some());
        assert_eq!(
            items[0]
                .condition_check()
                .expect("guard item")
                .condition_expression(),
            "val = :guard"
        );

        let delete = GuardedMutation::delete("lease", b"epoch-1", "target", b"value");
        let items = meta.guarded_transaction_items(delete);
        assert!(items[2].delete().is_some());
        assert!(items[3].delete().is_some());

        let conditional = TransactionCanceledException::builder()
            .cancellation_reasons(
                CancellationReason::builder()
                    .code("ConditionalCheckFailed")
                    .build(),
            )
            .build();
        assert!(transaction_condition_mismatch(
            &TransactWriteItemsError::TransactionCanceledException(conditional)
        ));

        let conflict = TransactionCanceledException::builder()
            .cancellation_reasons(
                CancellationReason::builder()
                    .code("TransactionConflict")
                    .build(),
            )
            .build();
        assert!(!transaction_condition_mismatch(
            &TransactWriteItemsError::TransactionCanceledException(conflict)
        ));

        let mixed = TransactionCanceledException::builder()
            .cancellation_reasons(
                CancellationReason::builder()
                    .code("ConditionalCheckFailed")
                    .build(),
            )
            .cancellation_reasons(
                CancellationReason::builder()
                    .code("TransactionConflict")
                    .build(),
            )
            .build();
        assert!(!transaction_condition_mismatch(
            &TransactWriteItemsError::TransactionCanceledException(mixed)
        ));
    }

    #[test]
    fn dynamo_v2_prefix_pages_are_query_bounded() {
        let registry = bucket_for_prefix("tbl/", 7).unwrap();
        let operations = bucket_for_prefix("append-operation/", 7).unwrap();
        assert_eq!(registry, "tbl#07");
        assert_eq!(operations, "append-operation#07");
        assert_ne!(registry, operations);
        let cursor = PrefixCursor::first("tbl/");
        assert_eq!(cursor.shard(), 0);
        assert!(cursor.last_key().is_none());
    }

    #[test]
    fn dynamo_dual_cas_is_atomic_and_fail_closed() {
        let meta = test_meta();
        let create = meta.cas_transaction_items("tbl/ns/t", None, b"one");
        assert_eq!(create.len(), 3);
        assert_eq!(create[0].put().expect("legacy put").table_name(), "meta");
        assert_eq!(
            create[1].put().expect("v2 put").table_name(),
            "meta_prefix_v2"
        );
        assert_eq!(
            create[0].put().expect("legacy put").condition_expression(),
            Some("attribute_not_exists(pk)")
        );
        assert_eq!(
            create[1].put().expect("v2 put").condition_expression(),
            Some("attribute_not_exists(pk)")
        );
        assert!(create[2].update().is_some(), "generation moves atomically");

        let update = meta.cas_transaction_items("tbl/ns/t", Some(b"one"), b"two");
        assert_eq!(
            update[1].put().expect("v2 put").condition_expression(),
            Some("attribute_not_exists(pk) OR val = :target")
        );
    }

    #[test]
    fn dynamo_dual_guarded_mutation_preserves_fence() {
        let meta = test_meta();
        let items = meta.guarded_transaction_items(GuardedMutation::update(
            "lease", b"epoch-1", "tbl/ns/t", b"one", b"two",
        ));
        assert_eq!(items.len(), 5);
        for guard in &items[..2] {
            assert!(guard.condition_check().is_some());
        }
        assert_eq!(
            items[0]
                .condition_check()
                .expect("legacy guard")
                .condition_expression(),
            "val = :guard"
        );
        assert_eq!(
            items[1]
                .condition_check()
                .expect("v2 guard")
                .condition_expression(),
            "attribute_not_exists(pk) OR val = :guard"
        );
        assert!(items[4].update().is_some());
    }

    #[test]
    fn dynamo_v2_backfill_never_overwrites_newer_value() {
        let meta = test_meta();
        let items = meta.backfill_transaction_items("append-operation/v1/current", b"scanned");
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0]
                .condition_check()
                .expect("legacy value guard")
                .condition_expression(),
            "val = :expected"
        );
        assert_eq!(
            items[1]
                .put()
                .expect("conditional v2 create")
                .condition_expression(),
            Some("attribute_not_exists(pk)")
        );
    }

    #[tokio::test]
    async fn dynamo_v2_finalize_requires_exact_verified_backfill() {
        let meta = test_meta();
        assert!(matches!(
            meta.verify_and_finalize_v2(0).await,
            Err(MetaError::InvalidMigrationPageSize { limit: 0 })
        ));
        assert!(matches!(
            meta.verify_and_finalize_v2(MIGRATION_PAGE_LIMIT_MAX + 1)
                .await,
            Err(MetaError::InvalidMigrationPageSize { .. })
        ));
    }

    #[test]
    fn dynamo_v1_dual_v2_migration_roundtrip() {
        let meta = test_meta();
        assert!(!meta.is_v2_authoritative());
        let items = meta.cas_transaction_items("tbl/ns/t", None, b"registered");
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].put().expect("v1 copy").table_name(), "meta");
        assert_eq!(
            items[1].put().expect("v2 copy").table_name(),
            "meta_prefix_v2"
        );
        assert!(items[2].update().is_some());
    }
}
