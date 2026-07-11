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
//! Schema: one on-demand table, partition key `pk` (String) holding the
//! [`MetaStore`] key, attribute `val` (Binary) holding the value bytes.

use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_dynamodb::{
    Client,
    operation::transact_write_items::TransactWriteItemsError,
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, ConditionCheck, Delete, KeySchemaElement,
        KeyType, Put, ScalarAttributeType, TransactWriteItem,
    },
};
use snafu::IntoError;

use crate::{
    error::{DynamoSnafu, MetaError, Result},
    store::{GuardedMutation, GuardedTarget, MetaScanPage, MetaStore},
};

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
    client: Client,
    table:  String,
}

impl DynamoMeta {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
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
            Ok(_) => Ok(()),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|e| e.is_resource_in_use_exception()) =>
            {
                Ok(())
            }
            Err(err) => Err(dynamo_err("create_table")(err)),
        }
    }

    fn guarded_transaction_items(&self, mutation: GuardedMutation<'_>) -> Vec<TransactWriteItem> {
        let guard = ConditionCheck::builder()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(mutation.guard_key.to_owned()))
            .condition_expression("val = :guard")
            .expression_attribute_values(
                ":guard",
                AttributeValue::B(Blob::new(mutation.guard_expected.to_vec())),
            )
            .build()
            .expect("guard condition check is complete");
        let target = match mutation.target {
            GuardedTarget::Put { expected, value } => {
                let mut put = Put::builder()
                    .table_name(&self.table)
                    .item("pk", AttributeValue::S(mutation.target_key.to_owned()))
                    .item("val", AttributeValue::B(Blob::new(value.to_vec())));
                put = match expected {
                    None => put.condition_expression("attribute_not_exists(pk)"),
                    Some(bytes) => put
                        .condition_expression("val = :target")
                        .expression_attribute_values(
                            ":target",
                            AttributeValue::B(Blob::new(bytes.to_vec())),
                        ),
                };
                TransactWriteItem::builder()
                    .put(put.build().expect("guarded target put is complete"))
            }
            GuardedTarget::Delete { expected } => {
                let delete = Delete::builder()
                    .table_name(&self.table)
                    .key("pk", AttributeValue::S(mutation.target_key.to_owned()))
                    .condition_expression("val = :target")
                    .expression_attribute_values(
                        ":target",
                        AttributeValue::B(Blob::new(expected.to_vec())),
                    )
                    .build()
                    .expect("guarded target delete is complete");
                TransactWriteItem::builder().delete(delete)
            }
        };
        vec![
            TransactWriteItem::builder().condition_check(guard).build(),
            target.build(),
        ]
    }
}

#[async_trait]
impl MetaStore for DynamoMeta {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
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
        let mut req = self
            .client
            .put_item()
            .table_name(&self.table)
            .item("pk", AttributeValue::S(key.to_owned()))
            .item("val", AttributeValue::B(Blob::new(new.to_vec())));
        req = match expected {
            None => req.condition_expression("attribute_not_exists(pk)"),
            Some(bytes) => req
                .condition_expression("val = :expected")
                .expression_attribute_values(
                    ":expected",
                    AttributeValue::B(Blob::new(bytes.to_vec())),
                ),
        };

        match req.send().await {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|e| e.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(err) => Err(dynamo_err(format!("put_item on '{key}'"))(err)),
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
        let result = self
            .client
            .delete_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(key.to_owned()))
            .condition_expression("val = :expected")
            .expression_attribute_values(
                ":expected",
                AttributeValue::B(Blob::new(expected.to_vec())),
            )
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|e| e.is_conditional_check_failed_exception()) =>
            {
                Ok(false)
            }
            Err(err) => Err(dynamo_err(format!("delete_item on '{key}'"))(err)),
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
        assert_eq!(items.len(), 2);
        assert!(items[0].condition_check().is_some());
        assert!(items[1].put().is_some());
        assert_eq!(
            items[0]
                .condition_check()
                .expect("guard item")
                .condition_expression(),
            "val = :guard"
        );

        let delete = GuardedMutation::delete("lease", b"epoch-1", "target", b"value");
        let items = meta.guarded_transaction_items(delete);
        assert!(items[1].delete().is_some());

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
}
