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
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
};
use snafu::IntoError;

use crate::{
    error::{DynamoSnafu, MetaError, Result},
    store::MetaStore,
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
        let existing = self
            .client
            .list_tables()
            .send()
            .await
            .map_err(dynamo_err("list_tables"))?;
        if existing.table_names().contains(&self.table) {
            return Ok(());
        }

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
}

#[async_trait]
impl MetaStore for DynamoMeta {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let resp = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(key.to_owned()))
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
