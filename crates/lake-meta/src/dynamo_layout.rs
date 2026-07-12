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

//! DynamoDB v2 physical key and continuation-token model.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{MetaError, Result};

pub(crate) const DYNAMO_V2_SHARDS: u8 = 64;
const CURSOR_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DynamoPhysicalKey {
    pub(crate) bucket:      String,
    pub(crate) logical_key: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrefixCursor {
    version:       u8,
    prefix_digest: String,
    shard:         u8,
    last_key:      Option<String>,
}

pub(crate) fn physical_key(logical_key: &str) -> DynamoPhysicalKey {
    let family = family(logical_key);
    let digest = Sha256::digest(logical_key.as_bytes());
    let shard = digest[0] % DYNAMO_V2_SHARDS;
    DynamoPhysicalKey {
        bucket:      format!("{family}#{shard:02x}"),
        logical_key: logical_key.to_owned(),
    }
}

pub(crate) fn bucket_for_prefix(prefix: &str, shard: u8) -> Result<String> {
    if shard >= DYNAMO_V2_SHARDS {
        return Err(MetaError::InvalidScanCursor {
            message: format!("shard {shard} is out of range"),
        });
    }
    if !prefix.contains('/') {
        return Err(MetaError::InvalidScanCursor {
            message: "v2 prefix queries must include a complete key family".to_owned(),
        });
    }
    Ok(format!("{}#{shard:02x}", family(prefix)))
}

fn family(key: &str) -> &str {
    key.split_once('/').map_or(
        "root",
        |(family, _)| {
            if family.is_empty() { "root" } else { family }
        },
    )
}

fn prefix_digest(prefix: &str) -> String {
    Sha256::digest(prefix.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}

impl PrefixCursor {
    pub(crate) fn first(prefix: &str) -> Self {
        Self {
            version:       CURSOR_VERSION,
            prefix_digest: prefix_digest(prefix),
            shard:         0,
            last_key:      None,
        }
    }

    pub(crate) fn after_key(prefix: &str, shard: u8, key: &str) -> Result<Self> {
        if shard >= DYNAMO_V2_SHARDS {
            return Err(MetaError::InvalidScanCursor {
                message: format!("shard {shard} is out of range"),
            });
        }
        if !key.starts_with(prefix) {
            return Err(MetaError::InvalidScanCursor {
                message: "last key does not belong to prefix".to_owned(),
            });
        }
        Ok(Self {
            version: CURSOR_VERSION,
            prefix_digest: prefix_digest(prefix),
            shard,
            last_key: Some(key.to_owned()),
        })
    }

    pub(crate) fn next_shard(prefix: &str, shard: u8) -> Option<Self> {
        let shard = shard.checked_add(1)?;
        (shard < DYNAMO_V2_SHARDS).then(|| Self {
            version: CURSOR_VERSION,
            prefix_digest: prefix_digest(prefix),
            shard,
            last_key: None,
        })
    }

    pub(crate) fn decode(prefix: &str, encoded: &str) -> Result<Self> {
        let cursor: Self =
            serde_json::from_str(encoded).map_err(|error| MetaError::InvalidScanCursor {
                message: error.to_string(),
            })?;
        if cursor.version != CURSOR_VERSION {
            return Err(MetaError::InvalidScanCursor {
                message: format!("unsupported version {}", cursor.version),
            });
        }
        if cursor.shard >= DYNAMO_V2_SHARDS {
            return Err(MetaError::InvalidScanCursor {
                message: format!("shard {} is out of range", cursor.shard),
            });
        }
        if cursor.prefix_digest != prefix_digest(prefix) {
            return Err(MetaError::InvalidScanCursor {
                message: "cursor belongs to a different prefix".to_owned(),
            });
        }
        if cursor
            .last_key
            .as_deref()
            .is_some_and(|key| !key.starts_with(prefix))
        {
            return Err(MetaError::InvalidScanCursor {
                message: "last key does not belong to prefix".to_owned(),
            });
        }
        Ok(cursor)
    }

    pub(crate) fn encode(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|error| MetaError::InvalidScanCursor {
            message: error.to_string(),
        })
    }

    pub(crate) const fn shard(&self) -> u8 { self.shard }

    pub(crate) fn last_key(&self) -> Option<&str> { self.last_key.as_deref() }
}

#[cfg(test)]
mod tests {
    use super::{DYNAMO_V2_SHARDS, PrefixCursor, physical_key};

    #[test]
    fn dynamo_v2_layout_is_stable_and_family_isolated() {
        let cases = [
            ("tbl/ns/video", "tbl"),
            ("append-operation/v1/op", "append-operation"),
            ("lance-manifest/ns/table/1", "lance-manifest"),
            ("lease", "root"),
            ("", "root"),
        ];

        for (logical_key, family) in cases {
            let first = physical_key(logical_key);
            let second = physical_key(logical_key);
            assert_eq!(first, second);
            assert_eq!(first.logical_key, logical_key);
            let (actual_family, shard) = first.bucket.split_once('#').expect("family#shard");
            assert_eq!(actual_family, family);
            assert!(u8::from_str_radix(shard, 16).expect("hex shard") < DYNAMO_V2_SHARDS);
        }

        assert_ne!(
            physical_key("tbl/ns/video").bucket,
            physical_key("append-operation/v1/op").bucket
        );
    }

    #[test]
    fn dynamo_v2_cursor_resumes_across_shards() {
        let prefix = "tbl/";
        let cursor = PrefixCursor::after_key(prefix, 17, "tbl/ns/video").expect("valid cursor");
        let encoded = cursor.encode().expect("encodable cursor");
        assert_eq!(
            PrefixCursor::decode(prefix, &encoded).expect("same prefix"),
            cursor
        );
        assert!(PrefixCursor::decode("append-operation/", &encoded).is_err());

        let next = PrefixCursor::next_shard(prefix, 17).expect("next shard");
        assert_eq!(next.shard(), 18);
        assert_eq!(next.last_key(), None);
        assert!(PrefixCursor::next_shard(prefix, DYNAMO_V2_SHARDS - 1).is_none());
    }
}
