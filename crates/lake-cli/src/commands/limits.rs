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

//! Startup parsing for immutable Query admission limits.

use std::{str::FromStr, time::Duration};

use anyhow::Context as _;
use lake_query::QueryLimits;

pub(crate) fn query_limits_from_env() -> anyhow::Result<QueryLimits> {
    let max_concurrent = env_value("LAKE_QUERY_MAX_CONCURRENT")?;
    let queue_ms = env_value("LAKE_QUERY_QUEUE_TIMEOUT_MS")?;
    let execution_ms = env_value("LAKE_QUERY_EXECUTION_TIMEOUT_MS")?;
    let max_sql_bytes = env_value("LAKE_QUERY_MAX_SQL_BYTES")?;
    query_limits_from_values(
        max_concurrent.as_deref(),
        queue_ms.as_deref(),
        execution_ms.as_deref(),
        max_sql_bytes.as_deref(),
    )
}

fn query_limits_from_values(
    max_concurrent: Option<&str>,
    queue_ms: Option<&str>,
    execution_ms: Option<&str>,
    max_sql_bytes: Option<&str>,
) -> anyhow::Result<QueryLimits> {
    let defaults = QueryLimits::default();
    let max_concurrent = parse_or(
        "LAKE_QUERY_MAX_CONCURRENT",
        max_concurrent,
        defaults.max_concurrent(),
    )?;
    let queue_ms = parse_or(
        "LAKE_QUERY_QUEUE_TIMEOUT_MS",
        queue_ms,
        u64::try_from(defaults.queue_wait().as_millis()).expect("default queue duration fits u64"),
    )?;
    let execution_ms = parse_or(
        "LAKE_QUERY_EXECUTION_TIMEOUT_MS",
        execution_ms,
        u64::try_from(defaults.execution_time().as_millis())
            .expect("default execution duration fits u64"),
    )?;
    let max_sql_bytes = parse_or(
        "LAKE_QUERY_MAX_SQL_BYTES",
        max_sql_bytes,
        defaults.max_sql_bytes(),
    )?;
    QueryLimits::try_new(
        max_concurrent,
        Duration::from_millis(queue_ms),
        Duration::from_millis(execution_ms),
        max_sql_bytes,
    )
    .context("invalid Query admission limits")
}

fn parse_or<T>(name: &str, value: Option<&str>, default: T) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match value {
        Some(value) => value
            .parse()
            .with_context(|| format!("{name} must be a positive integer")),
        None => Ok(default),
    }
}

fn env_value(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::query_limits_from_values;

    #[test]
    fn query_limit_values_are_validated_before_serving() {
        assert!(query_limits_from_values(Some("0"), None, None, None).is_err());
        assert!(query_limits_from_values(Some("many"), None, None, None).is_err());
        assert!(query_limits_from_values(None, Some("0"), None, None).is_err());

        let limits = query_limits_from_values(Some("7"), Some("250"), Some("5000"), Some("4096"))
            .expect("valid limits");
        assert_eq!(limits.max_concurrent(), 7);
        assert_eq!(limits.queue_wait(), Duration::from_millis(250));
        assert_eq!(limits.execution_time(), Duration::from_secs(5));
        assert_eq!(limits.max_sql_bytes(), 4096);
    }
}
