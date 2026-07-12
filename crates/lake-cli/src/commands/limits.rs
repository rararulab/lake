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

use std::{path::PathBuf, str::FromStr, time::Duration};

use anyhow::Context as _;
use lake_engine_lance::{DEFAULT_RETAINED_VERSIONS, LanceMaintenancePolicy};
use lake_metasrv::{AppendLimits, DEFAULT_APPEND_OPERATION_RETENTION, MaintenanceLimits};
use lake_query::{DiscoveryLimits, QueryLimits, QueryResources};

const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
const DEFAULT_OPERATION_GC_PAGE_SIZE: usize = 128;
const MAX_OPERATION_GC_PAGE_SIZE: usize = 10_000;
const DEFAULT_QUERY_TICKET_TTL: Duration = Duration::from_mins(5);
const MAX_QUERY_TICKET_TTL_SECS: u64 = 3600;

pub(crate) fn lance_maintenance_policy_from_env() -> anyhow::Result<LanceMaintenancePolicy> {
    let retained_versions = env_value("LAKE_LANCE_RETAIN_VERSIONS")?;
    lance_maintenance_policy_from_value(retained_versions.as_deref())
}

fn lance_maintenance_policy_from_value(
    retained_versions: Option<&str>,
) -> anyhow::Result<LanceMaintenancePolicy> {
    let retained_versions = parse_or(
        "LAKE_LANCE_RETAIN_VERSIONS",
        retained_versions,
        DEFAULT_RETAINED_VERSIONS,
    )?;
    LanceMaintenancePolicy::try_new(retained_versions).context("invalid Lance maintenance policy")
}

pub(crate) fn operation_policy_from_env() -> anyhow::Result<(Duration, usize)> {
    let retention = env_value("LAKE_APPEND_OPERATION_RETENTION_SECS")?;
    let page_size = env_value("LAKE_APPEND_OPERATION_GC_PAGE_SIZE")?;
    operation_policy_from_values(retention.as_deref(), page_size.as_deref())
}

pub(crate) fn append_limits_from_env() -> anyhow::Result<AppendLimits> {
    let max_concurrent = env_value("LAKE_APPEND_MAX_CONCURRENT")?;
    let queue_ms = env_value("LAKE_APPEND_QUEUE_TIMEOUT_MS")?;
    let max_stream_bytes = env_value("LAKE_APPEND_MAX_STREAM_BYTES")?;
    let max_buffered_bytes = env_value("LAKE_APPEND_MAX_BUFFERED_BYTES")?;
    append_limits_from_values(
        max_concurrent.as_deref(),
        queue_ms.as_deref(),
        max_stream_bytes.as_deref(),
        max_buffered_bytes.as_deref(),
    )
}

pub(crate) fn maintenance_limits_from_env() -> anyhow::Result<MaintenanceLimits> {
    let interval_secs = env_value("LAKE_MAINTENANCE_INTERVAL_SECS")?;
    let table_page_size = env_value("LAKE_MAINTENANCE_TABLE_PAGE_SIZE")?;
    let operation_gc_max_pages = env_value("LAKE_MAINTENANCE_OPERATION_GC_MAX_PAGES")?;
    let operation_gc_max_ms = env_value("LAKE_MAINTENANCE_OPERATION_GC_MAX_MS")?;
    maintenance_limits_from_values(
        interval_secs.as_deref(),
        table_page_size.as_deref(),
        operation_gc_max_pages.as_deref(),
        operation_gc_max_ms.as_deref(),
    )
}

fn maintenance_limits_from_values(
    interval_secs: Option<&str>,
    table_page_size: Option<&str>,
    operation_gc_max_pages: Option<&str>,
    operation_gc_max_ms: Option<&str>,
) -> anyhow::Result<MaintenanceLimits> {
    let defaults = MaintenanceLimits::default();
    let interval_secs = parse_or(
        "LAKE_MAINTENANCE_INTERVAL_SECS",
        interval_secs,
        defaults.interval().as_secs(),
    )?;
    let table_page_size = parse_or(
        "LAKE_MAINTENANCE_TABLE_PAGE_SIZE",
        table_page_size,
        defaults.table_page_size(),
    )?;
    let operation_gc_max_pages = parse_or(
        "LAKE_MAINTENANCE_OPERATION_GC_MAX_PAGES",
        operation_gc_max_pages,
        defaults.operation_gc_max_pages(),
    )?;
    let operation_gc_max_ms = parse_or(
        "LAKE_MAINTENANCE_OPERATION_GC_MAX_MS",
        operation_gc_max_ms,
        u64::try_from(defaults.operation_gc_max_duration().as_millis())
            .expect("default operation GC duration fits u64"),
    )?;
    MaintenanceLimits::try_new(
        Duration::from_secs(interval_secs),
        table_page_size,
        operation_gc_max_pages,
        Duration::from_millis(operation_gc_max_ms),
    )
    .context("invalid Metasrv maintenance limits")
}

fn append_limits_from_values(
    max_concurrent: Option<&str>,
    queue_ms: Option<&str>,
    max_stream_bytes: Option<&str>,
    max_buffered_bytes: Option<&str>,
) -> anyhow::Result<AppendLimits> {
    let defaults = AppendLimits::default();
    let max_concurrent = parse_or(
        "LAKE_APPEND_MAX_CONCURRENT",
        max_concurrent,
        defaults.max_concurrent(),
    )?;
    let queue_ms = parse_or(
        "LAKE_APPEND_QUEUE_TIMEOUT_MS",
        queue_ms,
        u64::try_from(defaults.queue_wait().as_millis())
            .expect("default append queue duration fits u64"),
    )?;
    let max_stream_bytes = parse_or(
        "LAKE_APPEND_MAX_STREAM_BYTES",
        max_stream_bytes,
        defaults.max_stream_bytes(),
    )?;
    let max_buffered_bytes = parse_or(
        "LAKE_APPEND_MAX_BUFFERED_BYTES",
        max_buffered_bytes,
        defaults.max_buffered_bytes(),
    )?;
    AppendLimits::try_new(
        max_concurrent,
        Duration::from_millis(queue_ms),
        max_stream_bytes,
        max_buffered_bytes,
    )
    .context("invalid Metasrv append admission limits")
}

fn operation_policy_from_values(
    retention_secs: Option<&str>,
    page_size: Option<&str>,
) -> anyhow::Result<(Duration, usize)> {
    let retention_secs = parse_or(
        "LAKE_APPEND_OPERATION_RETENTION_SECS",
        retention_secs,
        DEFAULT_APPEND_OPERATION_RETENTION.as_secs(),
    )?;
    let page_size = parse_or(
        "LAKE_APPEND_OPERATION_GC_PAGE_SIZE",
        page_size,
        DEFAULT_OPERATION_GC_PAGE_SIZE,
    )?;
    anyhow::ensure!(
        retention_secs > 0,
        "LAKE_APPEND_OPERATION_RETENTION_SECS must be greater than zero"
    );
    anyhow::ensure!(
        (1..=MAX_OPERATION_GC_PAGE_SIZE).contains(&page_size),
        "LAKE_APPEND_OPERATION_GC_PAGE_SIZE must be within 1..={MAX_OPERATION_GC_PAGE_SIZE}"
    );
    Ok((Duration::from_secs(retention_secs), page_size))
}

pub(crate) fn shutdown_grace_from_env() -> anyhow::Result<Duration> {
    shutdown_grace_from_value(env_value("LAKE_SHUTDOWN_GRACE_MS")?.as_deref())
}

fn shutdown_grace_from_value(value: Option<&str>) -> anyhow::Result<Duration> {
    let millis = parse_or(
        "LAKE_SHUTDOWN_GRACE_MS",
        value,
        u64::try_from(DEFAULT_SHUTDOWN_GRACE.as_millis()).expect("default grace fits u64"),
    )?;
    anyhow::ensure!(
        millis > 0,
        "LAKE_SHUTDOWN_GRACE_MS must be greater than zero"
    );
    Ok(Duration::from_millis(millis))
}

pub(crate) fn query_limits_from_env() -> anyhow::Result<QueryLimits> {
    let max_concurrent = env_value("LAKE_QUERY_MAX_CONCURRENT")?;
    let max_concurrent_per_tenant = env_value("LAKE_QUERY_MAX_CONCURRENT_PER_TENANT")?;
    let max_tracked_tenants = env_value("LAKE_QUERY_MAX_TRACKED_TENANTS")?;
    let queue_ms = env_value("LAKE_QUERY_QUEUE_TIMEOUT_MS")?;
    let execution_ms = env_value("LAKE_QUERY_EXECUTION_TIMEOUT_MS")?;
    let max_sql_bytes = env_value("LAKE_QUERY_MAX_SQL_BYTES")?;
    query_limits_from_values(
        max_concurrent.as_deref(),
        max_concurrent_per_tenant.as_deref(),
        max_tracked_tenants.as_deref(),
        queue_ms.as_deref(),
        execution_ms.as_deref(),
        max_sql_bytes.as_deref(),
    )
}

pub(crate) fn query_ticket_ttl_from_env() -> anyhow::Result<Duration> {
    query_ticket_ttl_from_value(env_value("LAKE_QUERY_TICKET_TTL_SECS")?.as_deref())
}

fn query_ticket_ttl_from_value(value: Option<&str>) -> anyhow::Result<Duration> {
    let seconds = parse_or(
        "LAKE_QUERY_TICKET_TTL_SECS",
        value,
        DEFAULT_QUERY_TICKET_TTL.as_secs(),
    )?;
    anyhow::ensure!(
        (1..=MAX_QUERY_TICKET_TTL_SECS).contains(&seconds),
        "LAKE_QUERY_TICKET_TTL_SECS must be within 1..={MAX_QUERY_TICKET_TTL_SECS}"
    );
    Ok(Duration::from_secs(seconds))
}

pub(crate) fn discovery_limits_from_env() -> anyhow::Result<DiscoveryLimits> {
    let max_rows = env_value("LAKE_QUERY_MAX_DISCOVERY_ROWS")?;
    let batch_rows = env_value("LAKE_QUERY_DISCOVERY_BATCH_ROWS")?;
    discovery_limits_from_values(max_rows.as_deref(), batch_rows.as_deref())
}

pub(crate) fn query_resources_from_env() -> anyhow::Result<QueryResources> {
    let memory_bytes = env_value("LAKE_QUERY_MEMORY_BYTES")?;
    let spill_bytes = env_value("LAKE_QUERY_SPILL_BYTES")?;
    let spill_root = env_value("LAKE_QUERY_SPILL_DIR")?;
    query_resources_from_values(
        memory_bytes.as_deref(),
        spill_bytes.as_deref(),
        spill_root.as_deref(),
    )
}

pub(crate) fn async_scheduler_limits_from_env() -> anyhow::Result<(usize, usize, Duration)> {
    let workers = env_value("LAKE_ASYNC_WORKER_CONCURRENCY")?;
    let workers_per_tenant = env_value("LAKE_ASYNC_WORKER_CONCURRENCY_PER_TENANT")?;
    let execution_ms = env_value("LAKE_ASYNC_EXECUTION_TIMEOUT_MS")?;
    async_scheduler_limits_from_values(
        workers.as_deref(),
        workers_per_tenant.as_deref(),
        execution_ms.as_deref(),
    )
}

fn async_scheduler_limits_from_values(
    workers: Option<&str>,
    workers_per_tenant: Option<&str>,
    execution_ms: Option<&str>,
) -> anyhow::Result<(usize, usize, Duration)> {
    let workers = parse_or("LAKE_ASYNC_WORKER_CONCURRENCY", workers, 4_usize)?;
    let workers_per_tenant = parse_or(
        "LAKE_ASYNC_WORKER_CONCURRENCY_PER_TENANT",
        workers_per_tenant,
        1_usize,
    )?;
    let execution_ms = parse_or(
        "LAKE_ASYNC_EXECUTION_TIMEOUT_MS",
        execution_ms,
        30_u64 * 60 * 1_000,
    )?;
    if !(1..=64).contains(&workers)
        || workers_per_tenant == 0
        || workers_per_tenant > workers
        || execution_ms == 0
        || execution_ms > 24 * 60 * 60 * 1_000
    {
        anyhow::bail!("invalid async scheduler limits");
    }
    Ok((
        workers,
        workers_per_tenant,
        Duration::from_millis(execution_ms),
    ))
}

fn query_resources_from_values(
    memory_bytes: Option<&str>,
    spill_bytes: Option<&str>,
    spill_root: Option<&str>,
) -> anyhow::Result<QueryResources> {
    let defaults = QueryResources::default();
    let memory_bytes = parse_or(
        "LAKE_QUERY_MEMORY_BYTES",
        memory_bytes,
        defaults.memory_bytes(),
    )?;
    let spill_bytes = parse_or(
        "LAKE_QUERY_SPILL_BYTES",
        spill_bytes,
        defaults.spill_bytes(),
    )?;
    let spill_root = spill_root
        .map(PathBuf::from)
        .unwrap_or_else(|| defaults.spill_root().to_path_buf());
    QueryResources::try_new(memory_bytes, spill_bytes, spill_root)
        .context("invalid Query execution resources")
}

fn query_limits_from_values(
    max_concurrent: Option<&str>,
    max_concurrent_per_tenant: Option<&str>,
    max_tracked_tenants: Option<&str>,
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
    let max_concurrent_per_tenant = parse_or(
        "LAKE_QUERY_MAX_CONCURRENT_PER_TENANT",
        max_concurrent_per_tenant,
        defaults.max_concurrent_per_tenant().min(max_concurrent),
    )?;
    let max_tracked_tenants = parse_or(
        "LAKE_QUERY_MAX_TRACKED_TENANTS",
        max_tracked_tenants,
        defaults.max_tracked_tenants(),
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
    .and_then(|limits| {
        limits.try_with_tenant_limits(max_concurrent_per_tenant, max_tracked_tenants)
    })
    .context("invalid Query admission limits")
}

fn discovery_limits_from_values(
    max_rows: Option<&str>,
    batch_rows: Option<&str>,
) -> anyhow::Result<DiscoveryLimits> {
    let defaults = DiscoveryLimits::default();
    let max_rows = parse_or(
        "LAKE_QUERY_MAX_DISCOVERY_ROWS",
        max_rows,
        defaults.max_rows(),
    )?;
    let batch_rows = parse_or(
        "LAKE_QUERY_DISCOVERY_BATCH_ROWS",
        batch_rows,
        defaults.batch_rows(),
    )?;
    DiscoveryLimits::try_new(max_rows, batch_rows).context("invalid Query discovery limits")
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

    use super::{
        append_limits_from_values, async_scheduler_limits_from_values,
        discovery_limits_from_values, lance_maintenance_policy_from_value,
        maintenance_limits_from_values, operation_policy_from_values, query_limits_from_values,
        query_resources_from_values, query_ticket_ttl_from_value, shutdown_grace_from_value,
    };

    #[test]
    fn lance_retention_values_are_validated_before_storage_open() {
        assert_eq!(
            lance_maintenance_policy_from_value(None)
                .unwrap()
                .retained_versions(),
            10
        );
        assert_eq!(
            lance_maintenance_policy_from_value(Some("37"))
                .unwrap()
                .retained_versions(),
            37
        );
        for invalid in ["0", "10001", "18446744073709551616", "many"] {
            assert!(
                lance_maintenance_policy_from_value(Some(invalid)).is_err(),
                "accepted invalid retention {invalid}"
            );
        }
    }

    #[test]
    fn append_operation_policy_values_are_validated_before_serving() {
        let (retention, page_size) =
            operation_policy_from_values(Some("86400"), Some("64")).expect("valid policy");
        assert_eq!(retention, Duration::from_hours(24));
        assert_eq!(page_size, 64);
        assert!(operation_policy_from_values(Some("0"), None).is_err());
        assert!(operation_policy_from_values(None, Some("0")).is_err());
        assert!(operation_policy_from_values(Some("forever"), None).is_err());
    }

    #[test]
    fn maintenance_limit_values_are_validated_before_serving() {
        assert!(maintenance_limits_from_values(Some("0"), None, None, None).is_err());
        assert!(maintenance_limits_from_values(Some("often"), None, None, None).is_err());
        assert!(maintenance_limits_from_values(None, Some("0"), None, None).is_err());
        assert!(maintenance_limits_from_values(None, Some("10001"), None, None).is_err());
        assert!(maintenance_limits_from_values(None, None, Some("0"), None).is_err());
        assert!(maintenance_limits_from_values(None, None, Some("10001"), None).is_err());
        assert!(maintenance_limits_from_values(None, None, Some("many"), None).is_err());
        assert!(maintenance_limits_from_values(None, None, None, Some("0")).is_err());
        assert!(maintenance_limits_from_values(None, None, None, Some("60001")).is_err());
        assert!(maintenance_limits_from_values(None, None, None, Some("long")).is_err());

        let defaults =
            maintenance_limits_from_values(None, None, None, None).expect("default limits");
        assert_eq!(defaults.operation_gc_max_pages(), 16);
        assert_eq!(
            defaults.operation_gc_max_duration(),
            Duration::from_secs(10)
        );
        let limits =
            maintenance_limits_from_values(Some("15"), Some("512"), Some("32"), Some("2500"))
                .expect("valid limits");
        assert_eq!(limits.interval(), Duration::from_secs(15));
        assert_eq!(limits.table_page_size(), 512);
        assert_eq!(limits.operation_gc_max_pages(), 32);
        assert_eq!(
            limits.operation_gc_max_duration(),
            Duration::from_millis(2500)
        );
    }

    #[test]
    fn append_limit_values_are_validated_before_serving() {
        assert!(append_limits_from_values(Some("0"), None, None, None).is_err());
        assert!(append_limits_from_values(Some("many"), None, None, None).is_err());
        assert!(append_limits_from_values(None, Some("0"), None, None).is_err());
        assert!(append_limits_from_values(None, None, Some("0"), None).is_err());
        assert!(append_limits_from_values(None, None, Some("65"), Some("64")).is_err());
        let semaphore_overflow = (u64::from(u32::MAX) + 1).to_string();
        assert!(append_limits_from_values(None, None, Some(&semaphore_overflow), None).is_err());

        let limits =
            append_limits_from_values(Some("3"), Some("250"), Some("1048576"), Some("4194304"))
                .expect("valid append limits");
        assert_eq!(limits.max_concurrent(), 3);
        assert_eq!(limits.queue_wait(), Duration::from_millis(250));
        assert_eq!(limits.max_stream_bytes(), 1024 * 1024);
        assert_eq!(limits.max_buffered_bytes(), 4 * 1024 * 1024);
    }

    #[test]
    fn query_limit_values_are_validated_before_serving() {
        assert!(query_limits_from_values(Some("0"), None, None, None, None, None).is_err());
        assert!(query_limits_from_values(Some("many"), None, None, None, None, None).is_err());
        assert!(query_limits_from_values(None, None, None, Some("0"), None, None).is_err());

        let limits = query_limits_from_values(
            Some("7"),
            None,
            None,
            Some("250"),
            Some("5000"),
            Some("4096"),
        )
        .expect("valid limits");
        assert_eq!(limits.max_concurrent(), 7);
        assert_eq!(limits.queue_wait(), Duration::from_millis(250));
        assert_eq!(limits.execution_time(), Duration::from_secs(5));
        assert_eq!(limits.max_sql_bytes(), 4096);
    }

    #[test]
    fn query_tenant_limit_values_are_validated_before_serving() {
        assert!(query_limits_from_values(Some("8"), Some("0"), None, None, None, None).is_err());
        assert!(query_limits_from_values(Some("8"), Some("9"), None, None, None, None).is_err());
        assert!(
            query_limits_from_values(Some("8"), Some("2"), Some("0"), None, None, None).is_err()
        );
        assert!(
            query_limits_from_values(Some("8"), Some("2"), Some("65537"), None, None, None,)
                .is_err()
        );
        assert!(query_limits_from_values(Some("8"), Some("many"), None, None, None, None).is_err());

        let limits = query_limits_from_values(
            Some("8"),
            Some("2"),
            Some("128"),
            Some("250"),
            Some("5000"),
            Some("4096"),
        )
        .expect("valid tenant limits");
        assert_eq!(limits.max_concurrent(), 8);
        assert_eq!(limits.max_concurrent_per_tenant(), 2);
        assert_eq!(limits.max_tracked_tenants(), 128);
    }

    #[test]
    fn query_ticket_ttl_is_bounded_before_serving() {
        assert_eq!(
            query_ticket_ttl_from_value(None).unwrap(),
            Duration::from_mins(5)
        );
        assert_eq!(
            query_ticket_ttl_from_value(Some("900")).unwrap(),
            Duration::from_mins(15)
        );
        for invalid in ["0", "3601", "forever"] {
            assert!(query_ticket_ttl_from_value(Some(invalid)).is_err());
        }
    }

    #[test]
    fn async_scheduler_limit_values_are_validated_before_serving() {
        for values in [
            (Some("0"), None, None),
            (Some("65"), None, None),
            (Some("2"), Some("0"), None),
            (Some("2"), Some("3"), None),
            (None, None, Some("0")),
            (None, None, Some("86400001")),
            (Some("many"), None, None),
        ] {
            assert!(async_scheduler_limits_from_values(values.0, values.1, values.2).is_err());
        }
        assert_eq!(
            async_scheduler_limits_from_values(Some("8"), Some("2"), Some("5000")).unwrap(),
            (8, 2, Duration::from_secs(5))
        );
    }

    #[test]
    fn discovery_limit_values_are_validated_before_serving() {
        assert!(discovery_limits_from_values(Some("0"), None).is_err());
        assert!(discovery_limits_from_values(Some("many"), None).is_err());
        assert!(discovery_limits_from_values(None, Some("0")).is_err());
        assert!(discovery_limits_from_values(Some("2"), Some("3")).is_err());

        let limits = discovery_limits_from_values(Some("4096"), Some("128"))
            .expect("valid discovery limits");
        assert_eq!(limits.max_rows(), 4096);
        assert_eq!(limits.batch_rows(), 128);
    }

    #[test]
    fn query_resource_values_are_validated_before_serving() {
        let resources = query_resources_from_values(
            Some("268435456"),
            Some("1073741824"),
            Some("/var/tmp/lake-query-test"),
        )
        .expect("valid Query resources");
        assert_eq!(resources.memory_bytes(), 256 * 1024 * 1024);
        assert_eq!(resources.spill_bytes(), 1024 * 1024 * 1024);
        assert_eq!(
            resources.spill_root(),
            std::path::Path::new("/var/tmp/lake-query-test")
        );

        assert!(query_resources_from_values(Some("0"), None, None).is_err());
        assert!(query_resources_from_values(None, Some("0"), None).is_err());
        assert!(query_resources_from_values(Some("lots"), None, None).is_err());
        assert!(query_resources_from_values(None, None, Some("")).is_err());
    }

    #[test]
    fn shutdown_grace_is_positive_and_defaults_to_thirty_seconds() {
        assert_eq!(
            shutdown_grace_from_value(None).unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            shutdown_grace_from_value(Some("1250")).unwrap(),
            Duration::from_millis(1250)
        );
        assert!(shutdown_grace_from_value(Some("0")).is_err());
        assert!(shutdown_grace_from_value(Some("later")).is_err());
    }
}
