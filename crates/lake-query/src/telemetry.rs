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

//! Low-cardinality Query metrics. Arguments are deliberately static enums;
//! user SQL, tenant, namespace, table, and object identities never become
//! labels.

pub(crate) fn describe() {
    lake_iceberg::describe_metrics();
    metrics::describe_counter!(
        "lake_query_admission_total",
        "Query admission decisions by bounded outcome"
    );
    metrics::describe_gauge!(
        "lake_query_inflight_requests",
        "Requests currently holding Query admission"
    );
    metrics::describe_counter!(
        "lake_query_rejections_total",
        "Query requests rejected before admission by bounded reason"
    );
    metrics::describe_counter!(
        "lake_query_catalog_refresh_total",
        "Catalog refresh attempts by phase and bounded outcome"
    );
    metrics::describe_gauge!(
        "lake_query_ready",
        "Whether Query is ready to receive Flight traffic"
    );
    metrics::describe_counter!(
        "lake_query_async_scheduler_total",
        "Durable async scheduler transitions by bounded outcome"
    );
    metrics::describe_gauge!(
        "lake_query_async_active_workers",
        "Durable async jobs currently owning process-local worker capacity"
    );
    metrics::describe_counter!(
        "lake_query_async_quota_rejections_total",
        "Durable async submission quota rejections by bounded reason"
    );
    metrics::describe_counter!(
        "lake_query_async_cluster_execution_total",
        "Durable async cluster execution lease outcomes by bounded outcome"
    );
}

pub(crate) fn admission(outcome: &'static str) {
    metrics::counter!("lake_query_admission_total", "outcome" => outcome).increment(1);
}

pub(crate) fn inflight_increment() {
    metrics::gauge!("lake_query_inflight_requests").increment(1.0);
}

pub(crate) fn inflight_decrement() {
    metrics::gauge!("lake_query_inflight_requests").decrement(1.0);
}

pub(crate) fn rejection(reason: &'static str) {
    metrics::counter!("lake_query_rejections_total", "reason" => reason).increment(1);
}

pub(crate) fn catalog_refresh(phase: &'static str, outcome: &'static str) {
    metrics::counter!(
        "lake_query_catalog_refresh_total",
        "phase" => phase,
        "outcome" => outcome
    )
    .increment(1);
}

pub(crate) fn ready(ready: bool) { metrics::gauge!("lake_query_ready").set(f64::from(ready)); }

pub(crate) fn async_scheduler(outcome: &'static str) {
    metrics::counter!("lake_query_async_scheduler_total", "outcome" => outcome).increment(1);
}

pub(crate) fn async_active_increment() {
    metrics::gauge!("lake_query_async_active_workers").increment(1.0);
}

pub(crate) fn async_active_decrement() {
    metrics::gauge!("lake_query_async_active_workers").decrement(1.0);
}

pub(crate) fn async_quota_rejection(reason: &'static str) {
    metrics::counter!("lake_query_async_quota_rejections_total", "reason" => reason).increment(1);
}

pub(crate) fn async_cluster_execution(outcome: &'static str) {
    metrics::counter!("lake_query_async_cluster_execution_total", "outcome" => outcome)
        .increment(1);
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use lake_common::{Principal, PrincipalId, PrincipalRole, TenantId};
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_flight::ClientSecurity;
    use lake_meta::{MetaError, MetaStore, MetaStoreRef};
    use metrics_exporter_prometheus::PrometheusBuilder;

    use crate::{
        QueryEngine, QueryLimits, QueryServerConfig, flight::QueryAdmission,
        run_catalog_refresh_loop, serve_with_config_and_shutdown,
    };

    #[derive(Default)]
    struct EmptyMeta;

    #[async_trait]
    impl MetaStore for EmptyMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> { Ok(None) }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            Ok(true)
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> { Ok(true) }
    }

    struct FailingScanMeta;

    #[async_trait]
    impl MetaStore for FailingScanMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> { Ok(None) }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            Ok(true)
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            Err(MetaError::Conflict {
                table: "metrics-test".to_owned(),
            })
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> { Ok(true) }
    }

    fn engine(meta: MetaStoreRef) -> Arc<QueryEngine> {
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        Arc::new(QueryEngine::new(meta, storage))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_metrics_cover_admission_and_catalog_refresh() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let readiness_handle = handle.clone();
        let server_addr = addr.to_string();
        let server = serve_with_config_and_shutdown(
            engine(Arc::new(EmptyMeta)),
            &server_addr,
            QueryServerConfig::new().with_shutdown_grace(Duration::from_secs(1)),
            async move {
                let _ = shutdown_rx.await;
            },
        );
        let driver = async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if ClientSecurity::new()
                        .connect(format!("http://{addr}"))
                        .await
                        .is_ok()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("Query starts");
            assert!(readiness_handle.render().contains("lake_query_ready 1"));
            shutdown_tx.send(()).unwrap();
        };
        let (server_result, ()) = tokio::join!(server, driver);
        server_result.unwrap();

        let cancellation = tokio_util::sync::CancellationToken::new();
        let loop_cancellation = cancellation.clone();
        let refresh =
            run_catalog_refresh_loop(engine(Arc::new(FailingScanMeta)), loop_cancellation);
        let refresh_handle = handle.clone();
        let drive_refresh = async {
            tokio::time::timeout(Duration::from_secs(7), async {
                loop {
                    if refresh_handle.render().contains(
                        "lake_query_catalog_refresh_total{phase=\"background\",outcome=\"error\"} \
                         1",
                    ) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("background refresh failure is recorded");
            cancellation.cancel();
        };
        tokio::join!(refresh, drive_refresh);

        let principal = |subject: &str, tenant: &str| {
            Principal::try_new(
                PrincipalId::try_new(subject).expect("valid subject"),
                TenantId::try_new(tenant).expect("valid tenant"),
                PrincipalRole::User,
                [tenant],
            )
            .expect("valid principal")
        };
        let alpha = principal("alpha-reader", "alpha");
        let beta = principal("beta-reader", "beta");
        let limits = QueryLimits::try_new(1, Duration::from_millis(1), Duration::from_secs(1), 4)
            .expect("query limits")
            .try_with_tenant_limits(1, 2)
            .expect("tenant query limits");
        let admission = QueryAdmission::new(limits);
        let permit = admission.acquire(&alpha).await.expect("alpha admitted");
        let active = handle.render();
        assert!(active.contains("lake_query_inflight_requests 1"));
        assert!(admission.acquire(&alpha).await.is_err());
        assert!(admission.acquire(&beta).await.is_err());
        let tracker_limits = limits
            .try_with_tenant_limits(1, 1)
            .expect("tracked tenant limit");
        let tracker_admission = QueryAdmission::new(tracker_limits);
        let tracker_permit = tracker_admission
            .acquire(&alpha)
            .await
            .expect("alpha tracker admitted");
        assert!(tracker_admission.acquire(&beta).await.is_err());
        assert!(admission.validate_sql_size(b"12345").is_err());
        drop((permit, tracker_permit));

        let rendered = handle.render();
        for expected in [
            "lake_query_admission_total{outcome=\"admitted\"} 2",
            "lake_query_admission_total{outcome=\"saturated\"} 1",
            "lake_query_admission_total{outcome=\"scope_saturated\"} 1",
            "lake_query_admission_total{outcome=\"scope_tracker_saturated\"} 1",
            "lake_query_inflight_requests 0",
            "lake_query_rejections_total{reason=\"sql_too_large\"} 1",
            "lake_query_catalog_refresh_total{phase=\"initial\",outcome=\"success\"} 1",
            "lake_query_catalog_refresh_total{phase=\"background\",outcome=\"error\"} 1",
            "lake_query_ready 0",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected}:\n{rendered}"
            );
        }
        for forbidden in [
            "SELECT",
            "tenant",
            "namespace",
            "table",
            "operation_id",
            "uri",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "forbidden label/value {forbidden}"
            );
        }
    }

    #[test]
    fn async_scheduler_metrics_are_bounded_and_identity_free() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        super::describe();
        for outcome in [
            "admitted",
            "scope_saturated",
            "completed",
            "failed",
            "deadline_exceeded",
            "invalid_state",
        ] {
            super::async_scheduler(outcome);
        }
        super::async_active_increment();
        super::async_active_decrement();

        let rendered = handle.render();
        for outcome in [
            "admitted",
            "scope_saturated",
            "completed",
            "failed",
            "deadline_exceeded",
            "invalid_state",
        ] {
            assert!(rendered.contains(&format!(
                "lake_query_async_scheduler_total{{outcome=\"{outcome}\"}} 1"
            )));
        }
        assert!(rendered.contains("lake_query_async_active_workers 0"));
        super::async_quota_rejection("outstanding_jobs");
        let rendered = handle.render();
        assert!(
            rendered
                .contains("lake_query_async_quota_rejections_total{reason=\"outstanding_jobs\"} 1")
        );
        for forbidden in [
            "secret-tenant",
            "tenant-a",
            "query-id",
            "principal",
            "tenant_id",
        ] {
            assert!(!rendered.contains(forbidden));
        }
    }

    #[test]
    fn async_global_execution_metrics_are_bounded_and_identity_free() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        super::describe();
        for outcome in ["reserved", "saturated", "released", "stale"] {
            super::async_cluster_execution(outcome);
        }

        let rendered = handle.render();
        for outcome in ["reserved", "saturated", "released", "stale"] {
            assert!(rendered.contains(&format!(
                "lake_query_async_cluster_execution_total{{outcome=\"{outcome}\"}} 1"
            )));
        }
        for forbidden in [
            "secret-tenant",
            "tenant-a",
            "query-id",
            "worker-id",
            "opaque-token",
        ] {
            assert!(!rendered.contains(forbidden));
        }
    }
}
