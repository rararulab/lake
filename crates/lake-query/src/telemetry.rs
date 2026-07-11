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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;
    use crate::{QueryLimits, flight::QueryAdmission};

    #[tokio::test(flavor = "current_thread")]
    async fn query_metrics_cover_admission_and_catalog_refresh() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        describe();
        ready(false);
        catalog_refresh("initial", "success");
        catalog_refresh("background", "error");

        let limits =
            QueryLimits::try_new(1, Duration::from_millis(1), Duration::from_secs(1), 4).unwrap();
        let admission = QueryAdmission::new(limits);
        let permit = admission.acquire().await.unwrap();
        let active = handle.render();
        assert!(active.contains("lake_query_inflight_requests 1"));
        assert!(admission.acquire().await.is_err());
        assert!(admission.validate_sql_size(b"12345").is_err());
        drop(permit);
        ready(true);

        let rendered = handle.render();
        for expected in [
            "lake_query_admission_total{outcome=\"admitted\"} 1",
            "lake_query_admission_total{outcome=\"saturated\"} 1",
            "lake_query_inflight_requests 0",
            "lake_query_rejections_total{reason=\"sql_too_large\"} 1",
            "lake_query_catalog_refresh_total{phase=\"initial\",outcome=\"success\"} 1",
            "lake_query_catalog_refresh_total{phase=\"background\",outcome=\"error\"} 1",
            "lake_query_ready 1",
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
}
