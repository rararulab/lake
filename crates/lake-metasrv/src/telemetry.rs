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

//! Low-cardinality metadata-authority metrics. Labels are finite state-machine
//! outcomes, never tenant, namespace, table, operation, or object identities.

pub(crate) fn describe() {
    metrics::describe_counter!(
        "lake_metasrv_append_admission_total",
        "FILE append admission decisions by bounded outcome"
    );
    metrics::describe_gauge!(
        "lake_metasrv_inflight_appends",
        "Appends currently holding process admission"
    );
    metrics::describe_gauge!(
        "lake_metasrv_reserved_append_bytes",
        "Worst-case append bytes currently reserved"
    );
    metrics::describe_counter!(
        "lake_metasrv_campaign_total",
        "Leadership campaign rounds by bounded outcome"
    );
    metrics::describe_gauge!(
        "lake_metasrv_write_ready",
        "Whether this node can accept or forward metadata writes"
    );
    metrics::describe_counter!(
        "lake_metasrv_maintenance_pages_total",
        "Completed bounded maintenance stages"
    );
    metrics::describe_counter!(
        "lake_metasrv_maintenance_items_total",
        "Maintenance items by bounded stage and outcome"
    );
}

pub(crate) fn append_admission(outcome: &'static str) {
    metrics::counter!("lake_metasrv_append_admission_total", "outcome" => outcome).increment(1);
}

pub(crate) fn append_acquired(bytes: usize) {
    metrics::gauge!("lake_metasrv_inflight_appends").increment(1.0);
    metrics::gauge!("lake_metasrv_reserved_append_bytes").increment(bytes as f64);
}

pub(crate) fn append_released(bytes: usize) {
    metrics::gauge!("lake_metasrv_inflight_appends").decrement(1.0);
    metrics::gauge!("lake_metasrv_reserved_append_bytes").decrement(bytes as f64);
}

pub(crate) fn campaign(outcome: &'static str) {
    metrics::counter!("lake_metasrv_campaign_total", "outcome" => outcome).increment(1);
}

pub(crate) fn write_ready(ready: bool) {
    metrics::gauge!("lake_metasrv_write_ready").set(f64::from(ready));
}

pub(crate) fn maintenance_page(stage: &'static str) {
    metrics::counter!("lake_metasrv_maintenance_pages_total", "stage" => stage).increment(1);
}

pub(crate) fn maintenance_items(stage: &'static str, outcome: &'static str, count: usize) {
    metrics::counter!(
        "lake_metasrv_maintenance_items_total",
        "stage" => stage,
        "outcome" => outcome
    )
    .increment(count as u64);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;
    use crate::{AppendLimits, control::AppendAdmission};

    #[tokio::test(flavor = "current_thread")]
    async fn metasrv_metrics_cover_append_leadership_and_maintenance() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        describe();

        let admission = AppendAdmission::new(
            AppendLimits::try_new(1, Duration::from_millis(1), 1024, 1024).unwrap(),
        );
        let permit = admission.acquire().await.unwrap();
        let active = handle.render();
        assert!(active.contains("lake_metasrv_inflight_appends 1"));
        assert!(active.contains("lake_metasrv_reserved_append_bytes 1024"));
        assert!(admission.acquire().await.is_err());
        drop(permit);

        campaign("leader");
        campaign("timeout");
        write_ready(true);
        write_ready(false);
        maintenance_page("tables");
        maintenance_items("tables", "maintained", 2);
        maintenance_items("tables", "failed", 1);

        let rendered = handle.render();
        for expected in [
            "lake_metasrv_append_admission_total{outcome=\"admitted\"} 1",
            "lake_metasrv_append_admission_total{outcome=\"saturated\"} 1",
            "lake_metasrv_inflight_appends 0",
            "lake_metasrv_reserved_append_bytes 0",
            "lake_metasrv_campaign_total{outcome=\"leader\"} 1",
            "lake_metasrv_campaign_total{outcome=\"timeout\"} 1",
            "lake_metasrv_write_ready 0",
            "lake_metasrv_maintenance_pages_total{stage=\"tables\"} 1",
            "lake_metasrv_maintenance_items_total{stage=\"tables\",outcome=\"maintained\"} 2",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected}:\n{rendered}"
            );
        }
        for forbidden in [
            "tenant",
            "namespace",
            "table=",
            "operation_id",
            "uri",
            "s3://",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "forbidden label/value {forbidden}"
            );
        }
    }
}
