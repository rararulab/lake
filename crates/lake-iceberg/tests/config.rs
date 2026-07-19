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

use lake_iceberg::{IcebergCatalogConfig, IcebergError, IcebergOAuthOptions, IcebergRestAuth};

#[test]
fn iceberg_configuration_rejects_invalid_or_duplicate_namespaces() {
    assert!(IcebergCatalogConfig::try_new("not a URI", "s3://warehouse", ["analytics"],).is_err());
    assert!(IcebergCatalogConfig::try_new("https://catalog.example", "", ["analytics"],).is_err());
    assert!(
        IcebergCatalogConfig::try_new(
            "https://catalog.example",
            "s3://warehouse",
            ["analytics", "analytics"],
        )
        .is_err()
    );
    assert!(
        IcebergCatalogConfig::try_new("https://catalog.example", "s3://warehouse", ["analytics"],)
            .is_ok()
    );
}

#[test]
fn iceberg_catalog_config_debug_redacts_warehouse() {
    const WAREHOUSE: &str = "abfss://tenant-secret@lake-account.dfs.core.windows.net/warehouse";
    let config = IcebergCatalogConfig::try_new("https://catalog.example", WAREHOUSE, ["analytics"])
        .expect("construct Iceberg configuration");

    let debug = format!("{config:?}");
    assert!(
        !debug.contains(WAREHOUSE),
        "warehouse identifier must not appear in diagnostics"
    );
    assert!(
        !debug.contains("tenant-secret"),
        "credential-looking warehouse component must not appear in diagnostics"
    );
    assert!(
        debug.contains("warehouse: \"configured\""),
        "diagnostics must retain the opaque configured warehouse marker"
    );
    assert_eq!(config.warehouse(), WAREHOUSE);
}

#[test]
fn external_rest_urls_require_tls_or_numeric_loopback() {
    for endpoint in [
        "https://catalog.example",
        "http://127.0.0.1:8181",
        "http://127.0.0.2:8181",
        "http://[::1]:8181",
    ] {
        assert!(
            IcebergCatalogConfig::try_new(endpoint, "s3://warehouse", ["analytics"]).is_ok(),
            "accepted endpoint must validate: {endpoint}"
        );
    }

    for endpoint in ["http://catalog.example", "http://localhost:8181"] {
        assert!(matches!(
            IcebergCatalogConfig::try_new(endpoint, "s3://warehouse", ["analytics"]),
            Err(IcebergError::InvalidEndpoint)
        ));
    }

    for endpoint in [
        "https://identity.example/oauth/token",
        "http://127.0.0.1:8181/oauth/token",
        "http://[::1]:8181/oauth/token",
    ] {
        assert!(
            IcebergRestAuth::oauth_client_credentials(
                "lake-client:lake-secret",
                IcebergOAuthOptions::builder()
                    .oauth2_server_uri(endpoint)
                    .build(),
            )
            .is_ok(),
            "accepted OAuth endpoint must validate: {endpoint}"
        );
    }

    for endpoint in [
        "http://identity.example/oauth/token",
        "http://localhost:8181/oauth/token",
    ] {
        assert!(matches!(
            IcebergRestAuth::oauth_client_credentials(
                "lake-client:lake-secret",
                IcebergOAuthOptions::builder()
                    .oauth2_server_uri(endpoint)
                    .build(),
            ),
            Err(IcebergError::InvalidRestAuth)
        ));
    }
}
