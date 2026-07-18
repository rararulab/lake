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

use lake_iceberg::IcebergCatalogConfig;

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
