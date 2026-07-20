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

use std::{fs, path::PathBuf};

fn root() -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..") }

fn read(path: &str) -> String {
    fs::read_to_string(root().join(path)).unwrap_or_else(|error| panic!("read {path}: {error}"))
}

fn assert_shared_shell(path: &str) {
    let source = read(path);
    for import in [
        "@/layouts/Layout.astro",
        "@/components/Header.astro",
        "@/components/Footer.astro",
    ] {
        assert!(source.contains(import), "{path} must import {import}");
    }
    assert!(
        source.contains("app-layout"),
        "{path} must use AstroPaper's narrow application shell"
    );
    assert!(
        !source.contains("site-shell") && !source.contains("content-shell"),
        "{path} must not use the legacy marketing shell"
    );
}

#[test]
fn astropaper_homepage_replaces_legacy_marketing_visual_contract() {
    let homepage = read("site/src/pages/index.astro");
    let header = read("site/src/components/Header.astro");
    let layout = read("site/src/layouts/Layout.astro");
    let styles = read("site/src/styles/global.css");
    let theme = read("site/src/styles/theme.css");

    for marker in [
        "data-layout=\"index\"",
        "app-layout",
        "@utility max-w-app",
        "@utility app-layout",
        "--font-google-sans-code",
        "id=\"nav-menu\"",
        "id=\"menu-btn\"",
    ] {
        assert!(
            homepage.contains(marker)
                || header.contains(marker)
                || layout.contains(marker)
                || styles.contains(marker)
                || theme.contains(marker),
            "AstroPaper visual contract is missing {marker}"
        );
    }

    for legacy in [
        "site-shell",
        "hero-grid",
        "layer-row",
        "Read path",
        "Architecture / 01",
        "Design targets",
        "10⁴",
        "10¹¹",
    ] {
        assert!(
            !homepage.contains(legacy) && !styles.contains(legacy),
            "legacy marketing marker remains: {legacy}"
        );
    }
}

#[test]
fn astropaper_theme_wraps_all_public_site_routes() {
    for path in [
        "site/src/pages/index.astro",
        "site/src/pages/docs/index.astro",
        "site/src/pages/search.astro",
        "site/src/pages/404.astro",
    ] {
        assert_shared_shell(path);
    }

    let docs_layout = read("site/src/layouts/DocsLayout.astro");
    assert!(docs_layout.contains("@/layouts/Layout.astro"));
    assert!(docs_layout.contains("@/components/Header.astro"));
    assert!(docs_layout.contains("@/components/Footer.astro"));
    assert!(docs_layout.contains("app-layout"));
    assert!(docs_layout.contains("app-prose"));
    assert!(!docs_layout.contains("site-shell"));
}
