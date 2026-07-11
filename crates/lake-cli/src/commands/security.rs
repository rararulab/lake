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

//! Process configuration for Flight TLS and opaque service credentials.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use lake_flight::{ClientSecurity, ServerSecurity};

pub(crate) fn server_security_from_files(
    token: Option<&Path>,
    certificate: Option<&Path>,
    private_key: Option<&Path>,
) -> anyhow::Result<ServerSecurity> {
    let mut security = match token {
        Some(path) => ServerSecurity::with_bearer_token(read_token(path)?)?,
        None => ServerSecurity::insecure(),
    };
    match (certificate, private_key) {
        (Some(certificate), Some(private_key)) => {
            let certificate = std::fs::read(certificate).with_context(|| {
                format!("cannot read TLS certificate {}", certificate.display())
            })?;
            let private_key = std::fs::read(private_key).with_context(|| {
                format!("cannot read TLS private key {}", private_key.display())
            })?;
            security = security.with_tls_identity_pem(&certificate, &private_key);
        }
        (None, None) => {}
        (..) => anyhow::bail!("LAKE_TLS_CERT_FILE and LAKE_TLS_KEY_FILE must be set together"),
    }
    Ok(security)
}

pub(crate) fn client_security_from_files(
    token: Option<&Path>,
    ca_certificate: Option<&Path>,
    server_name: Option<&str>,
) -> anyhow::Result<ClientSecurity> {
    let mut security = ClientSecurity::new();
    if let Some(path) = token {
        security = security.with_bearer_token(read_token(path)?)?;
    }
    if let Some(path) = ca_certificate {
        let certificate = std::fs::read(path)
            .with_context(|| format!("cannot read Flight CA certificate {}", path.display()))?;
        security = security.with_ca_certificate_pem(certificate);
    }
    if let Some(server_name) = server_name {
        security = security.with_server_name(server_name);
    }
    Ok(security)
}

pub(crate) fn server_security_from_env() -> anyhow::Result<ServerSecurity> {
    let token = env_path("LAKE_AUTH_TOKEN_FILE");
    let certificate = env_path("LAKE_TLS_CERT_FILE");
    let private_key = env_path("LAKE_TLS_KEY_FILE");
    server_security_from_files(
        token.as_deref(),
        certificate.as_deref(),
        private_key.as_deref(),
    )
}

pub(crate) fn metadata_client_security_from_env() -> anyhow::Result<ClientSecurity> {
    client_security_from_env_prefix("LAKE_METADATA")
}

pub(crate) fn peer_client_security_from_env() -> anyhow::Result<ClientSecurity> {
    client_security_from_env_prefix("LAKE_PEER")
}

pub(crate) fn allow_insecure_from_env() -> anyhow::Result<bool> {
    match std::env::var("LAKE_ALLOW_INSECURE") {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(error.into()),
        Ok(value) if value == "1" || value.eq_ignore_ascii_case("true") => Ok(true),
        Ok(value) if value == "0" || value.eq_ignore_ascii_case("false") => Ok(false),
        Ok(_) => anyhow::bail!("LAKE_ALLOW_INSECURE must be true/false or 1/0"),
    }
}

fn client_security_from_env_prefix(prefix: &str) -> anyhow::Result<ClientSecurity> {
    let token = env_path(&format!("{prefix}_AUTH_TOKEN_FILE"));
    let ca = env_path(&format!("{prefix}_CA_FILE"));
    let server_name = std::env::var(format!("{prefix}_SERVER_NAME")).ok();
    client_security_from_files(token.as_deref(), ca.as_deref(), server_name.as_deref())
}

fn env_path(name: &str) -> Option<PathBuf> { std::env::var_os(name).map(PathBuf::from) }

fn read_token(path: &Path) -> anyhow::Result<String> {
    let token = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read Flight credential {}", path.display()))?;
    Ok(token.trim_end_matches(['\r', '\n']).to_owned())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{client_security_from_files, server_security_from_files};

    #[test]
    fn security_files_require_complete_tls_and_redact_credentials() {
        let root = tempdir().expect("tempdir");
        let token = root.path().join("token");
        let cert = root.path().join("cert.pem");
        let key = root.path().join("key.pem");
        fs::write(&token, "deployment-secret\n").expect("token");
        fs::write(&cert, "certificate").expect("certificate");
        fs::write(&key, "private-key").expect("private key");

        assert!(
            server_security_from_files(Some(&token), Some(&cert), None).is_err(),
            "certificate without key must fail"
        );
        let server = server_security_from_files(Some(&token), Some(&cert), Some(&key))
            .expect("complete server security");
        assert!(!format!("{server:?}").contains("deployment-secret"));

        let client = client_security_from_files(Some(&token), Some(&cert), Some("meta.internal"))
            .expect("client security");
        assert_eq!(
            client.endpoint_for_authority("127.0.0.1:50052"),
            "https://127.0.0.1:50052"
        );
        assert!(!format!("{client:?}").contains("deployment-secret"));
    }
}
