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
use lake_common::{Principal, PrincipalId, PrincipalRole, TenantId};
use lake_flight::{BearerPrincipalBinding, ClientSecurity, ServerSecurity};
use lake_query::QueryTicketKeyRing;
use serde::Deserialize;

const MAX_PRINCIPAL_FILE_BYTES: u64 = 1024 * 1024;
const MAX_TICKET_KEY_FILE_BYTES: u64 = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrincipalFile {
    bindings: Vec<PrincipalBindingFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrincipalBindingFile {
    token:        String,
    principal_id: String,
    tenant_id:    String,
    role:         PrincipalRoleFile,
    namespaces:   Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryTicketKeyFile {
    active:       String,
    verification: Vec<String>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PrincipalRoleFile {
    User,
    QueryService,
    MetadataPeer,
    Admin,
}

impl From<PrincipalRoleFile> for PrincipalRole {
    fn from(role: PrincipalRoleFile) -> Self {
        match role {
            PrincipalRoleFile::User => Self::User,
            PrincipalRoleFile::QueryService => Self::QueryService,
            PrincipalRoleFile::MetadataPeer => Self::MetadataPeer,
            PrincipalRoleFile::Admin => Self::Admin,
        }
    }
}

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

pub(crate) fn server_security_from_principal_file(path: &Path) -> anyhow::Result<ServerSecurity> {
    validate_protected_file(path)?;
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("cannot inspect principal map {}", path.display()))?;
    if metadata.len() > MAX_PRINCIPAL_FILE_BYTES {
        anyhow::bail!("principal map {} exceeds 1 MiB", path.display());
    }
    let wire = std::fs::read(path)
        .with_context(|| format!("cannot read principal map {}", path.display()))?;
    let file: PrincipalFile = serde_json::from_slice(&wire)
        .with_context(|| format!("invalid principal map {}", path.display()))?;
    let bindings = file
        .bindings
        .into_iter()
        .map(|binding| {
            let principal = Principal::try_new(
                PrincipalId::try_new(binding.principal_id)?,
                TenantId::try_new(binding.tenant_id)?,
                binding.role.into(),
                binding.namespaces,
            )?;
            BearerPrincipalBinding::new(binding.token, principal).map_err(anyhow::Error::from)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    ServerSecurity::with_bearer_principals(bindings).map_err(Into::into)
}

pub(crate) fn query_ticket_keys_from_file(path: &Path) -> anyhow::Result<QueryTicketKeyRing> {
    validate_protected_file(path)?;
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("cannot inspect Query ticket key file {}", path.display()))?;
    if metadata.len() > MAX_TICKET_KEY_FILE_BYTES {
        anyhow::bail!("Query ticket key file {} exceeds 64 KiB", path.display());
    }
    let wire = std::fs::read(path)
        .with_context(|| format!("cannot read Query ticket key file {}", path.display()))?;
    let file: QueryTicketKeyFile = serde_json::from_slice(&wire)
        .with_context(|| format!("invalid Query ticket key file {}", path.display()))?;
    QueryTicketKeyRing::try_new(
        file.active.as_bytes(),
        file.verification.iter().map(String::as_bytes),
    )
    .map_err(|_| anyhow::anyhow!("invalid Query ticket key configuration"))
}

pub(crate) fn query_ticket_keys_from_env() -> anyhow::Result<Option<QueryTicketKeyRing>> {
    env_path("LAKE_QUERY_TICKET_KEYS_FILE")
        .map(|path| query_ticket_keys_from_file(&path))
        .transpose()
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
    let principals = env_path("LAKE_AUTH_PRINCIPALS_FILE");
    let certificate = env_path("LAKE_TLS_CERT_FILE");
    let private_key = env_path("LAKE_TLS_KEY_FILE");
    if token.is_some() && principals.is_some() {
        anyhow::bail!("LAKE_AUTH_TOKEN_FILE and LAKE_AUTH_PRINCIPALS_FILE are mutually exclusive");
    }
    let mut security = match principals {
        Some(path) => server_security_from_principal_file(&path)?,
        None => match token {
            Some(path) => ServerSecurity::with_bearer_token(read_token(&path)?)?,
            None => ServerSecurity::insecure(),
        },
    };
    match (certificate, private_key) {
        (Some(certificate), Some(private_key)) => {
            let certificate = std::fs::read(&certificate).with_context(|| {
                format!("cannot read TLS certificate {}", certificate.display())
            })?;
            let private_key = std::fs::read(&private_key).with_context(|| {
                format!("cannot read TLS private key {}", private_key.display())
            })?;
            security = security.with_tls_identity_pem(&certificate, &private_key);
        }
        (None, None) => {}
        (..) => anyhow::bail!("LAKE_TLS_CERT_FILE and LAKE_TLS_KEY_FILE must be set together"),
    }
    Ok(security)
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

fn validate_protected_file(path: &Path) -> anyhow::Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect principal map {}", path.display()))?;
    if !metadata.file_type().is_file() {
        anyhow::bail!("principal map {} must be a regular file", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        if metadata.permissions().mode() & 0o077 != 0 {
            anyhow::bail!(
                "principal map {} must not be accessible by group or other users",
                path.display()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use lake_common::Principal;
    use tempfile::tempdir;
    use tonic::{Request, service::Interceptor};

    use super::{
        client_security_from_files, query_ticket_keys_from_file, server_security_from_files,
        server_security_from_principal_file,
    };

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

    #[cfg(unix)]
    #[test]
    fn server_principal_map_loads_protected_tenant_bindings() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().unwrap();
        let path = root.path().join("principals.json");
        fs::write(
            &path,
            r#"{"bindings":[{"token":"alpha-secret","principal_id":"alice","tenant_id":"alpha","role":"user","namespaces":["alpha"]}]}"#,
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        let security = server_security_from_principal_file(&path).unwrap();
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer alpha-secret".parse().unwrap());
        let accepted = security.interceptor().call(request).unwrap();
        let principal = accepted.extensions().get::<Principal>().unwrap();
        assert_eq!(principal.tenant().as_str(), "alpha");
        assert!(principal.can_access_namespace("alpha"));
        assert!(!format!("{security:?}").contains("alpha-secret"));

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(server_security_from_principal_file(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn query_ticket_key_ring_requires_protected_bounded_unique_secrets() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().unwrap();
        let path = root.path().join("ticket-keys.json");
        let active = "active-query-ticket-secret-material-0001";
        let old = "previous-query-ticket-secret-material-01";
        fs::write(
            &path,
            format!(r#"{{"active":"{active}","verification":["{old}"]}}"#),
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        let keys = query_ticket_keys_from_file(&path).unwrap();

        let debug = format!("{keys:?}");
        assert!(!debug.contains(active));
        assert!(!debug.contains(old));

        fs::write(
            &path,
            format!(r#"{{"active":"{active}","verification":["{active}"]}}"#),
        )
        .unwrap();
        let duplicate = query_ticket_keys_from_file(&path).unwrap_err();
        assert!(!duplicate.to_string().contains(active));

        fs::write(
            &path,
            r#"{"active":"active-query-ticket-secret-material-0001","verification":["old-query-ticket-secret-material-000001","old-query-ticket-secret-material-000002","old-query-ticket-secret-material-000003","old-query-ticket-secret-material-000004"]}"#,
        )
        .unwrap();
        assert!(query_ticket_keys_from_file(&path).is_err());

        fs::write(&path, r#"{"active":"too-short","verification":[]}"#).unwrap();
        assert!(query_ticket_keys_from_file(&path).is_err());

        fs::write(
            &path,
            format!(r#"{{"active":"{active}","verification":[],"extra":true}}"#),
        )
        .unwrap();
        assert!(query_ticket_keys_from_file(&path).is_err());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(query_ticket_keys_from_file(&path).is_err());
    }
}
