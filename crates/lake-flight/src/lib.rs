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

//! Shared Flight transport-security contracts.

use std::{fmt, net::SocketAddr, sync::Arc};

use arrow_flight::{FlightClient, FlightData, sql::client::FlightSqlServiceClient};
use lake_common::AppendPayloadDigest;
pub use lake_common::{Principal, PrincipalId, PrincipalRole, TenantId};
use sha2::{Digest, Sha256};
use snafu::Snafu;
use subtle::ConstantTimeEq;
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig},
};

/// Metadata key used by trusted Query and metadata peers to preserve an
/// already-authorized namespace across an internal Flight hop.
pub const DELEGATED_NAMESPACE_HEADER: &str = "x-lake-delegated-namespace";
/// Internal authenticated header carrying the original principal's tenant.
pub const DELEGATED_TENANT_HEADER: &str = "x-lake-delegated-tenant";

const APPEND_DIGEST_DOMAIN: &[u8] = b"lake.append.flight-metadata.v1\0";

/// Incrementally hashes the ordered Arrow Flight metadata messages in one
/// append. The descriptor is excluded because it carries the digest itself;
/// every message's Arrow header, body, and application metadata are
/// length-delimited and covered.
pub struct AppendFlightPayloadHasher {
    hasher: Sha256,
}

impl AppendFlightPayloadHasher {
    /// Start a version-1 append payload digest.
    #[must_use]
    pub fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(APPEND_DIGEST_DOMAIN);
        Self { hasher }
    }

    /// Include one Flight message in wire order.
    pub fn update(&mut self, data: &FlightData) {
        for field in [&data.data_header, &data.data_body, &data.app_metadata] {
            self.hasher.update((field.len() as u64).to_be_bytes());
            self.hasher.update(field);
        }
    }

    /// Finish and return the lowercase SHA-256 digest.
    #[must_use]
    pub fn finalize(self) -> AppendPayloadDigest {
        use std::fmt::Write as _;

        let digest = self.hasher.finalize();
        let mut encoded = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
        }
        AppendPayloadDigest::parse(encoded)
            .expect("SHA-256 is a lowercase 64-character hexadecimal digest")
    }
}

impl Default for AppendFlightPayloadHasher {
    fn default() -> Self { Self::new() }
}

/// Hash an already-buffered append payload using the version-1 algorithm.
#[must_use]
pub fn append_flight_payload_digest(messages: &[FlightData]) -> AppendPayloadDigest {
    let mut hasher = AppendFlightPayloadHasher::new();
    for message in messages {
        hasher.update(message);
    }
    hasher.finalize()
}

/// Errors raised while constructing Flight transport security.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum FlightSecurityError {
    /// Bearer credentials must be non-empty and legal ASCII metadata.
    #[snafu(display("bearer credential is empty or invalid"))]
    InvalidBearerCredential,

    #[snafu(display("bearer principal map is empty or contains a duplicate credential"))]
    InvalidBearerPrincipalMap,

    /// The tonic endpoint URI is invalid.
    #[snafu(display("invalid Flight endpoint"))]
    InvalidEndpoint { source: tonic::transport::Error },

    /// TLS settings could not be applied to the endpoint.
    #[snafu(display("invalid Flight TLS configuration"))]
    InvalidTls { source: tonic::transport::Error },

    /// The configured endpoint could not be reached.
    #[snafu(display("Flight connection failed"))]
    Connect { source: tonic::transport::Error },

    /// TLS-only client settings were supplied for plaintext HTTP.
    #[snafu(display("TLS client settings require an https Flight endpoint"))]
    TlsRequiresHttps,

    /// A remotely reachable listener must not silently run insecurely.
    #[snafu(display(
        "non-loopback Flight listener {addr} requires TLS and authentication or an explicit \
         insecure override"
    ))]
    InsecureExposure { addr: SocketAddr },
}

/// Result type for Flight transport-security construction.
pub type Result<T> = std::result::Result<T, FlightSecurityError>;

#[derive(Clone)]
struct BearerToken {
    value:         Arc<str>,
    authorization: MetadataValue<Ascii>,
}

impl BearerToken {
    fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(FlightSecurityError::InvalidBearerCredential);
        }
        let authorization = format!("Bearer {value}")
            .parse()
            .map_err(|_| FlightSecurityError::InvalidBearerCredential)?;
        Ok(Self {
            value: Arc::from(value),
            authorization,
        })
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BearerToken([REDACTED])")
    }
}

#[derive(Clone)]
pub struct BearerPrincipalBinding {
    token:     BearerToken,
    principal: Principal,
}

impl BearerPrincipalBinding {
    pub fn new(value: impl Into<String>, principal: Principal) -> Result<Self> {
        Ok(Self {
            token: BearerToken::new(value)?,
            principal,
        })
    }
}

impl fmt::Debug for BearerPrincipalBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerPrincipalBinding")
            .field("token", &self.token)
            .field("principal", &self.principal)
            .finish()
    }
}

/// Constant-time opaque bearer authenticator for a tonic interceptor.
#[derive(Clone)]
pub struct BearerAuthenticator {
    bindings: Arc<[BearerPrincipalBinding]>,
}

impl BearerAuthenticator {
    /// Construct an authenticator without exposing the credential in errors.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        Self::from_bindings([BearerPrincipalBinding::new(
            value,
            Principal::deployment_admin(),
        )?])
    }

    pub fn from_bindings<I>(bindings: I) -> Result<Self>
    where
        I: IntoIterator<Item = BearerPrincipalBinding>,
    {
        let bindings = bindings.into_iter().collect::<Vec<_>>();
        if bindings.is_empty()
            || bindings.iter().enumerate().any(|(index, binding)| {
                bindings[index + 1..]
                    .iter()
                    .any(|other| binding.token.value == other.token.value)
            })
        {
            return Err(FlightSecurityError::InvalidBearerPrincipalMap);
        }
        Ok(Self {
            bindings: Arc::from(bindings),
        })
    }
}

impl fmt::Debug for BearerAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerAuthenticator")
            .field("bindings", &self.bindings)
            .finish()
    }
}

impl Interceptor for BearerAuthenticator {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        let supplied = request.metadata().get("authorization");
        let principal = supplied.and_then(|value| {
            let actual = value.as_encoded_bytes();
            let mut matched = None;
            for binding in self.bindings.iter() {
                if bool::from(actual.ct_eq(binding.token.authorization.as_encoded_bytes())) {
                    matched = Some(binding.principal.clone());
                }
            }
            matched
        });
        let Some(principal) = principal else {
            return Err(Status::unauthenticated(
                "missing or invalid bearer credential",
            ));
        };
        request.extensions_mut().insert(principal);
        Ok(request)
    }
}

/// Cloneable interceptor supporting either explicit development mode or
/// authenticated production requests.
#[derive(Clone, Debug)]
pub struct ServerInterceptor {
    bearer:                Option<BearerAuthenticator>,
    development_principal: Option<Principal>,
}

impl Interceptor for ServerInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        match &mut self.bearer {
            Some(authenticator) => authenticator.call(request),
            None => {
                if let Some(principal) = &self.development_principal {
                    request.extensions_mut().insert(principal.clone());
                }
                Ok(request)
            }
        }
    }
}

/// Server-side TLS identity, authentication, and exposure policy.
#[derive(Clone)]
pub struct ServerSecurity {
    bearer:                Option<BearerAuthenticator>,
    development_principal: Option<Principal>,
    tls_identity:          Option<Identity>,
}

impl ServerSecurity {
    /// Explicit plaintext/anonymous configuration for loopback development.
    #[must_use]
    pub fn insecure() -> Self { Self::insecure_with_principal(Principal::development_admin()) }

    /// Explicit plaintext development mode with a caller-selected identity.
    #[must_use]
    pub fn insecure_with_principal(principal: Principal) -> Self {
        Self {
            bearer:                None,
            development_principal: Some(principal),
            tls_identity:          None,
        }
    }

    /// Require one opaque deployment bearer credential on every RPC.
    pub fn with_bearer_token(value: impl Into<String>) -> Result<Self> {
        Ok(Self {
            bearer:                Some(BearerAuthenticator::new(value)?),
            development_principal: None,
            tls_identity:          None,
        })
    }

    /// Require one of the supplied opaque credential-to-principal bindings.
    pub fn with_bearer_principals<I>(bindings: I) -> Result<Self>
    where
        I: IntoIterator<Item = BearerPrincipalBinding>,
    {
        Ok(Self {
            bearer:                Some(BearerAuthenticator::from_bindings(bindings)?),
            development_principal: None,
            tls_identity:          None,
        })
    }

    /// Install a PEM certificate chain and private key for server TLS.
    #[must_use]
    pub fn with_tls_identity_pem(mut self, certificate: &[u8], private_key: &[u8]) -> Self {
        self.tls_identity = Some(Identity::from_pem(certificate, private_key));
        self
    }

    /// Validate that a remotely reachable listener always authenticates; an
    /// explicit override may delegate TLS, but never identity, to a proxy.
    pub fn validate_exposure(&self, addr: SocketAddr, allow_insecure: bool) -> Result<()> {
        if addr.ip().is_loopback()
            || (self.bearer.is_some() && (allow_insecure || self.tls_identity.is_some()))
        {
            return Ok(());
        }
        Err(FlightSecurityError::InsecureExposure { addr })
    }

    /// Build the interceptor applied to the complete Flight service.
    #[must_use]
    pub fn interceptor(&self) -> ServerInterceptor {
        ServerInterceptor {
            bearer:                self.bearer.clone(),
            development_principal: self.development_principal.clone(),
        }
    }

    /// Build tonic TLS configuration when a server identity is installed.
    #[must_use]
    pub fn tls_config(&self) -> Option<ServerTlsConfig> {
        self.tls_identity.clone().map(|identity| {
            ensure_crypto_provider();
            ServerTlsConfig::new().identity(identity)
        })
    }
}

impl fmt::Debug for ServerSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerSecurity")
            .field("authenticated", &self.bearer.is_some())
            .field("development_principal", &self.development_principal)
            .field("tls", &self.tls_identity.is_some())
            .finish()
    }
}

/// Client-side verified TLS and bearer injection shared by all Flight clients.
#[derive(Clone, Default)]
pub struct ClientSecurity {
    bearer:      Option<BearerToken>,
    ca:          Option<Certificate>,
    server_name: Option<String>,
    use_tls:     bool,
}

impl ClientSecurity {
    /// Start with an explicit plaintext/anonymous development configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            bearer:      None,
            ca:          None,
            server_name: None,
            use_tls:     false,
        }
    }

    /// Add the bearer credential sent on every Flight RPC.
    pub fn with_bearer_token(mut self, value: impl Into<String>) -> Result<Self> {
        self.bearer = Some(BearerToken::new(value)?);
        Ok(self)
    }

    /// Add a PEM CA certificate used alongside enabled public roots.
    #[must_use]
    pub fn with_ca_certificate_pem(mut self, certificate: Vec<u8>) -> Self {
        self.ca = Some(Certificate::from_pem(certificate));
        self.use_tls = true;
        self
    }

    /// Override the certificate DNS name, useful for internal service routing.
    #[must_use]
    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = Some(server_name.into());
        self.use_tls = true;
        self
    }

    /// Require TLS using the enabled public trust roots.
    #[must_use]
    pub const fn with_tls(mut self) -> Self {
        self.use_tls = true;
        self
    }

    /// Build a full endpoint URI from a discovered host-and-port authority.
    #[must_use]
    pub fn endpoint_for_authority(&self, authority: &str) -> String {
        let scheme = if self.use_tls { "https" } else { "http" };
        format!("{scheme}://{authority}")
    }

    /// Build the tonic endpoint shared by eager and lazy connections.
    fn endpoint(&self, endpoint: impl Into<String>) -> Result<Endpoint> {
        let endpoint = Endpoint::from_shared(endpoint.into())
            .map_err(|source| FlightSecurityError::InvalidEndpoint { source })?;
        let is_https = endpoint.uri().scheme_str() == Some("https");
        if !is_https && self.use_tls {
            return Err(FlightSecurityError::TlsRequiresHttps);
        }
        let endpoint = if is_https {
            ensure_crypto_provider();
            let mut tls = ClientTlsConfig::new().with_enabled_roots();
            if let Some(ca) = &self.ca {
                tls = tls.ca_certificate(ca.clone());
            }
            if let Some(server_name) = &self.server_name {
                tls = tls.domain_name(server_name);
            }
            endpoint
                .tls_config(tls)
                .map_err(|source| FlightSecurityError::InvalidTls { source })?
        } else {
            endpoint
        };
        Ok(endpoint)
    }

    /// Configure a lazy tonic Channel. TLS parsing/verification occurs when the
    /// channel first connects.
    pub fn connect_lazy(&self, endpoint: impl Into<String>) -> Result<Channel> {
        Ok(self.endpoint(endpoint)?.connect_lazy())
    }

    /// Connect a tonic Channel with the configured verified TLS policy.
    pub async fn connect(&self, endpoint: impl Into<String>) -> Result<Channel> {
        self.endpoint(endpoint)?
            .connect()
            .await
            .map_err(|source| FlightSecurityError::Connect { source })
    }

    /// Apply the same bearer credential to all high-level Flight operations.
    pub fn apply_to_flight_client(&self, client: &mut FlightClient) -> Result<()> {
        if let Some(bearer) = &self.bearer {
            client
                .metadata_mut()
                .insert("authorization", bearer.authorization.clone());
        }
        Ok(())
    }

    /// Apply the same bearer credential to Flight SQL and its DoGet clients.
    pub fn apply_to_sql_client(&self, client: &mut FlightSqlServiceClient<Channel>) {
        if let Some(bearer) = &self.bearer {
            client.set_token(bearer.value.to_string());
        }
    }

    /// Attach credentials to a generated low-level Flight request.
    pub fn authorize_request<T>(&self, mut request: Request<T>) -> Request<T> {
        if let Some(bearer) = &self.bearer {
            request
                .metadata_mut()
                .insert("authorization", bearer.authorization.clone());
        }
        request
    }
}

impl fmt::Debug for ClientSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientSecurity")
            .field("authenticated", &self.bearer.is_some())
            .field("custom_ca", &self.ca.is_some())
            .field("server_name", &self.server_name)
            .field("tls", &self.use_tls)
            .finish()
    }
}

fn ensure_crypto_provider() { let _ = rustls::crypto::ring::default_provider().install_default(); }

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use arrow_flight::{FlightClient, sql::client::FlightSqlServiceClient};
    use lake_common::{Principal, PrincipalId, PrincipalRole, TenantId};
    use tonic::{Request, service::Interceptor};

    use super::{
        BearerAuthenticator, BearerPrincipalBinding, ClientSecurity, ServerSecurity,
        append_flight_payload_digest,
    };

    fn request_with_authorization(value: &str) -> Request<()> {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", value.parse().expect("valid metadata"));
        request
    }

    #[test]
    fn bearer_authenticator_rejects_missing_and_wrong_credentials() {
        let secret = "query-secret-value";
        let mut authenticator = BearerAuthenticator::new(secret).expect("valid secret");

        let missing = authenticator
            .call(Request::new(()))
            .expect_err("missing rejected");
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);
        assert!(!missing.message().contains(secret));

        let wrong = authenticator
            .call(request_with_authorization("Bearer wrong"))
            .expect_err("wrong rejected");
        assert_eq!(wrong.code(), tonic::Code::Unauthenticated);
        assert!(!wrong.message().contains(secret));

        let accepted = authenticator
            .call(request_with_authorization(&format!("Bearer {secret}")))
            .expect("valid credential");
        assert!(accepted.extensions().get::<super::Principal>().is_some());
        assert!(!format!("{authenticator:?}").contains(secret));
    }

    #[test]
    fn bearer_principals_are_tenant_scoped_and_redacted() {
        let alpha_secret = "alpha-secret-value";
        let beta_secret = "beta-secret-value";
        let alpha = Principal::try_new(
            PrincipalId::try_new("alice@example.com").unwrap(),
            TenantId::try_new("tenant-alpha").unwrap(),
            PrincipalRole::User,
            ["alpha_episodes"],
        )
        .unwrap();
        let beta = Principal::try_new(
            PrincipalId::try_new("bob@example.com").unwrap(),
            TenantId::try_new("tenant-beta").unwrap(),
            PrincipalRole::User,
            ["beta_episodes"],
        )
        .unwrap();
        let mut authenticator = BearerAuthenticator::from_bindings([
            BearerPrincipalBinding::new(alpha_secret, alpha.clone()).unwrap(),
            BearerPrincipalBinding::new(beta_secret, beta.clone()).unwrap(),
        ])
        .unwrap();

        let accepted = authenticator
            .call(request_with_authorization(&format!(
                "Bearer {alpha_secret}"
            )))
            .unwrap();
        assert_eq!(accepted.extensions().get::<Principal>(), Some(&alpha));
        assert!(alpha.can_access_namespace("alpha_episodes"));
        assert!(!alpha.can_access_namespace("beta_episodes"));
        let debug = format!("{authenticator:?}");
        assert!(!debug.contains(alpha_secret));
        assert!(!debug.contains(beta_secret));

        assert!(TenantId::try_new("../escape").is_err());
        assert!(PrincipalId::try_new("").is_err());
        let duplicate = BearerAuthenticator::from_bindings([
            BearerPrincipalBinding::new(alpha_secret, alpha.clone()).unwrap(),
            BearerPrincipalBinding::new(alpha_secret, beta).unwrap(),
        ])
        .unwrap_err();
        assert!(!duplicate.to_string().contains(alpha_secret));
    }

    #[tokio::test]
    async fn client_security_configures_tls_and_authorization() {
        let secret = "client-secret-value";
        let security = ClientSecurity::new()
            .with_bearer_token(secret)
            .expect("valid secret")
            .with_ca_certificate_pem(b"test-ca".to_vec())
            .with_server_name("query.internal");

        let channel = security
            .connect_lazy("https://query.internal:443")
            .expect("TLS channel configuration");
        let mut flight = FlightClient::new(channel.clone());
        security
            .apply_to_flight_client(&mut flight)
            .expect("Flight auth");
        assert_eq!(
            flight
                .metadata()
                .get("authorization")
                .expect("authorization")
                .to_str()
                .expect("ASCII"),
            format!("Bearer {secret}")
        );

        let mut sql = FlightSqlServiceClient::new(channel);
        security.apply_to_sql_client(&mut sql);
        assert_eq!(sql.token().map(String::as_str), Some(secret));
        assert!(!format!("{security:?}").contains(secret));
    }

    #[test]
    fn non_loopback_server_security_fails_closed() {
        let loopback: SocketAddr = "127.0.0.1:50051".parse().expect("loopback");
        let public: SocketAddr = "0.0.0.0:50051".parse().expect("public");

        let insecure = ServerSecurity::insecure();
        assert!(insecure.validate_exposure(loopback, false).is_ok());
        assert!(insecure.validate_exposure(public, false).is_err());
        assert!(insecure.validate_exposure(public, true).is_err());

        let auth_only = ServerSecurity::with_bearer_token("secret").expect("auth");
        assert!(auth_only.validate_exposure(public, false).is_err());
        assert!(auth_only.validate_exposure(public, true).is_ok());

        let secure = auth_only.with_tls_identity_pem(b"certificate", b"private-key");
        assert!(secure.validate_exposure(public, false).is_ok());
    }

    #[test]
    fn insecure_loopback_uses_explicit_development_principal() {
        let development = Principal::try_new(
            PrincipalId::try_new("local-developer").unwrap(),
            TenantId::try_new("development").unwrap(),
            PrincipalRole::User,
            ["local"],
        )
        .unwrap();
        let security = ServerSecurity::insecure_with_principal(development.clone());

        let accepted = security
            .interceptor()
            .call(Request::new(()))
            .expect("loopback development request");

        assert_eq!(accepted.extensions().get::<Principal>(), Some(&development));
        let public: SocketAddr = "0.0.0.0:50051".parse().unwrap();
        assert!(security.validate_exposure(public, false).is_err());
    }

    #[test]
    fn append_flight_payload_digest_covers_ordered_metadata_messages() {
        let mut first = arrow_flight::FlightData {
            data_header: b"schema".to_vec().into(),
            data_body: b"body-one".to_vec().into(),
            ..Default::default()
        };
        first.flight_descriptor = Some(arrow_flight::FlightDescriptor::new_cmd("descriptor-a"));
        let second = arrow_flight::FlightData {
            data_header: b"record-batch".to_vec().into(),
            data_body: b"body-two".to_vec().into(),
            app_metadata: b"metadata".to_vec().into(),
            ..Default::default()
        };
        let expected = append_flight_payload_digest(&[first.clone(), second.clone()]);

        first.flight_descriptor = Some(arrow_flight::FlightDescriptor::new_cmd("descriptor-b"));
        assert_eq!(
            append_flight_payload_digest(&[first, second.clone()]),
            expected,
            "the self-referential command descriptor is excluded"
        );
        assert_ne!(
            append_flight_payload_digest(&[
                second,
                arrow_flight::FlightData {
                    data_header: b"schema".to_vec().into(),
                    data_body: b"body-one".to_vec().into(),
                    ..Default::default()
                }
            ]),
            expected,
            "message order is covered"
        );
    }
}
