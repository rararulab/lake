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

use arrow_flight::{FlightClient, sql::client::FlightSqlServiceClient};
use snafu::Snafu;
use subtle::ConstantTimeEq;
use tonic::{
    Request, Status,
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig},
};

/// Errors raised while constructing Flight transport security.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum FlightSecurityError {
    /// Bearer credentials must be non-empty and legal ASCII metadata.
    #[snafu(display("bearer credential is empty or invalid"))]
    InvalidBearerCredential,

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

/// Identity installed into authenticated tonic request extensions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Principal {
    subject: &'static str,
}

impl Principal {
    /// Stable subject for the initial deployment-level bearer authenticator.
    #[must_use]
    pub const fn subject(&self) -> &'static str { self.subject }
}

/// Constant-time opaque bearer authenticator for a tonic interceptor.
#[derive(Clone)]
pub struct BearerAuthenticator {
    expected: BearerToken,
}

impl BearerAuthenticator {
    /// Construct an authenticator without exposing the credential in errors.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        Ok(Self {
            expected: BearerToken::new(value)?,
        })
    }
}

impl fmt::Debug for BearerAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerAuthenticator")
            .field("expected", &self.expected)
            .finish()
    }
}

impl Interceptor for BearerAuthenticator {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        let supplied = request.metadata().get("authorization");
        let accepted = supplied.is_some_and(|value| {
            let actual = value.as_encoded_bytes();
            let expected = self.expected.authorization.as_encoded_bytes();
            bool::from(actual.ct_eq(expected))
        });
        if !accepted {
            return Err(Status::unauthenticated(
                "missing or invalid bearer credential",
            ));
        }
        request.extensions_mut().insert(Principal {
            subject: "deployment-bearer",
        });
        Ok(request)
    }
}

/// Cloneable interceptor supporting either explicit development mode or
/// authenticated production requests.
#[derive(Clone, Debug)]
pub struct ServerInterceptor {
    bearer: Option<BearerAuthenticator>,
}

impl Interceptor for ServerInterceptor {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        match &mut self.bearer {
            Some(authenticator) => authenticator.call(request),
            None => Ok(request),
        }
    }
}

/// Server-side TLS identity, authentication, and exposure policy.
#[derive(Clone)]
pub struct ServerSecurity {
    bearer:       Option<BearerAuthenticator>,
    tls_identity: Option<Identity>,
}

impl ServerSecurity {
    /// Explicit plaintext/anonymous configuration for loopback development.
    #[must_use]
    pub const fn insecure() -> Self {
        Self {
            bearer:       None,
            tls_identity: None,
        }
    }

    /// Require one opaque deployment bearer credential on every RPC.
    pub fn with_bearer_token(value: impl Into<String>) -> Result<Self> {
        Ok(Self {
            bearer:       Some(BearerAuthenticator::new(value)?),
            tls_identity: None,
        })
    }

    /// Install a PEM certificate chain and private key for server TLS.
    #[must_use]
    pub fn with_tls_identity_pem(mut self, certificate: &[u8], private_key: &[u8]) -> Self {
        self.tls_identity = Some(Identity::from_pem(certificate, private_key));
        self
    }

    /// Validate that a remotely reachable listener cannot downgrade silently.
    pub fn validate_exposure(&self, addr: SocketAddr, allow_insecure: bool) -> Result<()> {
        if addr.ip().is_loopback()
            || allow_insecure
            || (self.bearer.is_some() && self.tls_identity.is_some())
        {
            return Ok(());
        }
        Err(FlightSecurityError::InsecureExposure { addr })
    }

    /// Build the interceptor applied to the complete Flight service.
    #[must_use]
    pub fn interceptor(&self) -> ServerInterceptor {
        ServerInterceptor {
            bearer: self.bearer.clone(),
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
}

impl ClientSecurity {
    /// Start with an explicit plaintext/anonymous development configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            bearer:      None,
            ca:          None,
            server_name: None,
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
        self
    }

    /// Override the certificate DNS name, useful for internal service routing.
    #[must_use]
    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = Some(server_name.into());
        self
    }

    /// Build the tonic endpoint shared by eager and lazy connections.
    fn endpoint(&self, endpoint: impl Into<String>) -> Result<Endpoint> {
        let endpoint = Endpoint::from_shared(endpoint.into())
            .map_err(|source| FlightSecurityError::InvalidEndpoint { source })?;
        let is_https = endpoint.uri().scheme_str() == Some("https");
        if !is_https && (self.ca.is_some() || self.server_name.is_some()) {
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
            .finish()
    }
}

fn ensure_crypto_provider() { let _ = rustls::crypto::ring::default_provider().install_default(); }

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use arrow_flight::{FlightClient, sql::client::FlightSqlServiceClient};
    use tonic::{Request, service::Interceptor};

    use super::{BearerAuthenticator, ClientSecurity, ServerSecurity};

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
        assert!(insecure.validate_exposure(public, true).is_ok());

        let auth_only = ServerSecurity::with_bearer_token("secret").expect("auth");
        assert!(auth_only.validate_exposure(public, false).is_err());

        let secure = auth_only.with_tls_identity_pem(b"certificate", b"private-key");
        assert!(secure.validate_exposure(public, false).is_ok());
    }
}
