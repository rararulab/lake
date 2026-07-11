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

//! Validated authenticated-principal and tenant authorization values.

use std::{collections::BTreeSet, fmt, sync::Arc};

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum PrincipalError {
    #[snafu(display("principal identifier is invalid"))]
    InvalidPrincipalId,

    #[snafu(display("tenant identifier is invalid"))]
    InvalidTenantId,

    #[snafu(display("namespace grant is invalid"))]
    InvalidNamespaceGrant,

    #[snafu(display("user principals require at least one namespace grant"))]
    MissingNamespaceGrant,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PrincipalId(Arc<str>);

impl PrincipalId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, PrincipalError> {
        let value = value.into();
        if !valid_principal_id(&value) {
            return Err(PrincipalError::InvalidPrincipalId);
        }
        Ok(Self(Arc::from(value)))
    }

    #[must_use]
    pub fn as_str(&self) -> &str { &self.0 }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TenantId(Arc<str>);

impl TenantId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, PrincipalError> {
        let value = value.into();
        if !valid_tenant_id(&value) {
            return Err(PrincipalError::InvalidTenantId);
        }
        Ok(Self(Arc::from(value)))
    }

    #[must_use]
    pub fn as_str(&self) -> &str { &self.0 }
}

impl fmt::Display for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalRole {
    User,
    QueryService,
    MetadataPeer,
    Admin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Principal {
    id:         PrincipalId,
    tenant:     TenantId,
    role:       PrincipalRole,
    namespaces: BTreeSet<Arc<str>>,
}

impl Principal {
    pub fn try_new<I, S>(
        id: PrincipalId,
        tenant: TenantId,
        role: PrincipalRole,
        namespaces: I,
    ) -> Result<Self, PrincipalError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut validated = BTreeSet::new();
        for namespace in namespaces {
            let namespace = namespace.as_ref();
            if !valid_namespace(namespace) {
                return Err(PrincipalError::InvalidNamespaceGrant);
            }
            validated.insert(Arc::from(namespace));
        }
        if role == PrincipalRole::User && validated.is_empty() {
            return Err(PrincipalError::MissingNamespaceGrant);
        }
        Ok(Self {
            id,
            tenant,
            role,
            namespaces: validated,
        })
    }

    /// Backward-compatible deployment credential with unrestricted authority.
    #[must_use]
    pub fn deployment_admin() -> Self {
        Self {
            id:         PrincipalId(Arc::from("deployment-bearer")),
            tenant:     TenantId(Arc::from("deployment")),
            role:       PrincipalRole::Admin,
            namespaces: BTreeSet::new(),
        }
    }

    #[must_use]
    pub fn id(&self) -> &PrincipalId { &self.id }

    #[must_use]
    pub fn subject(&self) -> &str { self.id.as_str() }

    #[must_use]
    pub fn tenant(&self) -> &TenantId { &self.tenant }

    #[must_use]
    pub const fn role(&self) -> PrincipalRole { self.role }

    #[must_use]
    pub fn can_access_namespace(&self, namespace: &str) -> bool {
        self.role == PrincipalRole::Admin || self.namespaces.contains(namespace)
    }

    pub fn namespaces(&self) -> impl ExactSizeIterator<Item = &str> {
        self.namespaces.iter().map(AsRef::as_ref)
    }
}

fn valid_principal_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@' | b':')
        })
}

fn valid_tenant_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn valid_namespace(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}
