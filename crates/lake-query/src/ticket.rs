use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lake_common::Principal;
use prost::Message;
use ring::{
    aead::{self, Aad, LessSafeKey, Nonce, UnboundKey},
    digest,
    rand::{SecureRandom, SystemRandom},
};
use snafu::Snafu;
use subtle::ConstantTimeEq;

const TICKET_MAGIC: &[u8; 4] = b"LQTK";
const TICKET_VERSION: u8 = 2;
const KEY_ID_BYTES: usize = 16;
const NONCE_BYTES: usize = 12;
const NONCE_PREFIX_BYTES: usize = 8;
const HEADER_BYTES: usize = TICKET_MAGIC.len() + 1 + KEY_ID_BYTES + NONCE_BYTES;
const MAX_KEYS: usize = 4;
pub(crate) const MAX_TABLE_SNAPSHOTS: usize = 64;
const MAX_NAMESPACE_BYTES: usize = 256;
const MAX_TABLE_BYTES: usize = 256;
const MAX_ENGINE_BYTES: usize = 64;
const MAX_LOCATION_BYTES: usize = 4096;
const MAX_INCARNATION_BYTES: usize = 256;
const MIN_SECRET_BYTES: usize = 32;
const MAX_SECRET_BYTES: usize = 4096;
const FUTURE_SKEW: Duration = Duration::from_secs(30);
const MAX_TICKET_TTL: Duration = Duration::from_hours(1);
pub(crate) const MAX_DURABLE_JOB_TTL: Duration = Duration::from_hours(24);

/// Redacted statement-ticket validation or key-configuration failure.
#[derive(Debug, Eq, PartialEq, Snafu)]
#[non_exhaustive]
pub enum QueryTicketError {
    /// A ticket is malformed, unauthenticated, expired, or identity-mismatched.
    #[snafu(display("statement ticket is invalid"))]
    Invalid,
    /// Key count, secret length, TTL, audience, or randomness is invalid.
    #[snafu(display("statement ticket key configuration is invalid"))]
    InvalidConfiguration,
}

#[derive(Clone)]
struct TicketKey {
    id:  [u8; KEY_ID_BYTES],
    key: LessSafeKey,
}

impl TicketKey {
    fn derive(secret: &[u8]) -> Result<Self, QueryTicketError> {
        if !(MIN_SECRET_BYTES..=MAX_SECRET_BYTES).contains(&secret.len()) {
            return Err(QueryTicketError::InvalidConfiguration);
        }
        let mut context = digest::Context::new(&digest::SHA256);
        context.update(b"lake-query-statement-ticket-key-v1\0");
        context.update(secret);
        let digest = context.finish();
        let mut material = [0_u8; 32];
        material.copy_from_slice(digest.as_ref());

        let mut context = digest::Context::new(&digest::SHA256);
        context.update(b"lake-query-statement-ticket-key-id-v1\0");
        context.update(&material);
        let digest = context.finish();
        let mut id = [0_u8; KEY_ID_BYTES];
        id.copy_from_slice(&digest.as_ref()[..KEY_ID_BYTES]);
        let key = UnboundKey::new(&aead::AES_256_GCM, &material)
            .map(LessSafeKey::new)
            .map_err(|_| QueryTicketError::InvalidConfiguration)?;
        Ok(Self { id, key })
    }
}

/// One active sealing key plus a bounded set of rotation verification keys.
#[derive(Clone)]
pub struct QueryTicketKeyRing {
    keys: Arc<[TicketKey]>,
}

impl QueryTicketKeyRing {
    /// Derive one active AEAD key and at most three verification keys from
    /// independent high-entropy secrets of `32..=4096` bytes.
    pub fn try_new<'a, I>(active: &[u8], verification: I) -> Result<Self, QueryTicketError>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut keys = vec![TicketKey::derive(active)?];
        for secret in verification {
            if keys.len() == MAX_KEYS {
                return Err(QueryTicketError::InvalidConfiguration);
            }
            let key = TicketKey::derive(secret)?;
            if keys.iter().any(|candidate| candidate.id == key.id) {
                return Err(QueryTicketError::InvalidConfiguration);
            }
            keys.push(key);
        }
        Ok(Self { keys: keys.into() })
    }

    pub(crate) fn ephemeral() -> Result<Self, QueryTicketError> {
        let mut secret = [0_u8; 32];
        SystemRandom::new()
            .fill(&mut secret)
            .map_err(|_| QueryTicketError::InvalidConfiguration)?;
        Self::try_new(&secret, std::iter::empty())
    }

    fn active(&self) -> &TicketKey { &self.keys[0] }

    fn find(&self, id: &[u8]) -> Option<&TicketKey> {
        self.keys.iter().find(|key| key.id.as_slice() == id)
    }
}

impl fmt::Debug for QueryTicketKeyRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryTicketKeyRing")
            .field("key_count", &self.keys.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Message)]
struct StatementTicketPayload {
    #[prost(uint64, tag = "1")]
    issued_at_secs:  u64,
    #[prost(uint64, tag = "2")]
    expires_at_secs: u64,
    #[prost(string, tag = "3")]
    principal_id:    String,
    #[prost(string, tag = "4")]
    tenant_id:       String,
    #[prost(string, tag = "5")]
    sql:             String,
    #[prost(message, repeated, tag = "6")]
    snapshots:       Vec<StatementTableSnapshot>,
}

/// One exact physical lake table generation carried inside an encrypted
/// statement ticket.
#[derive(Clone, Eq, PartialEq, Message)]
pub(crate) struct StatementTableSnapshot {
    #[prost(string, tag = "1")]
    pub(crate) namespace:      String,
    #[prost(string, tag = "2")]
    pub(crate) table:          String,
    #[prost(string, tag = "3")]
    pub(crate) engine:         String,
    #[prost(string, tag = "4")]
    pub(crate) location:       String,
    #[prost(string, tag = "5")]
    pub(crate) incarnation_id: String,
    #[prost(uint64, tag = "6")]
    pub(crate) version:        u64,
}

/// Authenticated statement and its complete immutable input set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StatementTicket {
    pub(crate) sql:       String,
    pub(crate) snapshots: Vec<StatementTableSnapshot>,
}

#[derive(Clone)]
pub(crate) struct StatementTicketCodec {
    keys:          QueryTicketKeyRing,
    ttl:           Duration,
    max_ttl:       Duration,
    audience:      Arc<str>,
    nonce_prefix:  [u8; NONCE_PREFIX_BYTES],
    nonce_counter: Arc<AtomicU32>,
}

impl StatementTicketCodec {
    pub(crate) fn try_new(
        keys: QueryTicketKeyRing,
        ttl: Duration,
        audience: impl Into<Arc<str>>,
    ) -> Result<Self, QueryTicketError> {
        Self::try_new_with_max_ttl(keys, ttl, audience, MAX_TICKET_TTL)
    }

    pub(crate) fn try_new_durable_job(
        keys: QueryTicketKeyRing,
        ttl: Duration,
        audience: impl Into<Arc<str>>,
    ) -> Result<Self, QueryTicketError> {
        Self::try_new_with_max_ttl(keys, ttl, audience, MAX_DURABLE_JOB_TTL)
    }

    fn try_new_with_max_ttl(
        keys: QueryTicketKeyRing,
        ttl: Duration,
        audience: impl Into<Arc<str>>,
        max_ttl: Duration,
    ) -> Result<Self, QueryTicketError> {
        let audience = audience.into();
        if ttl.is_zero() || ttl > max_ttl || audience.is_empty() {
            return Err(QueryTicketError::InvalidConfiguration);
        }
        let mut nonce_prefix = [0_u8; NONCE_PREFIX_BYTES];
        SystemRandom::new()
            .fill(&mut nonce_prefix)
            .map_err(|_| QueryTicketError::InvalidConfiguration)?;
        Ok(Self {
            keys,
            ttl,
            max_ttl,
            audience,
            nonce_prefix,
            nonce_counter: Arc::new(AtomicU32::new(0)),
        })
    }

    #[cfg(test)]
    pub(crate) fn seal(
        &self,
        sql: &str,
        principal: &Principal,
    ) -> Result<Vec<u8>, QueryTicketError> {
        self.seal_at(sql, principal, SystemTime::now())
    }

    pub(crate) fn seal_statement(
        &self,
        statement: &StatementTicket,
        principal: &Principal,
    ) -> Result<Vec<u8>, QueryTicketError> {
        self.seal_statement_at(statement, principal, SystemTime::now())
    }

    #[cfg(test)]
    fn seal_at(
        &self,
        sql: &str,
        principal: &Principal,
        now: SystemTime,
    ) -> Result<Vec<u8>, QueryTicketError> {
        self.seal_statement_at(
            &StatementTicket {
                sql:       sql.to_owned(),
                snapshots: Vec::new(),
            },
            principal,
            now,
        )
    }

    fn seal_statement_at(
        &self,
        statement: &StatementTicket,
        principal: &Principal,
        now: SystemTime,
    ) -> Result<Vec<u8>, QueryTicketError> {
        self.seal_statement_at_version(statement, principal, now, TICKET_VERSION)
    }

    fn seal_statement_at_version(
        &self,
        statement: &StatementTicket,
        principal: &Principal,
        now: SystemTime,
        version: u8,
    ) -> Result<Vec<u8>, QueryTicketError> {
        validate_statement(statement)?;
        let issued_at_secs = unix_seconds(now)?;
        let expires_at_secs = issued_at_secs
            .checked_add(self.ttl.as_secs())
            .ok_or(QueryTicketError::Invalid)?;
        let payload = StatementTicketPayload {
            issued_at_secs,
            expires_at_secs,
            principal_id: principal.subject().to_owned(),
            tenant_id: principal.tenant().as_str().to_owned(),
            sql: statement.sql.clone(),
            snapshots: statement.snapshots.clone(),
        };
        let mut plaintext = payload.encode_to_vec();
        let key = self.keys.active();
        let nonce_bytes = self.next_nonce()?;
        let mut header = Vec::with_capacity(HEADER_BYTES);
        header.extend_from_slice(TICKET_MAGIC);
        header.push(version);
        header.extend_from_slice(&key.id);
        header.extend_from_slice(&nonce_bytes);
        let aad = self.aad(&header);
        key.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(aad.as_slice()),
                &mut plaintext,
            )
            .map_err(|_| QueryTicketError::Invalid)?;
        header.extend_from_slice(&plaintext);
        Ok(header)
    }

    #[cfg(test)]
    pub(crate) fn open(
        &self,
        ticket: &[u8],
        principal: &Principal,
    ) -> Result<String, QueryTicketError> {
        self.open_at(ticket, principal, SystemTime::now())
    }

    pub(crate) fn open_statement(
        &self,
        ticket: &[u8],
        principal: &Principal,
    ) -> Result<StatementTicket, QueryTicketError> {
        self.open_statement_at(ticket, principal, SystemTime::now())
    }

    #[cfg(test)]
    fn open_at(
        &self,
        ticket: &[u8],
        principal: &Principal,
        now: SystemTime,
    ) -> Result<String, QueryTicketError> {
        self.open_statement_at(ticket, principal, now)
            .map(|statement| statement.sql)
    }

    fn open_statement_at(
        &self,
        ticket: &[u8],
        principal: &Principal,
        now: SystemTime,
    ) -> Result<StatementTicket, QueryTicketError> {
        if ticket.len() <= HEADER_BYTES + aead::AES_256_GCM.tag_len()
            || &ticket[..TICKET_MAGIC.len()] != TICKET_MAGIC
            || ticket[TICKET_MAGIC.len()] != TICKET_VERSION
        {
            return Err(QueryTicketError::Invalid);
        }
        let key_start = TICKET_MAGIC.len() + 1;
        let nonce_start = key_start + KEY_ID_BYTES;
        let body_start = nonce_start + NONCE_BYTES;
        let key = self
            .keys
            .find(&ticket[key_start..nonce_start])
            .ok_or(QueryTicketError::Invalid)?;
        let nonce_bytes: [u8; NONCE_BYTES] = ticket[nonce_start..body_start]
            .try_into()
            .map_err(|_| QueryTicketError::Invalid)?;
        let aad = self.aad(&ticket[..body_start]);
        let mut ciphertext = ticket[body_start..].to_vec();
        let plaintext = key
            .key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(aad.as_slice()),
                &mut ciphertext,
            )
            .map_err(|_| QueryTicketError::Invalid)?;
        let payload =
            StatementTicketPayload::decode(&*plaintext).map_err(|_| QueryTicketError::Invalid)?;
        let now_secs = unix_seconds(now)?;
        let latest_issued = now_secs.saturating_add(FUTURE_SKEW.as_secs());
        if payload.issued_at_secs > latest_issued
            || payload.expires_at_secs <= now_secs
            || payload.expires_at_secs < payload.issued_at_secs
            || payload.expires_at_secs - payload.issued_at_secs > self.max_ttl.as_secs()
            || payload
                .principal_id
                .as_bytes()
                .ct_eq(principal.subject().as_bytes())
                .unwrap_u8()
                == 0
            || payload
                .tenant_id
                .as_bytes()
                .ct_eq(principal.tenant().as_str().as_bytes())
                .unwrap_u8()
                == 0
        {
            return Err(QueryTicketError::Invalid);
        }
        let statement = StatementTicket {
            sql:       payload.sql,
            snapshots: payload.snapshots,
        };
        validate_statement(&statement)?;
        Ok(statement)
    }

    fn aad(&self, header: &[u8]) -> Vec<u8> {
        let mut aad = Vec::with_capacity(header.len() + self.audience.len());
        aad.extend_from_slice(header);
        aad.extend_from_slice(self.audience.as_bytes());
        aad
    }

    fn next_nonce(&self) -> Result<[u8; NONCE_BYTES], QueryTicketError> {
        let counter = self
            .nonce_counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| QueryTicketError::Invalid)?;
        let mut nonce = [0_u8; NONCE_BYTES];
        nonce[..NONCE_PREFIX_BYTES].copy_from_slice(&self.nonce_prefix);
        nonce[NONCE_PREFIX_BYTES..].copy_from_slice(&counter.to_be_bytes());
        Ok(nonce)
    }
}

fn validate_statement(statement: &StatementTicket) -> Result<(), QueryTicketError> {
    if statement.sql.is_empty() || statement.snapshots.len() > MAX_TABLE_SNAPSHOTS {
        return Err(QueryTicketError::Invalid);
    }
    let mut previous: Option<(&str, &str)> = None;
    for snapshot in &statement.snapshots {
        if snapshot.namespace.is_empty()
            || snapshot.namespace.len() > MAX_NAMESPACE_BYTES
            || snapshot.table.is_empty()
            || snapshot.table.len() > MAX_TABLE_BYTES
            || snapshot.engine.is_empty()
            || snapshot.engine.len() > MAX_ENGINE_BYTES
            || snapshot.location.is_empty()
            || snapshot.location.len() > MAX_LOCATION_BYTES
            || snapshot.incarnation_id.is_empty()
            || snapshot.incarnation_id.len() > MAX_INCARNATION_BYTES
        {
            return Err(QueryTicketError::Invalid);
        }
        let identity = (snapshot.namespace.as_str(), snapshot.table.as_str());
        if previous.is_some_and(|previous| previous >= identity) {
            return Err(QueryTicketError::Invalid);
        }
        previous = Some(identity);
    }
    Ok(())
}

impl fmt::Debug for StatementTicketCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatementTicketCodec")
            .field("keys", &self.keys)
            .field("ttl", &self.ttl)
            .field("audience", &self.audience)
            .finish_non_exhaustive()
    }
}

fn unix_seconds(value: SystemTime) -> Result<u64, QueryTicketError> {
    value
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| QueryTicketError::Invalid)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::Ordering,
        time::{Duration, UNIX_EPOCH},
    };

    use lake_common::{Principal, PrincipalId, PrincipalRole, TenantId};

    use super::{
        MAX_LOCATION_BYTES, MAX_TABLE_SNAPSHOTS, QueryTicketError, QueryTicketKeyRing,
        StatementTableSnapshot, StatementTicket, StatementTicketCodec,
    };

    fn principal(id: &str, tenant: &str) -> Principal {
        Principal::try_new(
            PrincipalId::try_new(id).unwrap(),
            TenantId::try_new(tenant).unwrap(),
            PrincipalRole::User,
            [tenant],
        )
        .unwrap()
    }

    fn ring(active: &[u8], verification: &[&[u8]]) -> QueryTicketKeyRing {
        QueryTicketKeyRing::try_new(active, verification.iter().copied()).unwrap()
    }

    #[test]
    fn statement_ticket_is_confidential_and_identity_bound() {
        let codec = StatementTicketCodec::try_new(
            ring(b"active-ticket-key-material-00000001", &[]),
            Duration::from_mins(5),
            "lake-query",
        )
        .unwrap();
        let alice = principal("alice@example", "alpha");
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let sql = "SELECT * FROM lake.alpha.episodes";

        let ticket = codec.seal_at(sql, &alice, now).unwrap();

        for forbidden in [sql.as_bytes(), b"alice@example", b"alpha"] {
            assert!(
                !ticket
                    .windows(forbidden.len())
                    .any(|window| window == forbidden),
                "ticket leaks authenticated plaintext"
            );
        }
        assert_eq!(codec.open_at(&ticket, &alice, now).unwrap(), sql);
        assert!(matches!(
            codec.open_at(&ticket, &principal("bob@example", "alpha"), now),
            Err(QueryTicketError::Invalid)
        ));
        assert!(matches!(
            codec.open_at(&ticket, &principal("alice@example", "beta"), now),
            Err(QueryTicketError::Invalid)
        ));
    }

    #[test]
    fn statement_ticket_roundtrips_bounded_table_snapshots() {
        let codec = StatementTicketCodec::try_new(
            ring(b"snapshot-ticket-key-material-00000001", &[]),
            Duration::from_mins(5),
            "lake-query",
        )
        .unwrap();
        let alice = principal("alice@example", "alpha");
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let snapshot = StatementTableSnapshot {
            namespace:      "alpha".to_owned(),
            table:          "episodes".to_owned(),
            engine:         "lance".to_owned(),
            location:       "s3://lake/alpha/episodes/0198".to_owned(),
            incarnation_id: "0198f73b-12b0-7d20-b8ab-8195ce8bfe73".to_owned(),
            version:        41,
        };
        let statement = StatementTicket {
            sql:       "SELECT * FROM lake.alpha.episodes".to_owned(),
            snapshots: vec![snapshot.clone()],
        };

        let encrypted = codec
            .seal_statement_at(&statement, &alice, now)
            .expect("seal exact table snapshot");
        assert_eq!(
            codec
                .open_statement_at(&encrypted, &alice, now)
                .expect("open exact table snapshot"),
            statement
        );
        for forbidden in [
            snapshot.location.as_bytes(),
            snapshot.incarnation_id.as_bytes(),
        ] {
            assert!(
                !encrypted
                    .windows(forbidden.len())
                    .any(|window| window == forbidden),
                "snapshot identity must remain encrypted"
            );
        }

        let unbounded = StatementTicket {
            sql:       "SELECT 1".to_owned(),
            snapshots: vec![snapshot; MAX_TABLE_SNAPSHOTS + 1],
        };
        assert!(matches!(
            codec.seal_statement_at(&unbounded, &alice, now),
            Err(QueryTicketError::Invalid)
        ));
        let duplicate = StatementTicket {
            sql:       "SELECT 1".to_owned(),
            snapshots: vec![unbounded.snapshots[0].clone(); 2],
        };
        assert!(matches!(
            codec.seal_statement_at(&duplicate, &alice, now),
            Err(QueryTicketError::Invalid)
        ));
        let mut oversized = unbounded.snapshots[0].clone();
        oversized.location = "x".repeat(MAX_LOCATION_BYTES + 1);
        assert!(matches!(
            codec.seal_statement_at(
                &StatementTicket {
                    sql:       "SELECT 1".to_owned(),
                    snapshots: vec![oversized],
                },
                &alice,
                now
            ),
            Err(QueryTicketError::Invalid)
        ));
    }

    #[test]
    fn legacy_statement_ticket_fails_closed_after_snapshot_upgrade() {
        let codec = StatementTicketCodec::try_new(
            ring(b"legacy-ticket-key-material-0000000001", &[]),
            Duration::from_mins(5),
            "lake-query",
        )
        .unwrap();
        let alice = principal("alice@example", "alpha");
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let legacy = codec
            .seal_statement_at_version(
                &StatementTicket {
                    sql:       "SELECT * FROM lake.alpha.episodes".to_owned(),
                    snapshots: Vec::new(),
                },
                &alice,
                now,
                1,
            )
            .expect("construct a cryptographically valid v1 ticket");

        assert!(matches!(
            codec.open_statement_at(&legacy, &alice, now),
            Err(QueryTicketError::Invalid)
        ));
    }

    #[test]
    fn statement_ticket_rejects_tamper_time_audience_and_unknown_key() {
        let active = b"active-ticket-key-material-00000001";
        let codec =
            StatementTicketCodec::try_new(ring(active, &[]), Duration::from_mins(1), "lake-query")
                .unwrap();
        let alice = principal("alice@example", "alpha");
        let issued = UNIX_EPOCH + Duration::from_secs(10_000);
        let ticket = codec.seal_at("SELECT 1", &alice, issued).unwrap();

        let mut tampered = ticket.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(matches!(
            codec.open_at(&tampered, &alice, issued),
            Err(QueryTicketError::Invalid)
        ));
        assert!(matches!(
            codec.open_at(&ticket, &alice, issued + Duration::from_secs(61)),
            Err(QueryTicketError::Invalid)
        ));
        assert!(matches!(
            codec.open_at(&ticket, &alice, issued - Duration::from_secs(31)),
            Err(QueryTicketError::Invalid)
        ));

        let wrong_audience = StatementTicketCodec::try_new(
            ring(active, &[]),
            Duration::from_mins(1),
            "some-other-service",
        )
        .unwrap();
        assert!(matches!(
            wrong_audience.open_at(&ticket, &alice, issued),
            Err(QueryTicketError::Invalid)
        ));
        let unknown_key = StatementTicketCodec::try_new(
            ring(b"different-ticket-key-material-000001", &[]),
            Duration::from_mins(1),
            "lake-query",
        )
        .unwrap();
        assert!(matches!(
            unknown_key.open_at(&ticket, &alice, issued),
            Err(QueryTicketError::Invalid)
        ));
    }

    #[test]
    fn statement_ticket_rotation_preserves_only_configured_old_keys() {
        let old = b"old-ticket-key-material-000000000001";
        let new = b"new-ticket-key-material-000000000001";
        let issued = UNIX_EPOCH + Duration::from_secs(10_000);
        let alice = principal("alice@example", "alpha");
        let old_codec =
            StatementTicketCodec::try_new(ring(old, &[]), Duration::from_mins(5), "lake-query")
                .unwrap();
        let old_ticket = old_codec.seal_at("SELECT 1", &alice, issued).unwrap();
        let rotated =
            StatementTicketCodec::try_new(ring(new, &[old]), Duration::from_mins(5), "lake-query")
                .unwrap();

        assert_eq!(
            rotated.open_at(&old_ticket, &alice, issued).unwrap(),
            "SELECT 1"
        );
        let new_ticket = rotated.seal_at("SELECT 2", &alice, issued).unwrap();
        assert!(matches!(
            old_codec.open_at(&new_ticket, &alice, issued),
            Err(QueryTicketError::Invalid)
        ));
    }

    #[test]
    fn verifier_ttl_change_does_not_revoke_unexpired_ticket() {
        let secret = b"stable-ticket-key-material-00000000001";
        let issued = UNIX_EPOCH + Duration::from_secs(10_000);
        let alice = principal("alice@example", "alpha");
        let old =
            StatementTicketCodec::try_new(ring(secret, &[]), Duration::from_mins(5), "lake-query")
                .unwrap();
        let ticket = old.seal_at("SELECT 1", &alice, issued).unwrap();
        let reconfigured =
            StatementTicketCodec::try_new(ring(secret, &[]), Duration::from_mins(1), "lake-query")
                .unwrap();

        assert_eq!(
            reconfigured
                .open_at(&ticket, &alice, issued + Duration::from_secs(30))
                .unwrap(),
            "SELECT 1"
        );
    }

    #[test]
    fn statement_ticket_nonce_counter_exhaustion_fails_closed() {
        let codec = StatementTicketCodec::try_new(
            ring(b"nonce-counter-ticket-key-material-0001", &[]),
            Duration::from_mins(5),
            "lake-query",
        )
        .unwrap();
        codec.nonce_counter.store(u32::MAX, Ordering::Relaxed);

        assert!(matches!(
            codec.seal_at(
                "SELECT 1",
                &principal("alice@example", "alpha"),
                UNIX_EPOCH + Duration::from_secs(10_000)
            ),
            Err(QueryTicketError::Invalid)
        ));
    }
}
