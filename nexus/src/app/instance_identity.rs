// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! POC: verify a VM-instance attestation and mint an OIDC instance-identity
//! token (JWT).
//!
//! The verification logic is ported from sprue's `verify_instance_attestation`
//! and uses `dice_verifier` (already a workspace dependency, used by
//! sled-agent's RoT client). The wire types live in `nexus_internal_api` (so
//! they can also be the API body types) and mirror `vm-attest`'s serde shape.
//!
//! NOTE: POC-quality. Intentional shortcuts:
//!   - signing key is loaded from config (not RoT/KMS backed);
//!   - reference-measurement appraisal is skipped when no corpus is configured;
//!   - no JWKS/discovery endpoint (the verifier is given the pubkey directly);
//!   - the issued nonce is not persisted/replay-tracked;
//!   - `project`/`silo` claims are taken from the attested instance config
//!     rather than cross-checked against Nexus's database.

use chrono::Utc;
use dice_verifier::{Attestation, Log, MeasurementSet, Nonce};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use nexus_lockstep_api::VmInstanceAttestation;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;
use x509_cert::{Certificate, der::Decode, der::asn1::Utf8StringRef};

/// OID 2.5.4.10 (id-at-organizationName).
const ORGANIZATION_NAME_OID: &str = "2.5.4.10";

/// Organization (`O=`) every Oxide attestation cert chain must carry. Not
/// configurable: it's a fixed property of the Oxide attestation PKI, so it's a
/// constant rather than a stored/configured field.
pub const INSTANCE_IDENTITY_ORGANIZATION: &str = "Oxide Computer Company";

/// RoT identifiers as serialized by `vm-attest` on the wire (PascalCase).
const ROT_OXIDE_PLATFORM: &str = "OxidePlatform";
const ROT_OXIDE_INSTANCE: &str = "OxideInstance";

/// Only the fields we need from the OxideInstance log (a JSON `VmInstanceConf`).
/// Unknown fields are ignored, so this tolerates both current and older
/// `vm-attest` shapes; `project`/`silo` are optional for the same reason.
#[derive(Debug, Deserialize)]
struct VmInstanceConf {
    uuid: Uuid,
    #[serde(default)]
    project: Option<Uuid>,
    #[serde(default)]
    silo: Option<Uuid>,
}

/// Claims for the minted instance-identity token. Provider-neutral: a stable
/// subject plus the attested instance facts as first-class claims.
#[derive(Debug, Serialize)]
struct VmClaims {
    iss: String,
    sub: String,
    aud: String,
    exp: i64,
    iat: i64,
    jti: String,
    instance: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    project: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    silo: Option<Uuid>,
}

#[derive(Debug, Error)]
pub enum InstanceIdentityError {
    #[error("failed to load signing key or root certs")]
    LoadKeyMaterial,
    #[error("failed to decode nonce as hex")]
    NonceFormat(#[from] hex::FromHexError),
    #[error("nonce is not 32 bytes")]
    NonceInvalid,
    #[error("failed to parse a certificate from the attestation chain")]
    ParseCertificate,
    #[error("attestation cert chain is empty")]
    EmptyCertChain,
    #[error("failed to verify certificate chain: {0}")]
    VerifyChain(String),
    #[error("certificate chain is missing an organization name")]
    MissingOrganization,
    #[error("certificate chain has the wrong organization")]
    IncorrectOrganization,
    #[error("attestation is missing the OxidePlatform measurement log")]
    MissingPlatformLog,
    #[error("attestation is missing the OxideInstance measurement log")]
    MissingInstanceLog,
    #[error("failed to deserialize hubpack data: {0}")]
    Hubpack(String),
    #[error("failed to verify attestation: {0}")]
    VerifyAttestation(String),
    #[error("failed to verify measurements: {0}")]
    VerifyMeasurements(String),
    #[error("OxideInstance log is not valid JSON")]
    ParseInstanceConf(#[source] serde_json::Error),
    #[error(
        "attested instance id {attested} does not match requesting instance {expected}"
    )]
    WrongInstanceId { attested: Uuid, expected: Uuid },
    #[error("failed to sign token: {0}")]
    Sign(#[from] jsonwebtoken::errors::Error),
}

/// Holds the trust anchors + signing key needed to verify an attestation and
/// mint a token. Built once at Nexus startup from config.
#[derive(Clone)]
pub struct InstanceIdentitySigner {
    organization: String,
    root_certs: Arc<Vec<Certificate>>,
    /// Optional: when present, the OxidePlatform log is appraised against these.
    ref_measurements: Option<Arc<dice_verifier::ReferenceMeasurements>>,
    issuer: String,
    audience: String,
    token_ttl_secs: i64,
}

impl InstanceIdentitySigner {
    /// Build a signer from the per-request inputs: the trust anchor (root cert
    /// PEM, read from the configured file) plus the minting policy (`iss`,
    /// `aud`, `ttl`) that the caller loads from the active DB signing-key row.
    ///
    /// This is constructed per request (no caching, by design for the POC), so
    /// the feature is "on" whenever a live signing key exists in the database
    /// and the root cert file is staged — there is no separate config gate.
    ///
    /// `ref_measurements` is always `None` here: the POC skips RIM appraisal,
    /// and the production reference-measurement source is the update/TUF/DB
    /// path (a separate gap), not a config file.
    pub fn new(
        root_cert_pem: &[u8],
        organization: String,
        issuer: String,
        audience: String,
        token_ttl_secs: i64,
    ) -> Result<Self, InstanceIdentityError> {
        let root_certs = Certificate::load_pem_chain(root_cert_pem)
            .map_err(|_| InstanceIdentityError::ParseCertificate)?;

        Ok(Self {
            organization,
            root_certs: Arc::new(root_certs),
            ref_measurements: None,
            issuer,
            audience,
            token_ttl_secs,
        })
    }

    /// Verify the attestation against our trust anchors and, on success, mint a
    /// signed instance-identity token bound to the attested instance.
    ///
    /// SECURITY (POC gap — no replay protection): `nonce_hex` is taken straight
    /// from the request and used only to reconstruct the qualifying data the
    /// attestation is checked against. We do NOT persist the nonces handed out
    /// by `generate_nonce`, nor verify that this nonce was one we issued or that
    /// it has not been used before. Consequently a captured `(nonce,
    /// attestation)` pair can be replayed to mint additional tokens until the
    /// attestation's own freshness lapses. A real implementation must record
    /// issued nonces as single-use and time-bounded, and reject reused or
    /// unknown ones here.
    ///
    /// The signing key (PEM-encoded RSA private key) and its `kid` are supplied
    /// by the caller, which loads the active key from the database per request.
    pub fn verify_and_mint(
        &self,
        instance: Uuid,
        nonce_hex: &str,
        attestation: &VmInstanceAttestation,
        signing_key_pem: &[u8],
        kid: &str,
    ) -> Result<String, InstanceIdentityError> {
        let nonce: [u8; 32] = hex::decode(nonce_hex)?
            .try_into()
            .map_err(|_| InstanceIdentityError::NonceInvalid)?;

        // Parse the DER cert chain.
        let mut cert_chain = Vec::new();
        for der in &attestation.cert_chain {
            cert_chain.push(
                Certificate::from_der(der)
                    .map_err(|_| InstanceIdentityError::ParseCertificate)?,
            );
        }
        if cert_chain.is_empty() {
            return Err(InstanceIdentityError::EmptyCertChain);
        }

        // 1. Verify the cert chain to a known Oxide root.
        let verified_root = dice_verifier::verify_cert_chain(
            &cert_chain,
            Some(&self.root_certs),
        )
        .map_err(|e| InstanceIdentityError::VerifyChain(e.to_string()))?;
        let organization = cert_organization(verified_root)
            .ok_or(InstanceIdentityError::MissingOrganization)?;
        if organization.as_str() != self.organization {
            return Err(InstanceIdentityError::IncorrectOrganization);
        }

        // 2. Reconstruct the qualifying data the VM-instance RoT signed over:
        //    sha256(OxideInstance log || nonce).
        let mut qdata = Sha256::new();
        for log in &attestation.measurement_logs {
            if log.rot.as_str() == ROT_OXIDE_INSTANCE {
                qdata.update(&log.data);
            }
        }
        qdata.update(nonce);
        let qdigest: [u8; 32] = qdata.finalize().into();
        let qualifying_data = Nonce::N32(qdigest.into());

        // 3. Verify the platform RoT attestation signature.
        let platform_log_bytes = attestation
            .measurement_logs
            .iter()
            .find(|l| l.rot.as_str() == ROT_OXIDE_PLATFORM)
            .map(|l| &l.data)
            .ok_or(InstanceIdentityError::MissingPlatformLog)?;
        let (platform_log, _): (Log, _) =
            hubpack::deserialize(platform_log_bytes)
                .map_err(|e| InstanceIdentityError::Hubpack(e.to_string()))?;
        let (ox_attest, _): (Attestation, _) =
            hubpack::deserialize(&attestation.attestation)
                .map_err(|e| InstanceIdentityError::Hubpack(e.to_string()))?;
        dice_verifier::verify_attestation(
            &cert_chain[0],
            &ox_attest,
            &platform_log,
            &qualifying_data,
        )
        .map_err(|e| InstanceIdentityError::VerifyAttestation(e.to_string()))?;

        // 4. (optional) Appraise the platform measurement log against RIMs.
        if let Some(ref_measurements) = &self.ref_measurements {
            let measurements =
                MeasurementSet::from_artifacts(&cert_chain, &platform_log)
                    .map_err(|e| {
                        InstanceIdentityError::VerifyMeasurements(e.to_string())
                    })?;
            dice_verifier::verify_measurements(&measurements, ref_measurements)
                .map_err(|e| {
                    InstanceIdentityError::VerifyMeasurements(e.to_string())
                })?;
        }

        // 5. Cross-check the attested instance id against the requesting one,
        //    and lift the attested project/silo into the claims.
        let instance_log = attestation
            .measurement_logs
            .iter()
            .find(|l| l.rot.as_str() == ROT_OXIDE_INSTANCE)
            .ok_or(InstanceIdentityError::MissingInstanceLog)?;
        let conf: VmInstanceConf = serde_json::from_slice(&instance_log.data)
            .map_err(InstanceIdentityError::ParseInstanceConf)?;
        if conf.uuid != instance {
            return Err(InstanceIdentityError::WrongInstanceId {
                attested: conf.uuid,
                expected: instance,
            });
        }

        // 6. Mint the token, bound to the attested instance.
        self.mint(instance, conf.project, conf.silo, signing_key_pem, kid)
    }

    fn mint(
        &self,
        instance: Uuid,
        project: Option<Uuid>,
        silo: Option<Uuid>,
        signing_key_pem: &[u8],
        kid: &str,
    ) -> Result<String, InstanceIdentityError> {
        let encoding_key = EncodingKey::from_rsa_pem(signing_key_pem)
            .map_err(|_| InstanceIdentityError::LoadKeyMaterial)?;
        let now = Utc::now().timestamp();
        let claims = VmClaims {
            iss: self.issuer.clone(),
            sub: format!("instance:{instance}"),
            aud: self.audience.clone(),
            iat: now,
            exp: now + self.token_ttl_secs,
            jti: Uuid::new_v4().to_string(),
            instance,
            project,
            silo,
        };
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        Ok(jsonwebtoken::encode(&header, &claims, &encoding_key)?)
    }
}

/// 32 bytes of randomness, hex-encoded. The caller is responsible for any
/// persistence/replay-protection (not done in this POC).
pub fn generate_nonce() -> String {
    let nonce: [u8; 32] = rand::random();
    hex::encode(nonce)
}

/// Extract the organizationName (O=) from a cert's issuer.
fn cert_organization(cert: &Certificate) -> Option<Utf8StringRef<'_>> {
    let org_oid =
        x509_cert::der::asn1::ObjectIdentifier::new(ORGANIZATION_NAME_OID)
            .ok()?;
    for rdn in cert.tbs_certificate.issuer.0.iter() {
        for atav in rdn.0.iter() {
            if atav.oid == org_oid {
                return Utf8StringRef::try_from(&atav.value).ok();
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use dice_verifier::Attest;
    use hubpack::SerializedSize;
    use nexus_lockstep_api::MeasurementLog;
    use sprockets_tls_test_utils::{
        OutputFileExistsBehavior, alias_prefix, cert_path, certlist_path,
        generate_config, private_key_path, root_prefix,
    };
    use x509_cert::der::Encode;

    // A throwaway 2048-bit RSA key used only to sign test JWTs.
    const TEST_SIGNING_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDAR8XxsAoIPq2j
MfdZx7vs4qF5uzj5D9cNXADyYcuPsVHFKAFImNvDC9WRYyRybGP57CvUAyDYGMgE
3lgO8wS0Uz+jKfBEsjEkZft5C/c9b8ej/0LZfr2NXHAWy+2SUUjHYTjRfpVK2tzp
MmEKz2EMdMWnmdlwh0UnWK5n4RMEnlAI5zdefANZM3pAfEx3w36g0yVfA8CADuY/
vS+gg0/U1O7IsBC0gBrxMOMQ6hEkj9GoIp/qAikcAuyCwd5W87df68V/QEVbzZlF
uQl72ihq2+K4/JkapkuFq767HAR2Haes34TowKRcCcKHRkD8BVqZBYCQewnyKxMY
Y8sy8uUdAgMBAAECggEAd4ZmOsYmhleIEpFF5E5vuGJs5EIIuXIqSYiuof0+z9T3
MqqamalDuuxJVzYc+u/7+ejgmctUCGDnPXeFn81bWEkDnwa778ghGjI271kL2On6
XSyZPqA1boFOwC0GQlU6+42pBzk7zqtcda22e0TMXDTehT/y1auQxlOvHq3f55b3
LiHMy8YlMEnmCmD/BkFbKrFMuzHJjYy7vK5z9/O50Xx8/jlY5eBRWDjzzCIDrsGS
gBZSxZwMw2mWuURz8qXTKKErdqllCAz2GMW/iW0xaY7hz9b0hONBe7muW2I/EBNR
wgRJH6rcLw5lAfvE+n0CQrioLYFgsJDbP7I96xZKoQKBgQD/er3E9pWVgp64givh
IEmHMbZC5V0gx1BkfEdC/mwBWT69JU2DnhWftPuvYifyvBjTyi4WHF1aNfs4GgEI
UmNWUt0TimaUjlwTvHfBd5PsfYdKdBWYqgg1Sd2TiXd0Dy69KV/F73dPFa/NtcUD
7a3fZOLi1ZbRuW7lTT6qDT8bOQKBgQDArBEvaNfTU6r+VMHCcpbOmDm5ogv9vURL
s1KHgPU6+4Mzb+4ifgYLWmgJ3neEdcqXVBGQhW4mAM245zDLhExgOzKzioI7sV+P
sU9BOQK1UJhu+rdN+/ojL1MRBv8X7msLB+AfEUqD8iZTzbK8r9G+FCNaG6Dy/gCU
kdH25GlFBQKBgHqZklvk4V/AMR7mCNyeO+rO5mIv44MJYwD1ytTRd08mXr8yGYKh
6Eqd9XHyrq0Dqv5ntboSnpHd+dKwV/KAZv9UAJeSASwlLPajqOyEz83bZ4NJNdvv
LMU1z0kv3M0rRAiuxDEee6jOBXG4WvVJp/jeVyr3yMqYLCmJ6hp9BN2xAoGAKj/R
COdhpFqNfXXSgzQjc6wXxGgPXxW4uyzYZRRXMhbi/02FF5Uw+B5cAAYDiU1XHnX1
4A4SSE0Wz3wKzCX3pYZ1qL8vra1Iejb0XSYGPQIuWu+pcHVXtY10FqDFIVdLq8CP
WeCtyV85HllV32Biit7ZnbG/Sml+cRXEx/HwT40CgYAgsAuiaGSto4Va6KAvbCqd
/BoEY3yu4g/g5PbMEHZ0Le5uYC7A17oPUhijZXEfyD6ESJnsihykQkc9weScEJ/F
vkjBqPjqrfLtrInnSsGhh39QuOsvtq7DfL2cTbphHBGzwjW3h+G+LONPdjXmAU6h
avqCw+ihCp6iJ2OhES/jPQ==
-----END PRIVATE KEY-----
"#;

    // The matching RSA public key, used in-test to verify minted tokens the way
    // a relying party (AWS STS / Vault) would.
    const TEST_VERIFY_KEY_PEM: &str = r#"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwEfF8bAKCD6tozH3Wce7
7OKhebs4+Q/XDVwA8mHLj7FRxSgBSJjbwwvVkWMkcmxj+ewr1AMg2BjIBN5YDvME
tFM/oynwRLIxJGX7eQv3PW/Ho/9C2X69jVxwFsvtklFIx2E40X6VStrc6TJhCs9h
DHTFp5nZcIdFJ1iuZ+ETBJ5QCOc3XnwDWTN6QHxMd8N+oNMlXwPAgA7mP70voINP
1NTuyLAQtIAa8TDjEOoRJI/RqCKf6gIpHALsgsHeVvO3X+vFf0BFW82ZRbkJe9oo
atviuPyZGqZLhau+uxwEdh2nrN+E6MCkXAnCh0ZA/AVamQWAkHsJ8isTGGPLMvLl
HQIDAQAB
-----END PUBLIC KEY-----
"#;

    /// Generate the attestation PKI (root + alias chain) and a measurement log
    /// into `dir`, using the same `dice_verifier`/`attest_mock` tooling the rest
    /// of omicron uses for mock attestation.
    fn write_pki(dir: &camino::Utf8Path) {
        let behavior = OutputFileExistsBehavior::Overwrite;
        let doc = generate_config(1);
        doc.write_key_pairs(dir.to_path_buf(), behavior).unwrap();
        doc.write_certificates(dir.to_path_buf(), behavior).unwrap();
        doc.write_certificate_lists(dir.to_path_buf(), behavior).unwrap();

        let log_doc = attest_mock::log::Document {
            measurements: vec![attest_mock::log::Measurement {
                algorithm: "sha3-256".into(),
                digest: "be4df4e085175f3de0c8ac4837e1c2c9a34e8983209dac6b549e94154f7cdd9c".into(),
            }],
        };
        let out = attest_mock::log::mock(log_doc).unwrap();
        std::fs::write(dir.join("log.bin"), &out).unwrap();
    }

    /// Build a `VmInstanceAttestation` the way the VM-instance RoT would: the
    /// platform RoT signs over `sha256(instance_cfg || nonce)`.
    fn build_attestation(
        attest: &dice_verifier::mock::AttestMock,
        instance_cfg_json: &[u8],
        nonce: &[u8; 32],
    ) -> VmInstanceAttestation {
        let mut q = Sha256::new();
        q.update(instance_cfg_json);
        q.update(nonce);
        let qdigest: [u8; 32] = q.finalize().into();
        let qdata = Nonce::N32(qdigest.into());

        let attestation = attest.attest(&qdata).unwrap();
        let log = attest.get_measurement_log().unwrap();
        let certs = attest.get_certificates().unwrap();

        let mut att_buf = vec![0u8; Attestation::MAX_SIZE];
        let n = hubpack::serialize(&mut att_buf, &attestation).unwrap();
        let mut log_buf = vec![0u8; Log::MAX_SIZE];
        let m = hubpack::serialize(&mut log_buf, &log).unwrap();

        // The attestation cert chain excludes the self-signed root (which is the
        // separately-configured trust anchor).
        let cert_chain: Vec<Vec<u8>> = certs
            .iter()
            .filter(|c| {
                c.tbs_certificate.issuer != c.tbs_certificate.subject
            })
            .map(|c| c.to_der().unwrap())
            .collect();

        VmInstanceAttestation {
            attestation: att_buf[..n].to_vec(),
            cert_chain,
            measurement_logs: vec![
                // Use the literal wire strings vm-attest emits (not the
                // production consts) so this test independently pins the
                // expected encoding and would catch drift in those consts.
                MeasurementLog {
                    rot: "OxidePlatform".to_string(),
                    data: log_buf[..m].to_vec(),
                },
                MeasurementLog {
                    rot: "OxideInstance".to_string(),
                    data: instance_cfg_json.to_vec(),
                },
            ],
        }
    }

    /// Decode AND verify the JWT the way a relying party would: validate the
    /// RS256 signature against the public key, plus issuer/audience/expiry.
    fn verify_jwt(token: &str) -> serde_json::Value {
        use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
        let key = DecodingKey::from_rsa_pem(TEST_VERIFY_KEY_PEM.as_bytes())
            .expect("valid RSA public key");
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&["https://oxide.test"]);
        validation.set_audience(&["vault"]);
        decode::<serde_json::Value>(token, &key, &validation)
            .expect(
                "minted JWT must verify against the public key \
                 (signature + iss + aud + exp)",
            )
            .claims
    }

    #[test]
    fn verify_and_mint_roundtrip() {
        let tmp = camino_tempfile::tempdir().unwrap();
        let dir = tmp.path();
        write_pki(dir);

        let attest = dice_verifier::mock::AttestMock::load(
            certlist_path(dir.to_path_buf(), &alias_prefix(1)),
            dir.join("log.bin"),
            private_key_path(dir.to_path_buf(), &alias_prefix(1)),
        )
        .unwrap();

        let root_path = cert_path(dir.to_path_buf(), &root_prefix());

        // Match the signer's expected organization to whatever the test PKI uses.
        let root_pem = std::fs::read_to_string(&root_path).unwrap();
        let root = Certificate::load_pem_chain(root_pem.as_bytes()).unwrap();
        let organization =
            cert_organization(&root[0]).unwrap().as_str().to_string();

        // The signer is built per request from the root cert (file/trust
        // anchor) plus the minting policy (in production loaded from the active
        // DB signing-key row); the test supplies them directly here.
        let signer = InstanceIdentitySigner::new(
            root_pem.as_bytes(),
            organization,
            "https://oxide.test".into(),
            "vault".into(),
            300,
        )
        .unwrap();
        // The signing key + kid are supplied per request (in production from
        // the database); the test supplies a fixed throwaway key here.
        let signing_key_pem = TEST_SIGNING_KEY_PEM.as_bytes();
        let kid = "test-kid";

        let instance = Uuid::new_v4();
        let project = Uuid::new_v4();
        let silo = Uuid::new_v4();
        let instance_cfg_json = serde_json::to_vec(&serde_json::json!({
            "uuid": instance,
            "project": project,
            "silo": silo,
            "boot-digest": null,
        }))
        .unwrap();

        let nonce: [u8; 32] = rand::random();
        let nonce_hex = hex::encode(nonce);
        let attestation =
            build_attestation(&attest, &instance_cfg_json, &nonce);

        // Happy path: a valid attestation mints a token bound to the instance.
        let token = signer
            .verify_and_mint(
                instance,
                &nonce_hex,
                &attestation,
                signing_key_pem,
                kid,
            )
            .expect("verification + mint should succeed");
        let claims = verify_jwt(&token);
        assert_eq!(claims["sub"], format!("instance:{instance}"));
        assert_eq!(claims["instance"], instance.to_string());
        assert_eq!(claims["project"], project.to_string());
        assert_eq!(claims["silo"], silo.to_string());
        assert_eq!(claims["iss"], "https://oxide.test");
        assert_eq!(claims["aud"], "vault");

        // A token requested for a different instance id is rejected.
        let other = Uuid::new_v4();
        let err = signer
            .verify_and_mint(
                other,
                &nonce_hex,
                &attestation,
                signing_key_pem,
                kid,
            )
            .unwrap_err();
        assert!(
            matches!(err, InstanceIdentityError::WrongInstanceId { .. }),
            "expected WrongInstanceId, got {err:?}"
        );

        // A nonce that doesn't match the one the RoT signed over fails the
        // attestation signature check.
        let bad_nonce = hex::encode([0u8; 32]);
        let err = signer
            .verify_and_mint(
                instance,
                &bad_nonce,
                &attestation,
                signing_key_pem,
                kid,
            )
            .unwrap_err();
        assert!(
            matches!(err, InstanceIdentityError::VerifyAttestation(_)),
            "expected VerifyAttestation, got {err:?}"
        );
    }
}
