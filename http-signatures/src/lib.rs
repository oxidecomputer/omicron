// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::Signer;
use ed25519_dalek::Verifier;
use sha2::Digest;
use sha2::Sha256;
use sfv::BareItem;
use sfv::Dictionary;
use sfv::InnerList;
use sfv::Item;
use sfv::Key;
use sfv::ListEntry;
use sfv::ListSerializer;
use sfv::Parameters;
use sfv::Parser;

pub use ed25519_dalek::SigningKey;
pub use ed25519_dalek::VerifyingKey;

pub const SIGNATURE_INPUT_HEADER: &str = "signature-input";
pub const SIGNATURE_HEADER: &str = "signature";
pub const CONTENT_DIGEST_HEADER: &str = "content-digest";

pub const LABEL: &str = "sig1";
pub const ALGORITHM: &str = "ed25519";
pub const COVERED_COMPONENTS: [&str; 5] =
    ["@method", "@authority", "@path", "@query", "content-digest"];

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("malformed {header} header: {message}")]
    Malformed { header: &'static str, message: String },
    #[error("unsupported signature: {0}")]
    Unsupported(String),
    #[error("signature verification failed")]
    BadSignature,
    #[error("signature is valid from {created} until {deadline}")]
    Expired { created: i64, deadline: i64 },
    #[error("content digest does not match the request body")]
    DigestMismatch,
    #[error("invalid key: {0}")]
    InvalidKey(String),
}

fn malformed(header: &'static str, message: impl Into<String>) -> Error {
    Error::Malformed { header, message: message.into() }
}

pub fn content_digest(body: &[u8]) -> String {
    let mut hasher = DigestHasher::new();
    hasher.update(body);
    hasher.finish()
}

#[derive(Clone, Default)]
pub struct DigestHasher(Sha256);

impl DigestHasher {
    pub fn new() -> Self {
        Self(Sha256::new())
    }

    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    pub fn finish(self) -> String {
        format_content_digest(&self.0.finalize().into())
    }

    pub fn finish_bytes(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

fn format_content_digest(digest: &[u8; 32]) -> String {
    format!("sha-256=:{}:", BASE64.encode(digest))
}

pub fn parse_content_digest(value: &str) -> Result<[u8; 32], Error> {
    let dict: Dictionary = Parser::new(value)
        .parse()
        .map_err(|e| malformed(CONTENT_DIGEST_HEADER, e.to_string()))?;
    let entry = dict.get("sha-256").ok_or_else(|| {
        Error::Unsupported("content-digest must include sha-256".to_string())
    })?;
    let bytes = match entry {
        ListEntry::Item(Item { bare_item: BareItem::ByteSequence(b), .. }) => b,
        _ => {
            return Err(malformed(
                CONTENT_DIGEST_HEADER,
                "sha-256 value must be a byte sequence",
            ));
        }
    };
    bytes.as_slice().try_into().map_err(|_| {
        malformed(CONTENT_DIGEST_HEADER, "sha-256 digest must be 32 bytes")
    })
}

pub fn verify_content_digest(value: &str, body: &[u8]) -> Result<(), Error> {
    let expected = parse_content_digest(value)?;
    let actual: [u8; 32] = Sha256::digest(body).into();
    if expected == actual { Ok(()) } else { Err(Error::DigestMismatch) }
}

#[derive(Clone, Copy, Debug)]
pub struct Components<'a> {
    pub method: &'a str,
    pub authority: &'a str,
    pub path: &'a str,
    pub query: Option<&'a str>,
    pub content_digest: &'a str,
}

impl Components<'_> {
    fn values(&self) -> [(&'static str, String); 5] {
        let query = match self.query {
            Some(q) => format!("?{q}"),
            None => "?".to_string(),
        };
        [
            ("@method", self.method.to_ascii_uppercase()),
            ("@authority", self.authority.to_ascii_lowercase()),
            ("@path", self.path.to_string()),
            ("@query", query),
            ("content-digest", self.content_digest.trim().to_string()),
        ]
    }
}

pub fn signature_base(components: &[(&str, &str)], params: &str) -> String {
    let mut base = String::new();
    for (name, value) in components {
        base.push('"');
        base.push_str(name);
        base.push_str("\": ");
        base.push_str(value);
        base.push('\n');
    }
    base.push_str("\"@signature-params\": ");
    base.push_str(params);
    base
}

fn serialize_inner_list(list: &InnerList) -> String {
    let entry = ListEntry::InnerList(list.clone());
    let mut ser = ListSerializer::new();
    ser.members(std::iter::once(&entry));
    ser.finish().unwrap_or_default()
}

fn key(name: &str) -> Key {
    Key::from_string(name.to_string()).expect("static key is valid")
}

fn profile_params(
    key_id: &str,
    created: i64,
    expires: Option<i64>,
) -> InnerList {
    let items = COVERED_COMPONENTS
        .iter()
        .map(|c| {
            Item::new(
                sfv::String::from_string(c.to_string())
                    .expect("component name is a valid sf-string"),
            )
        })
        .collect();
    let mut params = Parameters::new();
    params.insert(
        key("created"),
        BareItem::Integer(
            sfv::Integer::try_from(created).expect("timestamp fits"),
        ),
    );
    if let Some(expires) = expires {
        params.insert(
            key("expires"),
            BareItem::Integer(
                sfv::Integer::try_from(expires).expect("timestamp fits"),
            ),
        );
    }
    params.insert(
        key("keyid"),
        BareItem::String(
            sfv::String::from_string(key_id.to_string())
                .expect("key id is a valid sf-string"),
        ),
    );
    params.insert(
        key("alg"),
        BareItem::String(
            sfv::String::from_string(ALGORITHM.to_string())
                .expect("valid sf-string"),
        ),
    );
    InnerList::with_params(items, params)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignatureHeaders {
    pub signature_input: String,
    pub signature: String,
}

pub fn sign(
    signing_key: &SigningKey,
    key_id: &str,
    created: i64,
    expires: Option<i64>,
    components: &Components<'_>,
) -> SignatureHeaders {
    let params =
        serialize_inner_list(&profile_params(key_id, created, expires));
    let values = components.values();
    let pairs: Vec<(&str, &str)> =
        values.iter().map(|(n, v)| (*n, v.as_str())).collect();
    let base = signature_base(&pairs, &params);
    let signature = signing_key.sign(base.as_bytes());
    SignatureHeaders {
        signature_input: format!("{LABEL}={params}"),
        signature: format!(
            "{LABEL}=:{}:",
            BASE64.encode(signature.to_bytes())
        ),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedSignature {
    pub label: String,
    pub key_id: String,
    pub created: i64,
    pub expires: Option<i64>,
    params: String,
    signature: [u8; 64],
}

pub fn parse(
    signature_input: &str,
    signature: &str,
) -> Result<ParsedSignature, Error> {
    let inputs: Dictionary = Parser::new(signature_input)
        .parse()
        .map_err(|e| malformed(SIGNATURE_INPUT_HEADER, e.to_string()))?;
    let signatures: Dictionary = Parser::new(signature)
        .parse()
        .map_err(|e| malformed(SIGNATURE_HEADER, e.to_string()))?;
    if inputs.len() != 1 || signatures.len() != 1 {
        return Err(Error::Unsupported(
            "exactly one signature is required".to_string(),
        ));
    }
    let (label, input) = inputs.first().expect("length checked");
    let list = match input {
        ListEntry::InnerList(list) => list,
        ListEntry::Item(_) => {
            return Err(malformed(
                SIGNATURE_INPUT_HEADER,
                "member must be an inner list",
            ));
        }
    };

    let covered: Vec<&str> = list
        .items
        .iter()
        .map(|item| match &item.bare_item {
            BareItem::String(s) if item.params.is_empty() => Ok(s.as_str()),
            _ => Err(Error::Unsupported(
                "covered components must be plain component names".to_string(),
            )),
        })
        .collect::<Result<_, _>>()?;
    if covered != COVERED_COMPONENTS {
        return Err(Error::Unsupported(format!(
            "covered components must be exactly {:?}",
            COVERED_COMPONENTS
        )));
    }

    let mut key_id = None;
    let mut created = None;
    let mut expires = None;
    for (name, value) in &list.params {
        match (name.as_str(), value) {
            ("keyid", BareItem::String(s)) => key_id = Some(s.as_str().to_string()),
            ("created", BareItem::Integer(i)) => created = Some(i64::from(*i)),
            ("expires", BareItem::Integer(i)) => expires = Some(i64::from(*i)),
            ("alg", BareItem::String(s)) if s.as_str() == ALGORITHM => {}
            ("alg", _) => {
                return Err(Error::Unsupported(
                    "only ed25519 is supported".to_string(),
                ));
            }
            (other, _) => {
                return Err(Error::Unsupported(format!(
                    "unsupported signature parameter {other:?}"
                )));
            }
        }
    }
    let key_id = key_id
        .ok_or_else(|| malformed(SIGNATURE_INPUT_HEADER, "missing keyid"))?;
    let created = created
        .ok_or_else(|| malformed(SIGNATURE_INPUT_HEADER, "missing created"))?;

    let signature = match signatures.get(label) {
        Some(ListEntry::Item(Item {
            bare_item: BareItem::ByteSequence(b), ..
        })) => <[u8; 64]>::try_from(b.as_slice()).map_err(|_| {
            malformed(SIGNATURE_HEADER, "signature must be 64 bytes")
        })?,
        Some(_) => {
            return Err(malformed(
                SIGNATURE_HEADER,
                "signature must be a byte sequence",
            ));
        }
        None => {
            return Err(malformed(
                SIGNATURE_HEADER,
                format!("no signature with label {:?}", label.as_str()),
            ));
        }
    };

    Ok(ParsedSignature {
        label: label.as_str().to_string(),
        key_id,
        created,
        expires,
        params: serialize_inner_list(list),
        signature,
    })
}

impl ParsedSignature {
    pub fn check_window(
        &self,
        now: i64,
        max_age_seconds: i64,
    ) -> Result<(), Error> {
        let ceiling = self.created + max_age_seconds;
        let deadline = self.expires.map_or(ceiling, |e| e.min(ceiling));
        if now + max_age_seconds < self.created || now > deadline {
            Err(Error::Expired { created: self.created, deadline })
        } else {
            Ok(())
        }
    }

    pub fn verify(
        &self,
        verifying_key: &VerifyingKey,
        components: &Components<'_>,
    ) -> Result<(), Error> {
        let values = components.values();
        let pairs: Vec<(&str, &str)> =
            values.iter().map(|(n, v)| (*n, v.as_str())).collect();
        let base = signature_base(&pairs, &self.params);
        let signature = ed25519_dalek::Signature::from_bytes(&self.signature);
        verifying_key
            .verify(base.as_bytes(), &signature)
            .map_err(|_| Error::BadSignature)
    }
}

pub fn verifying_key_from_pem(pem: &str) -> Result<VerifyingKey, Error> {
    use ed25519_dalek::pkcs8::DecodePublicKey;
    VerifyingKey::from_public_key_pem(pem.trim())
        .map_err(|e| Error::InvalidKey(e.to_string()))
}

pub fn verifying_key_from_der(der: &[u8]) -> Result<VerifyingKey, Error> {
    use ed25519_dalek::pkcs8::DecodePublicKey;
    VerifyingKey::from_public_key_der(der)
        .map_err(|e| Error::InvalidKey(e.to_string()))
}

pub fn verifying_key_to_der(key: &VerifyingKey) -> Vec<u8> {
    use ed25519_dalek::pkcs8::EncodePublicKey;
    key.to_public_key_der().expect("ed25519 key encodes").into_vec()
}

pub fn verifying_key_to_pem(key: &VerifyingKey) -> String {
    use ed25519_dalek::pkcs8::EncodePublicKey;
    key.to_public_key_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
        .expect("ed25519 key encodes")
}

pub fn signing_key_from_pem(pem: &str) -> Result<SigningKey, Error> {
    use ed25519_dalek::pkcs8::DecodePrivateKey;
    SigningKey::from_pkcs8_pem(pem.trim())
        .map_err(|e| Error::InvalidKey(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const RFC_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJ+DYvh6SEqVTm50DFtMDoQikTmiCqirVv9mWG9qfSnF
-----END PRIVATE KEY-----";
    const RFC_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAJrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=
-----END PUBLIC KEY-----";

    #[test]
    fn rfc9421_b26_ed25519_vector() {
        let input: Dictionary = Parser::new(
            r#"sig-b26=("date" "@method" "@path" "@authority" "content-type" "content-length");created=1618884473;keyid="test-key-ed25519""#,
        )
        .parse()
        .unwrap();
        let ListEntry::InnerList(list) = &input["sig-b26"] else { panic!() };
        let params = serialize_inner_list(list);
        let base = signature_base(
            &[
                ("date", "Tue, 20 Apr 2021 02:07:55 GMT"),
                ("@method", "POST"),
                ("@path", "/foo"),
                ("@authority", "example.com"),
                ("content-type", "application/json"),
                ("content-length", "18"),
            ],
            &params,
        );
        let key = signing_key_from_pem(RFC_PRIVATE_KEY).unwrap();
        let signature = key.sign(base.as_bytes());
        assert_eq!(
            BASE64.encode(signature.to_bytes()),
            "wqcAqbmYJ2ji2glfAMaRy4gruYYnx2nEFN2HN6jrnDnQCK1u02Gb04v9EDgwUPiu4A0w6vuQv5lIp5WPpBKRCw=="
        );
        let public = verifying_key_from_pem(RFC_PUBLIC_KEY).unwrap();
        assert_eq!(public, key.verifying_key());
    }

    #[test]
    fn rfc_example_digest() {
        assert_eq!(
            content_digest(
                br#"{"name":"example-name","description":"example-description","policy":"allow","failure_domain":"sled"}"#
            ),
            "sha-256=:Ig/vp+/Y3Q8h+6943cRCscJNdW26OIR2ad7EKTFTw5Y=:"
        );
        assert_eq!(
            content_digest(b""),
            "sha-256=:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=:"
        );
    }

    fn components<'a>(digest: &'a str, query: Option<&'a str>) -> Components<'a> {
        Components {
            method: "POST",
            authority: "localhost:12220",
            path: "/v1/anti-affinity-groups",
            query,
            content_digest: digest,
        }
    }

    #[test]
    fn round_trip() {
        let key = signing_key_from_pem(RFC_PRIVATE_KEY).unwrap();
        let digest = content_digest(b"{}");
        let c = components(&digest, Some("project=example-project"));
        let headers = sign(&key, "key-id", 1763300000, None, &c);
        assert_eq!(
            headers.signature_input,
            r#"sig1=("@method" "@authority" "@path" "@query" "content-digest");created=1763300000;keyid="key-id";alg="ed25519""#
        );
        let parsed = parse(&headers.signature_input, &headers.signature).unwrap();
        assert_eq!(parsed.key_id, "key-id");
        parsed.verify(&key.verifying_key(), &c).unwrap();
        parsed.check_window(1763300030, 60).unwrap();
        assert!(parsed.check_window(1763300061, 60).is_err());
        assert!(parsed.check_window(1763299939, 60).is_err());

        let other_query = components(&digest, None);
        assert_eq!(
            parsed.verify(&key.verifying_key(), &other_query),
            Err(Error::BadSignature)
        );
        let other_digest = content_digest(b"{ }");
        assert_eq!(
            parsed.verify(
                &key.verifying_key(),
                &components(&other_digest, Some("project=example-project"))
            ),
            Err(Error::BadSignature)
        );
    }

    #[test]
    fn expires() {
        let key = signing_key_from_pem(RFC_PRIVATE_KEY).unwrap();
        let digest = content_digest(b"{}");
        let c = components(&digest, None);
        let headers = sign(&key, "key-id", 1763300000, Some(1763300030), &c);
        assert_eq!(
            headers.signature_input,
            r#"sig1=("@method" "@authority" "@path" "@query" "content-digest");created=1763300000;expires=1763300030;keyid="key-id";alg="ed25519""#
        );
        let parsed = parse(&headers.signature_input, &headers.signature).unwrap();
        assert_eq!(parsed.expires, Some(1763300030));
        parsed.verify(&key.verifying_key(), &c).unwrap();
        parsed.check_window(1763300030, 60).unwrap();
        assert!(parsed.check_window(1763300031, 60).is_err());

        let headers = sign(&key, "key-id", 1763300000, Some(1763303600), &c);
        let parsed = parse(&headers.signature_input, &headers.signature).unwrap();
        parsed.check_window(1763300060, 60).unwrap();
        assert!(parsed.check_window(1763300061, 60).is_err());
    }

    #[test]
    fn rejects_other_profiles() {
        let sig = format!("sig1=:{}:", BASE64.encode([0u8; 64]));
        for input in [
            r#"sig1=("@method" "@authority" "@path" "@query");created=1;keyid="k""#,
            r#"sig1=("@method" "@authority" "@path" "@query" "content-digest");keyid="k""#,
            r#"sig1=("@method" "@authority" "@path" "@query" "content-digest");created=1;keyid="k";alg="rsa-pss-sha512""#,
            r#"sig1=("@method" "@authority" "@path" "@query" "content-digest");created=1;keyid="k";nonce="x""#,
        ] {
            assert!(parse(input, &sig).is_err(), "{input}");
        }
        let ok = r#"sig1=("@method" "@authority" "@path" "@query" "content-digest");created=1;keyid="k""#;
        assert!(parse(ok, &sig).is_ok());
        assert!(parse(ok, &sig.replace("sig1", "sig2")).is_err());
    }

    #[test]
    fn content_digest_verification() {
        let digest = content_digest(b"hello");
        verify_content_digest(&digest, b"hello").unwrap();
        assert_eq!(
            verify_content_digest(&digest, b"hellO"),
            Err(Error::DigestMismatch)
        );
        assert!(verify_content_digest("sha-512=:AAAA:", b"hello").is_err());
    }
}
