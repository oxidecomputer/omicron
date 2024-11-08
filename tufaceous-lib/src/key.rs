use anyhow::{bail, Result};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::Ed25519KeyPair;
use base64::{engine::general_purpose::URL_SAFE, Engine};
use std::fmt::Display;
use std::str::FromStr;
use tough::async_trait;
use tough::key_source::KeySource;
use tough::sign::{Sign, SignKeyPair};

pub(crate) fn boxed_keys(keys: Vec<Key>) -> Vec<Box<dyn KeySource>> {
    keys.into_iter().map(|k| Box::new(k) as Box<dyn KeySource>).collect()
}

#[derive(Debug, Clone)]
pub enum Key {
    Ed25519 { pkcs8: Vec<u8> },
}

impl Key {
    pub fn generate_ed25519() -> Result<Key> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        Ok(Key::Ed25519 { pkcs8: pkcs8.as_ref().to_vec() })
    }

    fn as_sign_key_pair(&self) -> Result<SignKeyPair> {
        match self {
            Key::Ed25519 { pkcs8 } => {
                Ok(SignKeyPair::ED25519(Ed25519KeyPair::from_pkcs8(pkcs8)?))
            }
        }
    }

    pub(crate) fn as_tuf_key(&self) -> Result<tough::schema::key::Key> {
        Ok(self.as_sign_key_pair()?.tuf_key())
    }
}

#[async_trait]
impl KeySource for Key {
    async fn as_sign(
        &self,
    ) -> Result<Box<dyn Sign>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        Ok(Box::new(self.as_sign_key_pair()?))
    }

    async fn write(
        &self,
        _value: &str,
        _key_id_hex: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        Err("cannot write key back to key source".into())
    }
}

impl FromStr for Key {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Key> {
        match s.split_once(':') {
            Some(("ed25519", base64)) => {
                Ok(Key::Ed25519 { pkcs8: URL_SAFE.decode(base64)? })
            }
            Some((kind, _)) => bail!("Invalid key source kind: {}", kind),
            None => bail!("Invalid key source (format is `kind:data`)"),
        }
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Ed25519 { pkcs8 } => {
                write!(f, "ed25519:{}", URL_SAFE.encode(pkcs8))
            }
        }
    }
}
