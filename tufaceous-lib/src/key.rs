use anyhow::{bail, Result};
use hex::FromHex;
use rand::{rngs::OsRng, RngCore};
use ring::rand::SecureRandom;
use ring::signature::Ed25519KeyPair;
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
    Ed25519(
        // We could store this as a `ring::signature::Ed25519KeyPair`, but that
        // doesn't impl `Clone`.
        [u8; 32],
    ),
}

impl Key {
    pub fn generate_ed25519() -> Key {
        let mut key = [0; 32];
        OsRng.fill_bytes(&mut key);
        Key::Ed25519(key)
    }

    pub(crate) fn as_sign(&self) -> SignKeyPair {
        match self {
            Key::Ed25519(key) => SignKeyPair::ED25519(
                Ed25519KeyPair::from_seed_unchecked(key)
                    .expect("ed25519 key length mismatch"),
            ),
        }
    }
}

#[async_trait]
impl Sign for Key {
    fn tuf_key(&self) -> tough::schema::key::Key {
        self.as_sign().tuf_key()
    }

    async fn sign(
        &self,
        msg: &[u8],
        rng: &(dyn SecureRandom + Sync),
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        self.as_sign().sign(msg, rng).await
    }
}

#[async_trait]
impl KeySource for Key {
    async fn as_sign(
        &self,
    ) -> Result<Box<dyn Sign>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        Ok(Box::new(self.clone()))
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
            Some(("ed25519", hex)) => Ok(Key::Ed25519(FromHex::from_hex(hex)?)),
            Some((kind, _)) => bail!("Invalid key source kind: {}", kind),
            None => bail!("Invalid key source (format is `kind:data`)"),
        }
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Ed25519(key) => {
                write!(f, "ed25519:{}", hex::encode(key))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Key;
    use ring::signature::Ed25519KeyPair;
    use std::str::FromStr;

    #[test]
    fn test_from_str() {
        let key = Key::from_str("ed25519:9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60").unwrap();
        match key {
            Key::Ed25519(key) => {
                let _ = Ed25519KeyPair::from_seed_unchecked(&key).unwrap();
            }
        }
    }
}
