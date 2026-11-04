// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use rand08::rngs::OsRng;
use secrecy::{ExposeSecret, SecretBox};
use std::fmt::Debug;
use vsss_rs::curve25519::WrappedScalar;
use vsss_rs::curve25519_dalek::Scalar;
use vsss_rs::shamir;
use vsss_rs::subtle::ConstantTimeEq;

/// A `RackSecret` is a shared secret used to perform a "rack-level" unlock.
///
/// Each server sled on an oxide rack contains up to 10 SSDs containing customer
/// data and oxide specific data. We want to ensure that if a small number of
/// disks or sleds get stolen, the data on them remains inaccessible to the
/// thief.  We also want to ensure that successfully rebooting a sled or an
/// entire rack does not require administrator intervention such as entering a
/// password.
///
/// To provide the above guarantees we must ensure that all disks are encrypted,
/// and that there is an automatic mechanism to retrieve the decryption key.
/// Furthermore, we must guarantee that the the key retrieval mechanism is not
/// available when a only a subset of sleds or disks from a rack are available
/// to an attacker. The mechanism we use to provide these guarantees is based on
/// <https://en.wikipedia.org/wiki/Shamir%27s_Secret_Sharing>. A threshold secret
/// is generated of which individual disk encryption keys are derived, and each
/// of `N` server sleds receives one share of the secret. `K` of these shares
/// must be combined in order to reconstruct the shared secret such that disks
/// may be decrypted. If fewer than `K` shares are available, no information
/// about the secret may be recovered, and the disks cannot be decrypted. We
/// call the threshold secret the `rack secret`.
///
/// Inside a rack then, the sleds cooperate over secure channels in order to
/// retrieve key shares and reconstruct the `rack secret`, the resulting derived
/// encryption keys, and unlock their own local storage. We call this procedure
/// `rack unlock`. The establishment of secure channels and the ability to trust
/// the validity of a participating peer is outside the scope of this particular
/// type and orthogonal to its implementation.
#[derive(Debug)]
pub struct RackSecret {
    secret: SecretBox<Scalar>,
}

impl Clone for RackSecret {
    fn clone(&self) -> Self {
        RackSecret {
            secret: SecretBox::new(Box::new(*self.secret.expose_secret())),
        }
    }
}

impl PartialEq for RackSecret {
    fn eq(&self, other: &Self) -> bool {
        self.secret.expose_secret().ct_eq(other.secret.expose_secret()).into()
    }
}

impl Eq for RackSecret {}

impl RackSecret {
    /// Create a secret based on Curve25519
    pub fn new() -> RackSecret {
        let mut rng = OsRng;

        RackSecret {
            secret: SecretBox::new(Box::new(Scalar::random(&mut rng))),
        }
    }

    /// Split a secret into `total_shares` number of shares, where combining
    /// `threshold` of the shares can be used to recover the secret.
    pub fn split(
        &self,
        threshold: u8,
        total_shares: u8,
    ) -> Result<SecretBox<Vec<Vec<u8>>>, vsss_rs::Error> {
        let mut rng = OsRng;
        Ok(SecretBox::new(Box::new(shamir::split_secret::<
            WrappedScalar,
            u8,
            Vec<u8>,
        >(
            threshold as usize,
            total_shares as usize,
            (*self.secret.expose_secret()).into(),
            &mut rng,
        )?)))
    }

    /// Combine a set of shares and return a RackSecret
    pub fn combine_shares(
        shares: &[Vec<u8>],
    ) -> Result<RackSecret, vsss_rs::Error> {
        let wrapped_scalar: WrappedScalar = vsss_rs::combine_shares(shares)?;
        Ok(RackSecret { secret: SecretBox::new(Box::new(wrapped_scalar.0)) })
    }

    pub fn expose_secret(&self) -> &Scalar {
        self.secret.expose_secret()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn verify(secret: &RackSecret, shares: &[Vec<u8>]) {
        let secret2 = RackSecret::combine_shares(&shares[..3]).unwrap();
        let secret3 = RackSecret::combine_shares(&shares[1..4]).unwrap();
        let secret4 = RackSecret::combine_shares(&shares[2..5]).unwrap();
        let shares2 =
            vec![shares[0].clone(), shares[2].clone(), shares[4].clone()];
        let secret5 = RackSecret::combine_shares(&shares2).unwrap();

        for s in [secret2, secret3, secret4, secret5] {
            assert_eq!(*secret, s);
        }
    }

    #[test]
    fn create_and_verify() {
        let secret = RackSecret::new();
        let shares = secret.split(3, 5).unwrap();
        verify(&secret, shares.expose_secret());
    }

    #[test]
    fn secret_splitting_fails_with_threshold_larger_than_total_shares() {
        let secret = RackSecret::new();
        assert!(secret.split(5, 3).is_err());
    }
}
