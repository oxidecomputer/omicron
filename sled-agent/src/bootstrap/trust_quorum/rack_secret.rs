// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::convert::AsRef;
use std::fmt::Debug;

use p256::elliptic_curve::group::ff::PrimeField;
use p256::elliptic_curve::subtle::ConstantTimeEq;
use p256::{NonZeroScalar, ProjectivePoint, Scalar, SecretKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use vsss_rs::{Feldman, FeldmanVerifier, Share};

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
/// must be combinded in order to reconstruct the shared secret such that disks
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
pub struct RackSecret {
    secret: NonZeroScalar,
}

impl PartialEq for RackSecret {
    fn eq(&self, other: &Self) -> bool {
        self.secret.ct_eq(&other.secret).into()
    }
}

impl Eq for RackSecret {}

/// A verifier used to ensure the validity of a given key share for an unknown
/// secret.
///
/// We use verifiable secret sharing to detect invalid shares from being
/// combined and generating an incorrect secret. Each share must be verified
/// before the secret is reconstructed.
//
// This is just a wrapper around a FeldmanVerifier from the vsss-rs crate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Verifier {
    verifier: FeldmanVerifier<Scalar, ProjectivePoint>,
}

impl Verifier {
    pub fn verify(&self, share: &Share) -> bool {
        self.verifier.verify(share)
    }
}

impl RackSecret {
    /// Create a secret based on the NIST P-256 curve
    pub fn new() -> RackSecret {
        let mut rng = OsRng::default();
        let sk = SecretKey::random(&mut rng);
        RackSecret { secret: sk.to_secret_scalar() }
    }

    /// Split a secert into `total_shares` number of shares, where combining
    /// `threshold` of the shares can be used to recover the secret.
    pub fn split(
        &self,
        threshold: usize,
        total_shares: usize,
    ) -> Result<(Vec<Share>, Verifier), vsss_rs::Error> {
        let mut rng = OsRng::default();
        let (shares, verifier) = Feldman { t: threshold, n: total_shares }
            .split_secret(*self.as_ref(), None, &mut rng)?;
        Ok((shares, Verifier { verifier }))
    }

    /// Combine a set of shares and return a RackSecret
    pub fn combine_shares(
        threshold: usize,
        total_shares: usize,
        shares: &[Share],
    ) -> Result<RackSecret, vsss_rs::Error> {
        let scalar = Feldman { t: threshold, n: total_shares }
            .combine_shares::<Scalar>(shares)?;
        let nzs = NonZeroScalar::from_repr(scalar.to_repr()).unwrap();
        let sk = SecretKey::from(nzs);
        Ok(RackSecret { secret: sk.to_secret_scalar() })
    }
}

impl AsRef<Scalar> for RackSecret {
    fn as_ref(&self) -> &Scalar {
        self.secret.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use super::*;

    // This is a secret. Let's not print it outside of tests.
    impl Debug for RackSecret {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.secret.as_ref().fmt(f)
        }
    }

    fn verify(secret: &RackSecret, verifier: &Verifier, shares: &[Share]) {
        for s in shares {
            assert!(verifier.verify(s));
        }

        let secret2 = RackSecret::combine_shares(3, 5, &shares[..3]).unwrap();
        let secret3 = RackSecret::combine_shares(3, 5, &shares[1..4]).unwrap();
        let secret4 = RackSecret::combine_shares(3, 5, &shares[2..5]).unwrap();
        let shares2 =
            vec![shares[0].clone(), shares[2].clone(), shares[4].clone()];
        let secret5 = RackSecret::combine_shares(3, 5, &shares2).unwrap();

        for s in [secret2, secret3, secret4, secret5] {
            assert_eq!(*secret, s);
        }
    }

    #[test]
    fn create_and_verify() {
        let secret = RackSecret::new();
        let (shares, verifier) = secret.split(3, 5).unwrap();
        verify(&secret, &verifier, &shares);
    }

    #[test]
    fn secret_splitting_fails_with_threshold_larger_than_total_shares() {
        let secret = RackSecret::new();
        assert!(secret.split(5, 3).is_err());
    }

    #[test]
    fn combine_deserialized_shares() {
        let secret = RackSecret::new();
        let (shares, verifier) = secret.split(3, 5).unwrap();
        let verifier_s = bincode::serialize(&verifier).unwrap();
        let shares_s = bincode::serialize(&shares).unwrap();

        let shares2: Vec<Share> = bincode::deserialize(&shares_s).unwrap();
        let verifier2: Verifier = bincode::deserialize(&verifier_s).unwrap();

        // Ensure we can reconstruct the secret with the deserialized shares and
        // verifier.
        verify(&secret, &verifier2, &shares2);

        // Ensure we can reconstruct the secret with the deserialized shares and
        // original verifier.
        verify(&secret, &verifier, &shares2);
    }
}
