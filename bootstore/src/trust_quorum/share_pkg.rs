// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Sha3_256Digest;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A container used distributed among trust quorum participants for
/// trust quorum version 0 Scheme: [`crate::SchemeV0`].
///
/// This scheme does not verify membership, and will hand out shares
/// to whoever asks.
#[derive(Clone, PartialEq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct SharePkgV0 {
    #[zeroize(skip)]
    pub rack_uuid: Uuid,
    // We aren't planning on doing any reconfigurations with this version of
    // the protocol
    pub epoch: u32,
    pub threshold: u8,
    pub share: Vec<u8>,

    // Digests of all 256 shares so that each sled can verify the integrity
    // of received shares.
    //
    // No need for expensive nonsense
    #[zeroize(skip)]
    pub share_digests: Vec<Sha3_256Digest>,

    // Nonce used for encryption. We aren't worried about nonce-reuse
    // because we only encrypt one message, namely the shares in the field below.
    pub nonce: [u8; 12],

    // We include a distinct subset of unused shares for each sled in the
    // initial  group. This allows the sled to hand out unique shares when a
    // new sled joins the cluster. We keep these encrypted so that a single
    // sled does not have enough unencrrypted shares to unlock the rack without
    // participating in trust quorum.
    //
    // Each sled should keep local track of which shares it has already
    // handed out.
    #[zeroize(skip)]
    pub encrypted_shares: Vec<u8>,

    // Sha3_256 Digest of all prior fields in order
    // We digest the fields individually, in order so that we
    // do not have to worry about canonical serialization.
    #[zeroize(skip)]
    pub pkg_digest: Sha3_256Digest,
}

// We don't want to risk debug-logging the actual share contents, so implement
// `Debug` manually and omit sensitive fields.
impl fmt::Debug for SharePkgV0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharePkgV0")
            .field("rack_uuid", &self.rack_uuid)
            .field("epoch", &self.epoch)
            .field("threshold", &self.threshold)
            .field("share", &"Share")
            .field("share_digests", &self.share_digests)
            .field("nonce", &self.nonce)
            .field("encrypted_shares", &"Encrypted")
            .field("pkg_digest", &self.pkg_digest)
            .finish()
    }
}
