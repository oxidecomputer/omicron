// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The two communication paths for the bootstore:
//!
//! RSS -> Sled Agent -> Coordinator -> Storage Nodes
//! Nexus -> Steno -> Sled Agent -> Coordinator -> Storage Nodes
//!
//!
//! Since some trust quorum membership information that is input via RSS must
//! make its way into CockroachDb so that reconfiguration works, we will load
//! that information from the trust quorum database, parse it, and write
//! it to CockroachDB when we start it up.

pub mod trust_quorum;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sha3_256Digest([u8; 32]);

// We keep these in a module to prevent naming conflicts
#[allow(unused)]
pub mod scheme_params {
    #[derive(Default, Debug, Clone, Copy)]
    pub struct ChaCha20Poly1305;

    #[derive(Default, Debug, Clone, Copy)]
    pub struct Sha3_256;

    #[derive(Default, Debug, Clone, Copy)]
    pub struct Hkdf;

    #[derive(Default, Debug, Clone, Copy)]
    pub struct Tcp;

    #[derive(Default, Debug, Clone, Copy)]
    pub struct No;

    #[derive(Default, Debug, Clone, Copy)]
    pub struct Curve25519;

    #[derive(Default, Debug, Clone, Copy)]
    pub struct Cbor;
}

/// A static description of the V0 scheme for trust quorum
///
/// This is primarily for informational purposes.
use scheme_params::*;
#[allow(unused)]
#[derive(Default, Debug, Clone, Copy)]
pub struct V0Scheme {
    encryption_algorithm: ChaCha20Poly1305,
    hash_algorithm: Sha3_256,
    key_derivation: Hkdf,
    trust_quorum_transport: Tcp,
    trusted_group_membership: No,
    shamir_curve: Curve25519,
    serialization: Cbor,
}
