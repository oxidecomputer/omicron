// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The v0 bootstore protocol (aka Low-Rent Trust Quorum)

mod fsm;
mod messages;
mod peer;
mod peer_networking;
mod request_manager;
mod share_pkg;
mod storage;

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::time::Duration;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

pub use fsm::{ApiError, ApiOutput, Fsm, State};
pub use messages::{
    Envelope, Msg, MsgError, Request, RequestType, Response, ResponseType,
};
pub use peer::{Config, Node, NodeHandle, NodeRequestError, Status};
pub use request_manager::{RequestManager, TrackableRequest};
pub use share_pkg::{create_pkgs, LearnedSharePkg, SharePkg, SharePkgCommon};
pub use storage::NetworkConfig;

/// The current version of supported messages within the v0 scheme
///
/// This number should be incremented when new messages or enum variants are
/// added.
#[allow(unused)]
pub const CURRENT_VERSION: u32 = 0;

/// A static description of the V0 scheme for trust quorum
///
/// This is primarily for informational purposes.
use super::params::*;
#[allow(unused)]
#[derive(Default, Debug, Clone, Copy)]
pub struct V0Scheme {
    encryption_algorithm: ChaCha20Poly1305,
    hash_algorithm: Sha3_256,
    key_derivation: Hkdf,
    trust_quorum_transport: Tcp,
    trusted_group_membership: No,
    shamir_curve: Curve25519,
    message_serialization: Cbor,
    message_framing_header: U32BigEndian,
    message_signing: No,
}

/// A newtype around Uuid useful for type-safe disambiguation
#[derive(
    Display,
    From,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct RackUuid(pub Uuid);

/// A secret share
#[derive(
    Zeroize, ZeroizeOnDrop, PartialEq, Eq, Clone, Serialize, Deserialize,
)]
pub struct Share(pub Vec<u8>);

// Manually implemented to redact info
impl Debug for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Share").finish()
    }
}

/// A combined set of secret shares. This is the format that `vsss_rs` requires
/// the shares in so that it will combine them to reconstruct the secret.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Shares(pub Vec<Vec<u8>>);

// Manually implemented to redact info
impl Debug for Shares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shares").finish()
    }
}

/// Configuration of the FSM
#[derive(Debug, Clone, Copy)]
pub struct FsmConfig {
    pub learn_timeout: Duration,
    pub rack_init_timeout: Duration,
    pub rack_secret_request_timeout: Duration,
}
