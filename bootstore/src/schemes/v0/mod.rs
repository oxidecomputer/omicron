// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The v0 bootstore protocol (aka Low-Rent Trust Quorum)

mod fsm;
mod fsm_output;
mod messages;
mod state;
mod state_initial_member;
mod state_learned;
mod state_learning;
mod state_uninitialized;

pub use fsm::Fsm;
pub use fsm_output::{ApiError, ApiOutput, Output};
pub use messages::{
    Envelope, Error as MsgError, Msg, Request, RequestType, Response,
    ResponseType,
};
pub use state::{
    Config, FsmCommonData, RackInitState, RackSecretState, State, Ticks,
};
pub use state_initial_member::InitialMemberState;
pub use state_learned::LearnedState;
pub use state_learning::{LearnAttempt, LearningState};
pub use state_uninitialized::UninitializedState;

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
    message_serialization: Bcs,
    message_framing_header: U32BigEndian,
    message_signing: No,
}
