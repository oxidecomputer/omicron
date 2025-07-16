// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A trait for the trust-quorum protocol state machine

mod coordinating;

use crate::{
    Envelope, PeerMsg, PersistentState, PlatformId, ReconfigureMsg,
    validators::ReconfigurationError,
};
use std::time::Instant;

/// An api shared by [`FsmCtxNodeApi`] and [`FsmCtxApi`]
pub trait FsmCtxCommonApi {
    fn now(&self) -> Instant;
    fn persistent_state(&self) -> &PersistentState;
}

/// An api for an [`FsmCtx`] usable from a [`crate::Node`]
pub trait FsmCtxNodeApi: FsmCtxCommonApi {
    fn set_time(&mut self, now: Instant);
    fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope>;
}

/// An api for an [`FsmCtx`] usable from inside FSM states
pub trait FsmCtxApi: FsmCtxCommonApi {
    fn send(&mut self, to: PlatformId, msg: PeerMsg);
    fn persistent_state_mut(&mut self) -> &mut PersistentState;
}

/// Common state shared among all FSM states
pub struct FsmCtx {
    /// The unique hardware ID of a sled
    platform_id: PlatformId,

    /// State that gets persistenly stored in ledgers
    persistent_state: PersistentState,

    /// Outgoing messages destined for other peers
    outgoing: Vec<Envelope>,

    /// The current time
    now: Instant,
}

impl FsmCtxCommonApi for FsmCtx {
    fn now(&self) -> Instant {
        self.now
    }

    fn persistent_state(&self) -> &PersistentState {
        &self.persistent_state
    }
}

impl FsmCtxApi for FsmCtx {
    fn send(&mut self, to: PlatformId, msg: PeerMsg) {
        self.outgoing.push(Envelope {
            to,
            from: self.platform_id.clone(),
            msg,
        });
    }

    fn persistent_state_mut(&mut self) -> &mut PersistentState {
        &mut self.persistent_state
    }
}

impl FsmCtxNodeApi for FsmCtx {
    fn set_time(&mut self, now: Instant) {
        self.now = now;
    }

    fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope> {
        self.outgoing.drain(..)
    }
}

pub(crate) trait FsmState {
    /// Return the name of this state
    fn name(&self) -> &'static str;

    /// Handle a message from another node
    fn handle(
        &mut self,
        ctx: &mut dyn FsmCtxApi,
        from: PlatformId,
        msg: PeerMsg,
    );

    /// Process a timer tick
    ///
    /// Ticks are issued by the caller in order to move the protocol forward.
    /// The current time is passed in to make the calls deterministic.
    fn tick(&mut self, ctx: &mut dyn FsmCtxApi);

    /// Start coordinating a reconfiguration
    ///
    /// On success:
    ///  * puts messages that need sending to other nodes in `outbox`.
    ///  * returns `Ok(Some(PersistentState))` which the caller must write to
    ///    disk if the state has changed. Returns `Ok(None)` otherwise.
    fn coordinate_reconfiguration(
        &mut self,
        ctx: &mut dyn FsmCtxApi,
        msg: ReconfigureMsg,
    ) -> Result<Option<&PersistentState>, ReconfigurationError>;
}
