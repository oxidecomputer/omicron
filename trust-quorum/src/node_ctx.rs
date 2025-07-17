// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parameter to Node API calls that allows interaction with the system at large

use crate::{Envelope, PeerMsg, PersistentState, PlatformId};
use std::time::Instant;

/// An API shared by [`NodeCallerCtx`] and [`NodeHandlerCtx`]
pub trait NodeCommonCtx {
    fn platform_id(&self) -> &PlatformId;
    fn now(&self) -> Instant;
    fn persistent_state(&self) -> &PersistentState;
}

/// An API for an [`NodeCtx`] usable from a [`crate::Node`]
pub trait NodeCallerCtx: NodeCommonCtx {
    fn set_time(&mut self, now: Instant);
    fn num_envelopes(&self) -> usize;
    fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope>;
    fn envelopes(&self) -> impl Iterator<Item = &Envelope>;
    fn persistent_state_changed(&mut self) -> bool;
}

/// An API for an [`NodeCtx`] usable from inside FSM states
pub trait NodeHandlerCtx: NodeCommonCtx {
    fn send(&mut self, to: PlatformId, msg: PeerMsg);
    fn update_persistent_state<F>(&mut self, f: F)
    where
        F: FnOnce(&mut PersistentState) -> bool;
}

/// Common parameter to [`crate::Node`] methods
///
/// We separate access to this context via different APIs; namely [`NodeCallerCtx`]
/// and [`NodeHandlerCtx`]. This statically prevents both the caller and
/// [`Node`] internals from performing improper mutations.
pub struct NodeCtx {
    /// The unique hardware ID of a sled
    platform_id: PlatformId,

    /// State that gets persistenly stored in ledgers
    persistent_state: PersistentState,

    /// Was persistent_state modified by a call to `update_persistent_state`?
    ///
    /// This gets reset by reading the persistent state with
    /// [`NodeCallerCtx::persistent_state_change`].
    persistent_state_changed: bool,

    /// Outgoing messages destined for other peers
    outgoing: Vec<Envelope>,

    /// The current time
    now: Instant,
}

impl NodeCtx {
    pub fn new(platform_id: PlatformId) -> NodeCtx {
        NodeCtx {
            platform_id,
            persistent_state: PersistentState::empty(),
            persistent_state_changed: false,
            outgoing: Vec::new(),
            now: Instant::now(),
        }
    }
}

impl NodeCommonCtx for NodeCtx {
    fn platform_id(&self) -> &PlatformId {
        &self.platform_id
    }

    fn now(&self) -> Instant {
        self.now
    }

    fn persistent_state(&self) -> &PersistentState {
        &self.persistent_state
    }
}

impl NodeHandlerCtx for NodeCtx {
    fn send(&mut self, to: PlatformId, msg: PeerMsg) {
        self.outgoing.push(Envelope {
            to,
            from: self.platform_id.clone(),
            msg,
        });
    }

    fn update_persistent_state<F>(&mut self, f: F)
    where
        F: FnOnce(&mut PersistentState) -> bool,
    {
        // We don't ever revert from true to false, which allows calling this
        // method multiple times in handler context.
        if f(&mut self.persistent_state) {
            self.persistent_state_changed = true
        }
    }
}

impl NodeCallerCtx for NodeCtx {
    fn set_time(&mut self, now: Instant) {
        self.now = now;
    }

    fn num_envelopes(&self) -> usize {
        self.outgoing.len()
    }

    fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope> {
        self.outgoing.drain(..)
    }

    fn envelopes(&self) -> impl Iterator<Item = &Envelope> {
        self.outgoing.iter()
    }

    fn persistent_state_changed(&mut self) -> bool {
        let changed = self.persistent_state_changed;
        self.persistent_state_changed = false;
        changed
    }
}
