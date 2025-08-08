// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parameter to Node API calls that allows interaction with the system at large

use crate::{
    Alarm, Envelope, PeerMsg, PeerMsgKind, PersistentState, PlatformId,
    persistent_state::PersistentStateDiff,
};
use daft::{BTreeSetDiff, Diffable, Leaf};
use std::collections::BTreeSet;

/// An API shared by [`NodeCallerCtx`] and [`NodeHandlerCtx`]
pub trait NodeCommonCtx {
    fn platform_id(&self) -> &PlatformId;
    fn persistent_state(&self) -> &PersistentState;
    fn connected(&self) -> &BTreeSet<PlatformId>;
    fn alarms(&self) -> &BTreeSet<Alarm>;
}

/// An API for an [`NodeCtx`] usable from a [`crate::Node`]
pub trait NodeCallerCtx: NodeCommonCtx {
    fn num_envelopes(&self) -> usize;
    fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope>;
    fn envelopes(&self) -> impl Iterator<Item = &Envelope>;

    /// Check if the contained `PersistentState` has been mutated
    ///
    /// IMPORTANT: Calling this method resets the state of mutation to
    /// `false`. This means that callers should only call this once after each
    /// [`crate::Node`] API call and cache the result as necessary. This is also
    /// why this method takes an
    /// `&mut self`.
    fn persistent_state_change_check_and_reset(&mut self) -> bool;
}

/// An API for an [`NodeCtx`] usable from inside FSM states
pub trait NodeHandlerCtx: NodeCommonCtx {
    fn send(&mut self, to: PlatformId, msg: PeerMsgKind);

    /// Attempt to update the persistent state inside the callback `f`. If
    /// the state is updated, then `f` should return `true`, otherwise it should
    /// return `false`.
    ///
    /// Returns the same value as `f`.
    ///
    /// IMPORTANT: This method sets a bit indicating whether or not the
    /// underlying `PersistentState` was mutated, for use by callers. This
    /// method can safely be called multiple times. If any call mutates the
    /// persistent state, then the bit will remain set. The bit is only cleared
    /// when a caller calls `persistent_state_change_check_and_reset`.
    fn update_persistent_state<F>(&mut self, f: F) -> bool
    where
        F: FnOnce(&mut PersistentState) -> bool;

    /// Add a peer to the connected set
    fn add_connection(&mut self, id: PlatformId);

    /// Remove a peer from the connected set
    fn remove_connection(&mut self, id: &PlatformId);

    /// Record (in-memory) that an alarm has occurred
    fn raise_alarm(&mut self, alarm: Alarm);
}

/// Common parameter to [`crate::Node`] methods
///
/// We separate access to this context via different APIs; namely [`NodeCallerCtx`]
/// and [`NodeHandlerCtx`]. This statically prevents both the caller and
/// [`crate::Node`] internals from performing improper mutations.
#[derive(Debug, Clone, Diffable)]
#[cfg_attr(feature = "danger_partial_eq_ct_wrapper", derive(PartialEq, Eq))]
pub struct NodeCtx {
    /// The unique hardware ID of a sled
    platform_id: PlatformId,

    /// State that gets persistenly stored in ledgers
    persistent_state: PersistentState,

    /// Was persistent_state modified by a call to `update_persistent_state`?
    ///
    /// This gets reset by reading the persistent state with
    /// [`NodeCallerCtx::persistent_state_change_check_and_reset`].
    persistent_state_changed: bool,

    /// Outgoing messages destined for other peers
    outgoing: Vec<Envelope>,

    /// Connected peer nodes
    connected: BTreeSet<PlatformId>,

    /// Any alarms that have occurred
    alarms: BTreeSet<Alarm>,
}

// For diffs we want to allow access to all fields, but not make them public in
// the `NodeCtx` type itself.
impl<'daft> NodeCtxDiff<'daft> {
    pub fn platform_id(&self) -> Leaf<&PlatformId> {
        self.platform_id
    }

    pub fn persistent_state(&self) -> &PersistentStateDiff<'daft> {
        &self.persistent_state
    }

    pub fn persistent_state_changed(&self) -> Leaf<&bool> {
        self.persistent_state_changed
    }

    pub fn outgoing(&self) -> Leaf<&[Envelope]> {
        self.outgoing
    }

    pub fn connected(&self) -> &BTreeSetDiff<'daft, PlatformId> {
        &self.connected
    }

    pub fn alarms(&self) -> &BTreeSetDiff<'daft, Alarm> {
        &self.alarms
    }
}

impl NodeCtx {
    pub fn new(platform_id: PlatformId) -> NodeCtx {
        NodeCtx {
            platform_id,
            persistent_state: PersistentState::empty(),
            persistent_state_changed: false,
            outgoing: Vec::new(),
            connected: BTreeSet::new(),
            alarms: BTreeSet::new(),
        }
    }
}

impl NodeCommonCtx for NodeCtx {
    fn platform_id(&self) -> &PlatformId {
        &self.platform_id
    }

    fn persistent_state(&self) -> &PersistentState {
        &self.persistent_state
    }

    fn connected(&self) -> &BTreeSet<PlatformId> {
        &self.connected
    }

    fn alarms(&self) -> &BTreeSet<Alarm> {
        &self.alarms
    }
}

impl NodeHandlerCtx for NodeCtx {
    fn send(&mut self, to: PlatformId, msg_kind: PeerMsgKind) {
        let rack_id = self.persistent_state.rack_id().expect("rack id exists");
        self.outgoing.push(Envelope {
            to,
            from: self.platform_id.clone(),
            msg: PeerMsg { rack_id, kind: msg_kind },
        });
    }

    fn update_persistent_state<F>(&mut self, f: F) -> bool
    where
        F: FnOnce(&mut PersistentState) -> bool,
    {
        // We don't ever revert from true to false, which allows calling this
        // method multiple times in handler context.
        if f(&mut self.persistent_state) {
            self.persistent_state_changed = true;
            true
        } else {
            false
        }
    }

    fn add_connection(&mut self, id: PlatformId) {
        self.connected.insert(id);
    }

    fn remove_connection(&mut self, id: &PlatformId) {
        self.connected.remove(id);
    }

    fn raise_alarm(&mut self, alarm: Alarm) {
        self.alarms.insert(alarm);
    }
}

impl NodeCallerCtx for NodeCtx {
    fn num_envelopes(&self) -> usize {
        self.outgoing.len()
    }

    fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope> {
        self.outgoing.drain(..)
    }

    fn envelopes(&self) -> impl Iterator<Item = &Envelope> {
        self.outgoing.iter()
    }

    fn persistent_state_change_check_and_reset(&mut self) -> bool {
        let changed = self.persistent_state_changed;
        self.persistent_state_changed = false;
        changed
    }
}
