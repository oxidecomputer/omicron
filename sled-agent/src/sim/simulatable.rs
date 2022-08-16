// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::nexus::NexusClient;
use async_trait::async_trait;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

/// Describes Oxide API objects that can be simulated here in the sled agent
///
/// We only simulate these objects from the perspective of an API consumer, which
/// means for example accepting a request to boot it, reporting the current state
/// as "starting", and then some time later reporting that the state is
/// "running".
///
/// The basic idea is that for any type that we want to simulate (e.g.,
/// Instances), there's a `CurrentState` (which you could think of as "stopped",
/// "starting", "running", "stopping") and a `RequestedState` (which would only
/// be "stopped" and "running" -- you can't ask an Instance to transition to
/// "starting" or "stopping")
///
/// (The term "state" here is a bit overloaded.  `CurrentState` refers to the
/// state of the object itself that's being simulated.  This might be an Instance
/// that is currently in state "running".  `RequestedState` refers to a requested
/// _change_ to the state of the object.  The state of the _simulated_ object
/// includes both of these: e.g., a "starting" Instance that is requested to be
/// "running".  So in most cases in the interface below, the state is represented
/// by a tuple of `(CurrentState, Option<RequestedState>)`.)
///
/// Transitioning between states is always either synchronous (which means that
/// we make the transition immediately) or asynchronous (which means that we
/// first transition to some intermediate state and some time later finish the
/// transition to the requested state).  An Instance transition from "Stopped" to
/// "Destroyed" is synchronous.  An Instance transition from "Stopped" to
/// "Running" is asynchronous; it first goes to "Starting" and some time later
/// becomes "Running".
///
/// It's expected that an object can begin another user-requested state
/// transition no matter what state it's in, although some particular transitions
/// may be disallowed (e.g., "reboot" from a stopped state).
///
/// The implementor determines the set of possible states (via `CurrentState` and
/// `RequestedState`) as well as what transitions are allowed.
///
/// When an asynchronous state change completes, we notify the control plane via
/// the `notify()` function.
#[async_trait]
pub trait Simulatable: fmt::Debug + Send + Sync {
    /// Represents a possible current runtime state of the simulated object.
    /// For an Instance, you might think of the state as "starting" or "running",
    /// etc., although in practice it's likely an object that includes this as
    /// well as a generation counter and other metadata.
    type CurrentState: Send + Clone + fmt::Debug;

    /// Represents a possible requested state of the simulated object.  This is
    /// often a subset of current states, since users may not be able to request
    /// transitions to intermediate states.
    type RequestedState: Send + Clone + fmt::Debug;

    /// Arguments to start a producer on the simulated object.
    type ProducerArgs: Send + Clone + fmt::Debug;

    /// Represents an action that should be taken by the Sled Agent.
    /// Generated in response to a state change, either requested or observed.
    type Action: Send + Clone + fmt::Debug;

    /// Creates a new Simulatable object.
    fn new(current: Self::CurrentState) -> Self;

    /// Sets the producer based on the provided arguments.
    async fn set_producer(
        &mut self,
        args: Self::ProducerArgs,
    ) -> Result<(), Error>;

    /// Requests that the simulated object transition to a new target.
    ///
    /// If successful, returns the action that must be taken by the Sled Agent
    /// to alter the resource into the desired state.
    fn request_transition(
        &mut self,
        target: &Self::RequestedState,
    ) -> Result<Option<Self::Action>, Error>;

    /// Updates the state in response to an update within the simulated
    /// resource: whatever state was "desired" is observed immediately.
    ///
    /// Returns any actions that should be taken by the Sled Agent to continue
    /// altering the resource into a desired state.
    fn execute_desired_transition(&mut self) -> Option<Self::Action>;

    /// Returns the generation number for the current state.
    fn generation(&self) -> Generation;

    /// Returns the current state.
    fn current(&self) -> &Self::CurrentState;

    /// Returns the "desired" state, if one exists.
    ///
    /// If this returns None, either no state was requested, or the desired
    /// state has been reached.
    fn desired(&self) -> &Option<Self::RequestedState>;

    /// Returns true if the state `current` is a terminal state representing that
    /// the object has been destroyed.
    fn ready_to_destroy(&self) -> bool;

    /// Notifies Nexus (via `nexus_client`) about a new state (`current`) for
    /// the object identified by `id`.
    async fn notify(
        nexus_client: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), Error>;
}
