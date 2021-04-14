use omicron_common::error::ApiError;
use omicron_common::NexusClient;
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Describes Oxide API objects that can be simulated here in the sled agent
 *
 * We only simulate these objects from the perspective of an API consumer, which
 * means for example accepting a request to boot it, reporting the current state
 * as "starting", and then some time later reporting that the state is
 * "running".
 *
 * This interface defines only associated functions, not constructors nor what
 * would traditionally be called "methods".  On the one hand, this approach is
 * relatively simple to reason about, since all the functions are stateless.  On
 * the other hand, the interface is a little gnarly.  That's largely because
 * this plugs into a more complex simulation mechanism (implemented by
 * `SimObject` and `SimCollection`) and this interface defines only the small
 * chunks of functionality that differ between types.  Still, this is cleaner
 * than what was here before!
 *
 * The basic idea is that for any type that we want to simulate (e.g.,
 * Instances), there's a `CurrentState` (which you could think of as "stopped",
 * "starting", "running", "stopping") and a `RequestedState` (which would only
 * be "stopped" and "running" -- you can't ask an Instance to transition to
 * "starting" or "stopping")
 *
 * (The term "state" here is a bit overloaded.  `CurrentState` refers to the
 * state of the object itself that's being simulated.  This might be an Instance
 * that is currently in state "running".  `RequestedState` refers to a requested
 * _change_ to the state of the object.  The state of the _simulated_ object
 * includes both of these: e.g., a "starting" Instance that is requested to be
 * "running".  So in most cases in the interface below, the state is represented
 * by a tuple of `(CurrentState, Option<RequestedState>)`.)
 *
 * Transitioning between states is always either synchronous (which means that
 * we make the transition immediately) or asynchronous (which means that we
 * first transition to some intermediate state and some time later finish the
 * transition to the requested state).  An Instance transition from "Stopped" to
 * "Destroyed" is synchronous.  An Instance transition from "Stopped" to
 * "Running" is asynchronous; it first goes to "Starting" and some time later
 * becomes "Running".
 *
 * It's expected that an object can begin another user-requested state
 * transition no matter what state it's in, although some particular transitions
 * may be disallowed (e.g., "reboot" from a stopped state).
 *
 * The implementor determines the set of possible states (via `CurrentState` and
 * `RequestedState`) as well as what transitions are allowed (via
 * `next_state_for_new_target()`).
 *
 * When an asynchronous state change completes, we notify the control plane via
 * the `notify()` function.
 */
/*
 * TODO-cleanup Among the awkward bits here is that the an object's state is
 * essentially represented by a tuple `(CurrentState, Option<RequestedState>)`,
 * but that's not represented anywhere.  Would it help to have that be a
 * first-class type?  Maybe it would be easier if there were a separate type
 * representing pairs of possible values here?  That sounds worse (because it
 * sounds MxN), but in practice many combinations are not legal and so it might
 * eliminate code that checks for these cases.
 */
#[async_trait]
pub trait Simulatable: fmt::Debug {
    /**
     * Represents a possible current runtime state of the simulated object.
     * For an Instance, you might think of the state as "starting" or "running",
     * etc., although in practice it's likely an object that includes this as
     * well as a generation counter and other metadata.
     */
    type CurrentState: Send + Clone + fmt::Debug;

    /**
     * Represents a possible requested state of the simulated object.  This is
     * often a subset of current states, since users may not be able to request
     * transitions to intermediate states.
     */
    type RequestedState: Send + Clone + fmt::Debug;

    /**
     * Given `current` (the current state of a simulated object), `pending`
     * (the requested state associated with a currently outstanding asynchronous
     * transition), and `target` (a new requested state), return a tuple
     * describing the next state and the requested state for the next
     * asynchronous transition, if any.
     *
     * If the requested transition is illegal (which should not be common),
     * return an appropriate `ApiError`.  If the requested transition can be
     * completed immediately (synchronously), the second field of the returned
     * tuple should be `None`.  If the requested transition is asynchronous, the
     * immediate next state should be returned as the first element and the
     * pending asynchronous request should be returned as the second.
     */
    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        next: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>;

    /**
     * Given `current` (the current state of a simulated object) and `pending`
     * (the requested state associated with a currently outstanding asynchronous
     * transition), return a tuple of the immediate next state and the requested
     * state for the next asynchronous transition, if any.  The return value has
     * the same semantics as for `next_state_for_new_target()`; however, this
     * function is not allowed to fail.  (Put differently, if this could fail,
     * then it should fail when the asynchronous state change was started, back
     * in next_state_for_new_target()`.)
     */
    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>);

    /**
     * Returns true if "state2" represents no meaningful change from "state1".
     * If possible, this should use a generation number or the like.
     */
    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool;

    /**
     * Returns true if the state `current` is a terminal state representing that
     * the object has been destroyed.
     */
    fn ready_to_destroy(current: &Self::CurrentState) -> bool;

    /**
     * Notifies Nexus (via `csc`) about a new state (`current`) for the object
     * identified by `id`.
     */
    async fn notify(
        csc: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError>;
}
