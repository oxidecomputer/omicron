// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::InitialMember`

use std::collections::BTreeMap;

use crate::schemes::v0::fsm_output::{ApiError, ApiOutput};
use crate::trust_quorum::SharePkgV0;

use super::fsm::{
    broadcast_share_requests, next_peer, validate_share, StateHandler,
};
use super::fsm_output::Output;
use super::messages::{Error, Request, RequestType, Response, ResponseType};
use super::state::{
    FsmCommonData, InitialMemberState, RackInitState, RackSecretState,
    RequestMetadata, ShareIdx, State,
};
use sled_hardware::Baseboard;
use uuid::Uuid;

#[derive(Debug)]
pub struct InitialMemberState {
    pub pkg: SharePkgV0,

    /// Shares given to other sleds. We mark them as used so that we don't
    /// hand them out twice. If the same sled asks us for a share, because
    /// it crashes or there is a network blip, we will return the same
    /// share each time.
    ///
    /// Note that this is a fairly optimistic strategy as the requesting
    /// sled can always go ask another sled after a network blip. However,
    /// this guarantees that a single sled never hands out more than one of
    /// its shares to any given sled.
    ///
    /// We can't do much better than this without some sort of centralized
    /// distributor which is part of the reconfiguration mechanism in later
    /// versions of the trust quourum protocol.
    pub distributed_shares: BTreeMap<Baseboard, ShareIdx>,

    // Acknowledgements tracked during rack initialization if this node is
    // the one acting as coordinator (local to RSS).
    pub rack_init_state: Option<RackInitState>,

    // Pending learn requests from other peers mapped to their start time.
    //
    // When a peer attempts to learn a share, we may not be able to give it
    // one because we cannot yet recompute the rack secret. We queue requests
    // here and respond when we can recompute the rack secret. If we timeout before
    // we can recompute the rack secret, we respond with a timeout Error.
    //
    // Note that if we get a new `RequestType::Learn` from a peer that is already
    // pending, we will reset the start time.
    pub pending_learn_requests: BTreeMap<Baseboard, RequestMetadata>,

    // In `InitialMember` or `Learned` states, it is sometimes necessary to
    // reconstruct the rack secret.
    //
    // This is needed to both unlock local storage or decrypt our extra shares
    // to hand out to learners.
    pub rack_secret_state: Option<RackSecretState>,
}

impl InitialMemberState {
    pub fn new(
        pkg: SharePkgV0,
        distributed_shares: BTreeMap<Baseboard, ShareIdx>,
    ) -> Self {
        InitialMemberState {
            pkg,
            distributed_shares,
            rack_init_state: None,
            pending_learn_requests: BTreeMap::new(),
            rack_secret_state: None,
        }
    }

    pub fn name(&self) -> &'static str {
        "initial_member"
    }

    // Process a `Share` received in a `Response`
    pub fn on_share(
        &mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        share: Vec<u8>,
    ) -> Output {
        let rack_secret_result = match &mut self.rack_secret_state {
            None => {
                return ApiError::UnexpectedResponse {
                    from,
                    state: self.name(),
                    request_id,
                    msg: response.name(),
                }
                .into()
            }
            Some(RackSecretState::Secret(_)) => {
                // We already have the rack secret, just drop the extra share
                return Output::none();
            }
            Some(RackSecretState::Shares(shares)) => {
                /// Compute and validate hash of the received key share
                if let Err(api_error) =
                    validate_share(&from, &share, &self.pkg.share_digests)
                {
                    return api_error.into();
                }

                // Add the share to our current set
                shares.insert(from, share);

                if shares.len() == pkg.threshold as usize {
                    let to_combine: Vec<_> = shares.values().cloned().collect();
                    RackSecret::combine_shares(&to_combine)
                } else {
                    return Output::none();
                }
            }
        };

        // Did computation of the rack secret succeed or fail?
        //
        // If we got to this point it means we at least had enough shares to try
        // to reconstruct the rack secret.

        // If we have a pending API request for the rack secret we can
        // resolve it now.
        let api_output = common
            .resolve_pending_api_request(rack_secret_result.as_ref().ok());

        match rack_secret_result {
            Ok(rack_secret) => {
                // If we have any pending peer learn requests, we
                // can now resolve them.
                let (persist, envelopes) =
                    self.resolve_learn_requests(&rack_secret);

                self.rack_secret_state =
                    Some(RackSecretState::Secret(rack_secret));

                Output { persist, envelopes, api_output }
            }
            Err(e) => {
                // Resolve all pending learn requests with an error
                let envelopes = self.resolve_learn_requests_with_error(
                    Error::FailedToReconstructRackSecret,
                );

                Output { persist: false, envelopes, api_output }
            }
        }
    }

    // We've just recomputed the rack secret. Now resolve any pending learn
    // requests. Return whether we need to persist state and any envelopes to
    // send to peers.
    fn resolve_learn_requests(
        &mut self,
        rack_secret: &RackSecret,
    ) -> (bool, Vec<Envelope>) {
        let mut persist = false;
        let mut envelopes =
            Vec::with_capacity(self.pending_learn_requests.len());
        match pkg.decrypt_shares(rack_secret) {
            Ok(shares) => {
                while let Some((to, metadata)) =
                    self.pending_learn_requests.pop_first()
                {
                    if let Some(idx) = self.distributed_shares.get(&to) {
                        // The share was already handed out to this
                        // peer. Give back the same one.
                        let share = shares.expose_secret()[idx.0].clone();
                        let msg = Response {
                            request_id: metadata.request_id,
                            type_: ResponseType::Share(share),
                        };
                        envelopes.push(Envelope { to, msg: msg.into() });
                    } else {
                        // We need to pick a share to hand out and
                        // persist that fact. We find the highest currently used
                        // index and add 1 or we select index 0.
                        let idx = distributed_shares
                            .values()
                            .max()
                            .cloned()
                            .map(|idx| idx.0 + 1)
                            .unwrap_or(0);

                        match shares.expose_secret().get(idx) {
                            Some(share) => {
                                // We have a share to hand out. Let's mark it as used.
                                self.distributed_shares
                                    .insert(to.clone(), ShareIdx(idx));
                                let msg = Response {
                                    request_id: metadata.request_id,
                                    type_: ResponseType::Share(share.clone()),
                                };
                                envelopes
                                    .push(Envelope { to, msg: msg.into() });

                                // We just handed out a new share Ensure we
                                // perist this state
                                persist = true
                            }
                            None => {
                                // We're fresh out of shares
                                let msg = Response {
                                    request_id: metadata.request_id,
                                    type_: Error::CannotSpareAShare.into(),
                                };
                                envelopes
                                    .push(Envelope { to, msg: msg.into() });
                            }
                        }
                    }
                }
            }
            Err(_) => {
                envelopes = self.resolve_learn_requests_with_error(
                    Error::FailedToDecryptShares,
                );
            }
        }

        (persist, envelopes)
    }

    /// An error prevented successful resolution of learn requests
    pub fn resolve_learn_requests_with_error(
        &mut self,
        error: Error,
    ) -> Vec<Envelope> {
        let mut envelopes =
            Vec::with_capacity(self.pending_learn_requests.len());
        while let Some((to, metadata)) = self.pending_learn_requests.pop_first()
        {
            let msg = Response {
                request_id: metadata.request_id,
                type_: error.into(),
            };
            envelopes.push(Envelope { to, msg: msg.into() })
        }
        envelopes
    }
}

impl StateHandler for InitialMemberState {
    fn handle_request(
        mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        request: RequestType,
    ) -> (state, Output) {
        use RequestType::*;
        let output = match request {
            Init(new_pkg) => {
                if new_pkg == self.pkg {
                    // Idempotent response given same pkg
                    Output::respond(from, request_id, ResponseType::InitAck)
                } else {
                    let rack_uuid = self.pkg.rack_uuid;
                    Output::respond(
                        from,
                        request_id,
                        Error::AlreadyInitialized { rack_uuid }.into(),
                    )
                }
            }
            InitLearner => {
                let rack_uuid = self.pkg.rack_uuid;
                Output::respond(
                    from,
                    request_id,
                    Error::AlreadyInitialized { rack_uuid }.into(),
                )
            }
            GetShare { rack_uuid } => {
                if rack_uuid != self.pkg.rack_uuid {
                    Output::respond(
                        from,
                        request_id,
                        Error::RackUuidMismatch {
                            expected: self.pkg.rack_uuid,
                            got: rack_uuid,
                        },
                    )
                } else {
                    Output::respond(
                        from,
                        request_id,
                        ResponseType::Share(self.pkg.share.clone()),
                    )
                }
            }
            Learn => {
                match &self.rack_secret_state {
                    Some(RackSecretState::Secret(rack_secret)) => {
                        // We already know the rack secret so respond to the
                        // peer.
                        send_share_response(
                            from,
                            request_id,
                            &self.pkg,
                            &mut self.distributed_shares,
                            rack_secret,
                        )
                    }
                    Some(RackSecretState::Shares(shares)) => {
                        // Register the request and try to collect enough
                        // shares to unlock the rack secret. When we have
                        // enough we will respond to the caller.
                        self.pending_learn_requests.insert(
                            from,
                            RequestMetadata { request_id, start: self.clock },
                        );
                        broadcast_share_requests(
                            &common,
                            self.pkg.rack_uuid,
                            Some(shares),
                        )
                    }
                    None => {
                        // Register the request and try to collect enough
                        // shares to unlock the rack secret. When we have
                        // enough we will respond to the caller.
                        pending_learn_requests.insert(
                            from,
                            RequestMetadata { request_id, start: self.clock },
                        );
                        // Start to track collecting shares by inserting ourself
                        self.rack_secret_state =
                            Some(RackSecretState::Shares(BTreeMap::from([(
                                common.id.clone(),
                                self.pkg.share.clone(),
                            )])));
                        broadcast_share_requests(
                            &common,
                            self.pkg.rack_uuid,
                            None,
                        )
                    }
                }
            }
        };

        // This is a terminal state
        (self.into(), output)
    }

    fn handle_response(
        mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        response: ResponseType,
    ) -> (State, Output) {
        use ResponseType::*;
        let output = match response {
            InitAck => {
                if let Some(rack_init_state) = &mut self.rack_init_state {
                    rack_init_state.acks.insert(from);
                    if rack_init_state.is_complete() {
                        // TODO: We should probably have an enum variant to indicate `Done`
                        self.rack_init_state = None;
                        return (
                            self.into(),
                            ApiOutput::RackInitComplete.into(),
                        );
                    }
                }
                Output::none()
            }
            Share(share) => self.on_share(common, from, request_id, share),
            Pkg(_) => ApiError::UnexpectedResponse {
                from,
                state: self.name(),
                request_id,
                msg: response.name(),
            }
            .into(),
            Error(error) => ApiError::ErrorResponseReceived {
                from,
                state: self.name(),
                request_id,
                error,
            }
            .into(),
        };
    }

    ///  TODO: check for pending learn request timeouts
    ///  * rack secret expiry - so we can zero it or shares
    fn tick(mut self, common: &mut FsmCommonData) -> (State, Output) {
        // Check for rack initialization timeout
        if let Some(rack_init_state) = &mut self.rack_init_state {
            if rack_init_state
                .timer_expired(common.clock, common.config.rack_init_timeout)
            {
                let unacked_peers = pkg
                    .initial_membership
                    .difference(&rack_init_state.acks)
                    .cloned()
                    .collect();
                self.rack_init_state = None;
                (
                    self.into(),
                    ApiError::RackInitTimeout { unacked_peers }.into(),
                )
            }
        }

        // Check for rack secret request timeout
        if let Some(start) = common.pending_api_rack_secret_request {
            // Check for rack secret request expiry
            if common.clock.saturating_sub(start)
                > common.config.rack_secret_request_timeout
            {
                common.pending_api_rack_secret_request = None;
                return (self.into(), ApiError::RackSecretLoadTimeout.into());
            }
        }

        (self.into(), Output::none())
    }
}

// Send a `ResponseType::Share` message once we have recomputed the rack secret
fn send_share_response(
    from: Baseboard,
    request_id: Uuid,
    pkg: &SharePkgV0,
    distributed_shares: &mut BTreeMap<Baseboard, ShareIdx>,
    rack_secret: &RackSecret,
) -> Output {
    match pkg.decrypt_shares(rack_secret) {
        Ok(shares) => {
            if let Some(idx) = distributed_shares.get(&from) {
                // The share was already handed out to this
                // peer. Give back the same one.
                let share = shares.expose_secret()[idx.0].clone();
                Output::respond(from, request_id, ResponseType::Share(share))
            } else {
                // We need to pick a share to hand out and
                // persist that fact. We find the highest currently used
                // index and add 1 or we select index 0.
                let idx = distributed_shares
                    .values()
                    .max()
                    .cloned()
                    .map(|idx| idx.0 + 1)
                    .unwrap_or(0);

                match shares.expose_secret().get(idx) {
                    Some(share) => {
                        distributed_shares.insert(from.clone(), ShareIdx(idx));
                        Output::persist_and_respond(
                            from,
                            request_id,
                            ResponseType::Share(share.clone()),
                        )
                    }
                    None => Output::respond(
                        from,
                        request_id,
                        Error::CannotSpareAShare.into(),
                    ),
                }
            }
        }
        Err(_) => Output::respond(
            from,
            request_id,
            Error::FailedToDecryptShares.into(),
        ),
    }
}
