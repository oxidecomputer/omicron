// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::InitialMember`

use std::collections::BTreeMap;

use super::share_pkg::{LearnedSharePkg, SharePkg};
use crate::trust_quorum::RackSecret;

use super::fsm::StateHandler;
use super::fsm_output::{ApiError, ApiOutput, Output};
use super::messages::{Envelope, Error, RequestType, Response, ResponseType};
use super::state::{
    FsmCommonData, RackInitState, RequestMetadata, ShareIdx, State,
};
use secrecy::{ExposeSecret, Secret};
use sled_hardware::Baseboard;
use uuid::Uuid;

#[derive(Debug)]
pub struct InitialMemberState {
    pub pkg: SharePkg,

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
}

impl InitialMemberState {
    pub fn new(
        pkg: SharePkg,
        distributed_shares: BTreeMap<Baseboard, ShareIdx>,
    ) -> Self {
        InitialMemberState {
            pkg,
            distributed_shares,
            rack_init_state: None,
            pending_learn_requests: BTreeMap::new(),
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
        // The RackSecret state needs this deadline to know how long to keep
        // the secret
        // TODO: Use a separate timeout for this purpose?
        let rack_secret_expiry =
            common.clock + common.config.rack_secret_request_timeout;

        let mut output = common.rack_secret_state.on_share(
            from,
            request_id,
            share,
            rack_secret_expiry,
        );

        if let Some(Ok(ApiOutput::RackSecret(rack_secret))) =
            &mut output.api_output
        {
            // We have the rack secret as part of `output`. We may also
            // have learn request that we need to resolve now.
            //
            // Note that we only persist if there are learn requests which
            // cause an update to persistent state by handing out a share to the requester.
            let (persist, envelopes) =
                self.resolve_learn_requests(&rack_secret);
            output.envelopes.extend_from_slice(&envelopes);
            output.persist = persist;
        }
        output
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
        match self.pkg.decrypt_shares(rack_secret) {
            Ok(shares) => {
                while let Some((to, metadata)) =
                    self.pending_learn_requests.pop_first()
                {
                    let Output {
                        persist: persist_once,
                        envelopes: envelopes_once,
                        ..
                    } = send_share_response(
                        to.clone(),
                        metadata.request_id,
                        &self.pkg,
                        &mut self.distributed_shares,
                        &shares,
                    );

                    if persist_once {
                        persist = true;
                    }
                    envelopes.extend(envelopes_once);
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
    ) -> (State, Output) {
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
            GetShare { rack_uuid } => {
                if rack_uuid != self.pkg.rack_uuid {
                    Output::respond(
                        from,
                        request_id,
                        Error::RackUuidMismatch {
                            expected: self.pkg.rack_uuid,
                            got: rack_uuid,
                        }
                        .into(),
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
                let expiry =
                    common.clock + common.config.rack_secret_request_timeout;
                let output = common.rack_secret_state.load(
                    self.pkg.rack_uuid,
                    &common.peers,
                    &common.id,
                    &self.pkg.share,
                    expiry,
                    self.pkg.threshold.into(),
                    &self.pkg.share_digests,
                );

                if let Some(Ok(ApiOutput::RackSecret(rack_secret))) =
                    &output.api_output
                {
                    // We already know the rack secret so respond to the
                    // peer.
                    decrypt_and_send_share_response(
                        from,
                        request_id,
                        &self.pkg,
                        &mut self.distributed_shares,
                        rack_secret,
                    )
                } else {
                    let expiry = common.clock + common.config.learn_timeout;
                    self.pending_learn_requests
                        .insert(from, RequestMetadata { request_id, expiry });
                    output
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
                    if rack_init_state.on_ack(from) {
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

        // This is a terminal state
        (self.into(), output)
    }

    fn tick(mut self, common: &mut FsmCommonData) -> (State, Output) {
        // Check for rack initialization timeout
        if let Some(rack_init_state) = &mut self.rack_init_state {
            if rack_init_state
                .timer_expired(common.clock, common.config.rack_init_timeout)
            {
                let unacked_peers = rack_init_state.unacked_peers();
                *rack_init_state = RackInitState::Timeout;
                return (
                    self.into(),
                    ApiError::RackInitTimeout { unacked_peers }.into(),
                );
            }
        }
        self.pending_learn_requests
            .retain(|_, metadata| metadata.expiry >= common.clock);

        (self.into(), common.rack_secret_state.on_tick(common.clock))
    }

    fn on_connect(
        &mut self,
        common: &mut FsmCommonData,
        peer: Baseboard,
    ) -> Output {
        common.on_connect(peer, self.pkg.rack_uuid)
    }

    fn on_disconnect(
        &mut self,
        _common: &mut FsmCommonData,
        _peer: Baseboard,
    ) -> Output {
        // TODO: Discard any learn requests from this peer?
        // The will expire anyway on timeout, but they may eat a share if the
        //timeout doesn't occur
        Output::none()
    }
}

// Send a `ResponseType::Share` message once we have recomputed the rack secret
fn decrypt_and_send_share_response(
    from: Baseboard,
    request_id: Uuid,
    pkg: &SharePkg,
    distributed_shares: &mut BTreeMap<Baseboard, ShareIdx>,
    rack_secret: &RackSecret,
) -> Output {
    match pkg.decrypt_shares(rack_secret) {
        Ok(shares) => send_share_response(
            from,
            request_id,
            pkg,
            distributed_shares,
            &shares,
        ),
        Err(_) => Output::respond(
            from,
            request_id,
            Error::FailedToDecryptShares.into(),
        ),
    }
}

fn send_share_response(
    from: Baseboard,
    request_id: Uuid,
    pkg: &SharePkg,
    distributed_shares: &mut BTreeMap<Baseboard, ShareIdx>,
    shares: &Secret<Vec<Vec<u8>>>,
) -> Output {
    if let Some(idx) = distributed_shares.get(&from) {
        // The share was already handed out to this
        // peer. Give back the same one.
        let share = shares.expose_secret()[idx.0].clone();
        let learned_pkg = LearnedSharePkg {
            rack_uuid: pkg.rack_uuid,
            epoch: pkg.epoch,
            threshold: pkg.threshold,
            share: share.clone(),
            share_digests: pkg.share_digests.clone(),
        };
        Output::respond(from, request_id, ResponseType::Pkg(learned_pkg))
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
                let learned_pkg = LearnedSharePkg {
                    rack_uuid: pkg.rack_uuid,
                    epoch: pkg.epoch,
                    threshold: pkg.threshold,
                    share: share.clone(),
                    share_digests: pkg.share_digests.clone(),
                };
                Output::persist_and_respond(
                    from,
                    request_id,
                    ResponseType::Pkg(learned_pkg),
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
