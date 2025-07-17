// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An [`crate::FsmState`] for a new node with no persistent state yet.

use crate::validators::{ReconfigurationError, ValidatedReconfigureMsg};
use crate::{
    Configuration, Epoch, FsmCtxApi, FsmState, PeerMsg, PlatformId,
    ReconfigureMsg,
};
use slog::{Logger, error, o, warn};
use std::time::Instant;

use super::coordinating::Coordinating;

pub struct Uninitialized {
    log: Logger,
}

impl Uninitialized {
    pub fn new(log: &Logger) -> Uninitialized {
        Uninitialized { log: log.new(o!("state" => "uninitialized")) }
    }

    // This is a helper to allow short circuiting on errors without moving
    // self.
    fn coordinate_reconfig_impl(
        &self,
        ctx: &mut dyn FsmCtxApi,
        msg: ReconfigureMsg,
    ) -> Result<Box<dyn FsmState>, ReconfigurationError> {
        let last_reconfig_msg = None;
        let Some(validated_msg) = ValidatedReconfigureMsg::new(
            &self.log,
            ctx.platform_id(),
            msg,
            ctx.persistent_state().into(),
            last_reconfig_msg,
        )?
        else {
            unreachable!(
                "Saw idempotent reconfiguration in uninitialized state"
            )
        };

        // Transition to the `Coordinating` state
        let state = Box::new(Coordinating::new_uninitialized(
            &self.log,
            ctx,
            validated_msg,
        )?);

        Ok(state)
    }
}

impl FsmState for Uninitialized {
    fn name(&self) -> &'static str {
        "uninitialized"
    }

    fn handle(
        &mut self,
        ctx: &mut dyn FsmCtxApi,
        from: PlatformId,
        msg: PeerMsg,
    ) {
        todo!()
    }

    fn tick(&mut self, ctx: &mut dyn super::FsmCtxApi) {
        todo!()
    }

    fn coordinate_reconfiguration(
        self: Box<Self>,
        ctx: &mut dyn FsmCtxApi,
        msg: ReconfigureMsg,
    ) -> Result<Box<dyn FsmState>, (Box<dyn FsmState>, ReconfigurationError)>
    {
        let state = self
            .coordinate_reconfig_impl(ctx, msg)
            .map_err(|e| (self as Box<dyn FsmState>, e))?;

        Ok(state)
    }
}
