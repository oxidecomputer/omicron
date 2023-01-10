// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use std::collections::HashMap;

use crate::error::ConfigError;
use crate::error::SpCommsError;
use crate::management_switch::ManagementSwitch;
use crate::management_switch::SpIdentifier;
use crate::management_switch::SwitchConfig;
use crate::management_switch::SwitchPort;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::Stream;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::PowerState;
use gateway_messages::SpComponent;
use gateway_messages::SpState;
use gateway_messages::UpdateStatus;
use gateway_sp_comms::AttachedSerialConsole;
use gateway_sp_comms::HostPhase2Provider;
use gateway_sp_comms::SingleSp;
use gateway_sp_comms::SpInventory;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use uuid::Uuid;

/// Helper trait that allows us to return an `impl FuturesUnordered<_>` where
/// the caller can call `.is_empty()` without knowing the type of the future
/// inside the collection.
pub trait FuturesUnorderedImpl: Stream + Unpin {
    fn is_empty(&self) -> bool;
}

impl<Fut> FuturesUnorderedImpl for FuturesUnordered<Fut>
where
    Fut: Future,
{
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

#[derive(Debug)]
pub struct Communicator {
    switch: ManagementSwitch,
    log: Logger,
}

impl Communicator {
    pub async fn new<T: HostPhase2Provider + Clone>(
        config: SwitchConfig,
        host_phase2_provider: T,
        log: &Logger,
    ) -> Result<Self, ConfigError> {
        let log = log.new(o!("component" => "SpCommunicator"));
        let switch =
            ManagementSwitch::new(config, host_phase2_provider, &log).await?;

        info!(&log, "started SP communicator");
        Ok(Self { switch, log })
    }

    /// Have we completed the discovery process to know how to map logical SP
    /// positions to switch ports?
    pub fn is_discovery_complete(&self) -> bool {
        self.switch.is_discovery_complete()
    }

    /// Get the name of our location.
    ///
    /// This matches one of the names specified as a possible location in the
    /// configuration we were given.
    pub fn location_name(&self) -> Result<&str, SpCommsError> {
        self.switch.location_name()
    }

    fn id_to_port(&self, sp: SpIdentifier) -> Result<SwitchPort, SpCommsError> {
        self.switch.switch_port(sp)?.ok_or(SpCommsError::SpDoesNotExist(sp))
    }

    fn port_to_id(
        &self,
        port: SwitchPort,
    ) -> Result<SpIdentifier, SpCommsError> {
        self.switch.switch_port_to_id(port)
    }

    pub fn sp_by_id(
        &self,
        id: SpIdentifier,
    ) -> Result<&SingleSp, SpCommsError> {
        let port = self.id_to_port(id)?;
        Ok(self.switch.sp(port))
    }

    /// Ask the local ignition controller for the ignition state of a given SP.
    pub async fn get_ignition_state(
        &self,
        sp: SpIdentifier,
    ) -> Result<IgnitionState, SpCommsError> {
        let controller = self.switch.ignition_controller();
        let port = self.id_to_port(sp)?;
        Ok(controller.ignition_state(port.as_ignition_target()).await?)
    }

    /// Ask the local ignition controller for the ignition state of all SPs.
    ///
    /// TODO: This _does not_ return the ignition state for our local ignition
    /// controller! If this function returns at all, it's on. Is that good
    /// enough? Should we try to query the other sidecar?
    pub async fn get_ignition_state_all(
        &self,
    ) -> Result<HashMap<SpIdentifier, IgnitionState>, SpCommsError> {
        let controller = self.switch.ignition_controller();
        let bulk_state = controller.bulk_ignition_state().await?;

        // map ignition target indices back to `SpIdentifier`s for our caller
        bulk_state
            .into_iter()
            .enumerate()
            .filter_map(|(target, state)| {
                // If the SP returns an ignition target we don't have a port
                // for, discard it. This _shouldn't_ happen, but may if:
                //
                // 1. We're getting bogus messages from the SP.
                // 2. We're misconfigured and don't know about all ports.
                //
                // Case 2 may happen intentionally during development and
                // testing.
                match self.switch.switch_port_from_ignition_target(target) {
                    Some(port) =>
                        Some(self.port_to_id(port).map(|id| (id, state))),
                    None => {
                        warn!(self.log, "ignoring unknown ignition target {target} returned by SP");
                        None
                    }
                }
            })
            .collect()
    }

    /// Instruct the local ignition controller to perform the given `command` on
    /// `target_sp`.
    pub async fn send_ignition_command(
        &self,
        target_sp: SpIdentifier,
        command: IgnitionCommand,
    ) -> Result<(), SpCommsError> {
        let controller = self.switch.ignition_controller();
        let target = self.id_to_port(target_sp)?.as_ignition_target();
        Ok(controller.ignition_command(target, command).await?)
    }

    /// Attach to the serial console of `sp`.
    // TODO-cleanup: This currently does not actually contact the target SP; it
    // sets up the websocket connection in the current process which knows how
    // to relay any information sent or received on that connection to the SP
    // via UDP. SPs will continuously broadcast any serial console data, even if
    // there is no attached client. Maybe this is fine, since the serial console
    // shouldn't be noisy without a corresponding client driving it?
    //
    // TODO Because this method doesn't contact the target SP, it succeeds even
    // if we don't know the IP address of that SP (yet, or possibly ever)! The
    // connection will start working if we later discover the address, but this
    // is probably not the behavior we want.
    pub async fn serial_console_attach(
        &self,
        sp: SpIdentifier,
        component: SpComponent,
    ) -> Result<AttachedSerialConsole, SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        Ok(sp.serial_console_attach(component).await?)
    }

    /// Detach any existing connection to the given SP component's serial
    /// console.
    pub async fn serial_console_detach(
        &self,
        sp: SpIdentifier,
    ) -> Result<(), SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        sp.serial_console_detach().await?;
        Ok(())
    }

    /// Get the state of a given SP.
    pub async fn get_state(
        &self,
        sp: SpIdentifier,
    ) -> Result<SpState, SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        Ok(sp.state().await?)
    }

    /// Get the inventory of a given SP.
    pub async fn inventory(
        &self,
        sp: SpIdentifier,
    ) -> Result<SpInventory, SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        Ok(sp.inventory().await?)
    }

    /// Start sending an update payload to the given SP.
    ///
    /// This function will return before the update is complete! Once the SP
    /// acknowledges that we want to apply an update, we spawn a background task
    /// to stream the update to the SP and then return. Poll the status of the
    /// update via [`Self::update_status()`].
    ///
    /// # Panics
    ///
    /// Panics if `image.is_empty()`.
    pub async fn start_update(
        &self,
        sp: SpIdentifier,
        component: SpComponent,
        update_id: Uuid,
        slot: u16,
        image: Vec<u8>,
    ) -> Result<(), SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        Ok(sp.start_update(component, update_id, slot, image).await?)
    }

    /// Get the status of an in-progress update.
    pub async fn update_status(
        &self,
        sp: SpIdentifier,
        component: SpComponent,
    ) -> Result<UpdateStatus, SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        Ok(sp.update_status(component).await?)
    }

    /// Abort an in-progress update.
    pub async fn update_abort(
        &self,
        sp: SpIdentifier,
        component: SpComponent,
        update_id: Uuid,
    ) -> Result<(), SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        Ok(sp.update_abort(component, update_id).await?)
    }

    /// Get the current (SP-controlled) power state.
    pub async fn power_state(
        &self,
        sp: SpIdentifier,
    ) -> Result<PowerState, SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        Ok(sp.power_state().await?)
    }

    /// Set the current (SP-controlled) power state.
    pub async fn set_power_state(
        &self,
        sp: SpIdentifier,
        power_state: PowerState,
    ) -> Result<(), SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        Ok(sp.set_power_state(power_state).await?)
    }

    /// Reset a given SP.
    pub async fn reset(&self, sp: SpIdentifier) -> Result<(), SpCommsError> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port);
        sp.reset_prepare().await?;
        sp.reset_trigger().await?;
        Ok(())
    }

    /// Query all SPs.
    pub fn query_all_sps<'a, F, T, Fut>(
        &'a self,
        mut f: F,
    ) -> Result<impl FuturesUnorderedImpl<Item = T>, SpCommsError>
    where
        F: FnMut(SpIdentifier, &'a SingleSp) -> Fut + Clone + 'a,
        Fut: Future<Output = T>,
    {
        let all_sps = self.switch.all_sps()?;
        Ok(all_sps
            .map(move |(id, sp)| f(id, sp))
            .collect::<FuturesUnordered<_>>())
    }
}
