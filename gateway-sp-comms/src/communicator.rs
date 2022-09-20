// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::error::BadResponseType;
use crate::error::Error;
use crate::error::StartupError;
use crate::management_switch::ManagementSwitch;
use crate::management_switch::SwitchPort;
use crate::single_sp::AttachedSerialConsole;
use crate::Elapsed;
use crate::SpIdentifier;
use crate::SwitchConfig;
use crate::Timeout;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::Stream;
use gateway_messages::BulkIgnitionState;
use gateway_messages::DiscoverResponse;
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::ResponseKind;
use gateway_messages::SpComponent;
use gateway_messages::SpState;
use gateway_messages::UpdateStatus;
use slog::info;
use slog::o;
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
}

impl Communicator {
    pub async fn new(
        config: SwitchConfig,
        log: &Logger,
    ) -> Result<Self, StartupError> {
        let log = log.new(o!("component" => "SpCommunicator"));
        let switch = ManagementSwitch::new(config, &log).await?;

        info!(&log, "started SP communicator");
        Ok(Self { switch })
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
    pub fn location_name(&self) -> Result<&str, Error> {
        self.switch.location_name()
    }

    fn id_to_port(&self, sp: SpIdentifier) -> Result<SwitchPort, Error> {
        self.switch.switch_port(sp)?.ok_or(Error::SpDoesNotExist(sp))
    }

    fn port_to_id(&self, port: SwitchPort) -> Result<SpIdentifier, Error> {
        self.switch.switch_port_to_id(port)
    }

    /// Returns true if we've discovered the IP address of our local ignition
    /// controller.
    ///
    /// This method exists to be polled during test setup (to wait for discovery
    /// to happen); it should not be called outside tests.
    pub fn local_ignition_controller_address_known(&self) -> bool {
        self.switch.ignition_controller().is_some()
    }

    /// Returns true if we've discovered the IP address of the specified SP.
    ///
    /// This method exists to be polled during test setup (to wait for discovery
    /// to happen); it should not be called outside tests. In particular, it
    /// panics instead of running an error if `sp` describes an SP that isn't
    /// known to this communicator or if discovery isn't complete yet.
    pub fn address_known(&self, sp: SpIdentifier) -> bool {
        let port = self.switch.switch_port(sp).unwrap().unwrap();
        self.switch.sp(port).is_some()
    }

    /// Ask the local ignition controller for the ignition state of a given SP.
    pub async fn get_ignition_state(
        &self,
        sp: SpIdentifier,
    ) -> Result<IgnitionState, Error> {
        let controller = self
            .switch
            .ignition_controller()
            .ok_or(Error::LocalIgnitionControllerAddressUnknown)?;
        let port = self.id_to_port(sp)?;
        Ok(controller.ignition_state(port.as_ignition_target()).await?)
    }

    /// Ask the local ignition controller for the ignition state of all SPs.
    pub async fn get_ignition_state_all(
        &self,
    ) -> Result<Vec<(SpIdentifier, IgnitionState)>, Error> {
        let controller = self
            .switch
            .ignition_controller()
            .ok_or(Error::LocalIgnitionControllerAddressUnknown)?;
        let bulk_state = controller.bulk_ignition_state().await?;

        // map ignition target indices back to `SpIdentifier`s for our caller
        bulk_state
            .targets
            .iter()
            .filter(|state| {
                // TODO-cleanup `state.id` should match one of the constants
                // defined in RFD 142 section 5.2.2, all of which are nonzero.
                // What does the real ignition controller return for unpopulated
                // sleds? Our simulator returns 0 for unpopulated targets;
                // filter those out.
                state.id != 0
            })
            .copied()
            .enumerate()
            .map(|(target, state)| {
                let port = self
                    .switch
                    .switch_port_from_ignition_target(target)
                    .ok_or(Error::BadIgnitionTarget(target))?;
                let id = self.port_to_id(port)?;
                Ok((id, state))
            })
            .collect()
    }

    /// Instruct the local ignition controller to perform the given `command` on
    /// `target_sp`.
    pub async fn send_ignition_command(
        &self,
        target_sp: SpIdentifier,
        command: IgnitionCommand,
    ) -> Result<(), Error> {
        let controller = self
            .switch
            .ignition_controller()
            .ok_or(Error::LocalIgnitionControllerAddressUnknown)?;
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
    ) -> Result<AttachedSerialConsole, Error> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port).ok_or(Error::SpAddressUnknown(sp))?;
        Ok(sp.serial_console_attach(component).await?)
    }

    /// Detach any existing connection to the given SP component's serial
    /// console.
    pub async fn serial_console_detach(
        &self,
        sp: SpIdentifier,
    ) -> Result<(), Error> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port).ok_or(Error::SpAddressUnknown(sp))?;
        sp.serial_console_detach().await?;
        Ok(())
    }

    /// Get the state of a given SP.
    pub async fn get_state(&self, sp: SpIdentifier) -> Result<SpState, Error> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port).ok_or(Error::SpAddressUnknown(sp))?;
        Ok(sp.state().await?)
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
    ) -> Result<(), Error> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port).ok_or(Error::SpAddressUnknown(sp))?;
        Ok(sp.start_update(component, update_id, slot, image).await?)
    }

    /// Get the status of an in-progress update.
    pub async fn update_status(
        &self,
        sp: SpIdentifier,
        component: SpComponent,
    ) -> Result<UpdateStatus, Error> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port).ok_or(Error::SpAddressUnknown(sp))?;
        Ok(sp.update_status(component).await?)
    }

    /// Abort an in-progress update.
    pub async fn update_abort(
        &self,
        sp: SpIdentifier,
        component: SpComponent,
        update_id: Uuid,
    ) -> Result<(), Error> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port).ok_or(Error::SpAddressUnknown(sp))?;
        Ok(sp.update_abort(component, update_id).await?)
    }

    /// Reset a given SP.
    pub async fn reset(&self, sp: SpIdentifier) -> Result<(), Error> {
        let port = self.id_to_port(sp)?;
        let sp = self.switch.sp(port).ok_or(Error::SpAddressUnknown(sp))?;
        sp.reset_prepare().await?;
        sp.reset_trigger().await?;
        Ok(())
    }

    /// Query all online SPs.
    ///
    /// `ignition_state` should be the state returned by a (recent) call to
    /// [`crate::communicator::Communicator::get_ignition_state_all()`].
    ///
    /// All SPs included in `ignition_state` will be yielded by the returned
    /// stream. The order in which they are yielded is undefined; the offline
    /// SPs are likely to be first, but even that is not guaranteed. The item
    /// yielded by offline SPs will be `None`; the item yielded by online SPs
    /// will be `Some(Ok(_))` if the future returned by `f` for that item
    /// completed before `timeout` or `Some(Err(_))` if not.
    ///
    /// Note that the timeout is be applied to each _element_ of the returned
    /// stream rather than the stream as a whole, allowing easy access to which
    /// SPs timed out based on the yielded value associated with those SPs.
    pub fn query_all_online_sps<F, T, Fut>(
        &self,
        ignition_state: &[(SpIdentifier, IgnitionState)],
        timeout: Timeout,
        f: F,
    ) -> impl FuturesUnorderedImpl<
        Item = (SpIdentifier, IgnitionState, Option<Result<T, Elapsed>>),
    >
    where
        F: FnMut(SpIdentifier) -> Fut + Clone,
        Fut: Future<Output = T>,
    {
        ignition_state
            .iter()
            .copied()
            .map(move |(id, state)| {
                let mut f = f.clone();
                async move {
                    let val = if state.is_powered_on() {
                        Some(timeout.timeout_at(f(id)).await)
                    } else {
                        None
                    };
                    (id, state, val)
                }
            })
            .collect::<FuturesUnordered<_>>()
    }
}

// When we send a request we expect a specific kind of response; the boilerplate
// for confirming that is a little noisy, so it lives in this extension trait.
pub(crate) trait ResponseKindExt {
    fn name(&self) -> &'static str;

    fn expect_discover(self) -> Result<DiscoverResponse, BadResponseType>;

    fn expect_ignition_state(self) -> Result<IgnitionState, BadResponseType>;

    fn expect_bulk_ignition_state(
        self,
    ) -> Result<BulkIgnitionState, BadResponseType>;

    fn expect_ignition_command_ack(self) -> Result<(), BadResponseType>;

    fn expect_sp_state(self) -> Result<SpState, BadResponseType>;

    fn expect_serial_console_attach_ack(self) -> Result<(), BadResponseType>;

    fn expect_serial_console_write_ack(self) -> Result<u64, BadResponseType>;

    fn expect_serial_console_detach_ack(self) -> Result<(), BadResponseType>;

    fn expect_update_prepare_ack(self) -> Result<(), BadResponseType>;

    fn expect_update_status(self) -> Result<UpdateStatus, BadResponseType>;

    fn expect_update_chunk_ack(self) -> Result<(), BadResponseType>;

    fn expect_update_abort_ack(self) -> Result<(), BadResponseType>;

    fn expect_sys_reset_prepare_ack(self) -> Result<(), BadResponseType>;
}

impl ResponseKindExt for ResponseKind {
    fn name(&self) -> &'static str {
        match self {
            ResponseKind::Discover(_) => response_kind_names::DISCOVER,
            ResponseKind::IgnitionState(_) => {
                response_kind_names::IGNITION_STATE
            }
            ResponseKind::BulkIgnitionState(_) => {
                response_kind_names::BULK_IGNITION_STATE
            }
            ResponseKind::IgnitionCommandAck => {
                response_kind_names::IGNITION_COMMAND_ACK
            }
            ResponseKind::SpState(_) => response_kind_names::SP_STATE,
            ResponseKind::SerialConsoleAttachAck => {
                response_kind_names::SERIAL_CONSOLE_ATTACH_ACK
            }
            ResponseKind::SerialConsoleWriteAck { .. } => {
                response_kind_names::SERIAL_CONSOLE_WRITE_ACK
            }
            ResponseKind::SerialConsoleDetachAck => {
                response_kind_names::SERIAL_CONSOLE_DETACH_ACK
            }
            ResponseKind::UpdatePrepareAck => {
                response_kind_names::UPDATE_PREPARE_ACK
            }
            ResponseKind::UpdateStatus(_) => response_kind_names::UPDATE_STATUS,
            ResponseKind::UpdateAbortAck => {
                response_kind_names::UPDATE_ABORT_ACK
            }
            ResponseKind::UpdateChunkAck => {
                response_kind_names::UPDATE_CHUNK_ACK
            }
            ResponseKind::SysResetPrepareAck => {
                response_kind_names::SYS_RESET_PREPARE_ACK
            }
        }
    }

    fn expect_discover(self) -> Result<DiscoverResponse, BadResponseType> {
        match self {
            ResponseKind::Discover(discover) => Ok(discover),
            other => Err(BadResponseType {
                expected: response_kind_names::DISCOVER,
                got: other.name(),
            }),
        }
    }

    fn expect_ignition_state(self) -> Result<IgnitionState, BadResponseType> {
        match self {
            ResponseKind::IgnitionState(state) => Ok(state),
            other => Err(BadResponseType {
                expected: response_kind_names::IGNITION_STATE,
                got: other.name(),
            }),
        }
    }

    fn expect_bulk_ignition_state(
        self,
    ) -> Result<BulkIgnitionState, BadResponseType> {
        match self {
            ResponseKind::BulkIgnitionState(state) => Ok(state),
            other => Err(BadResponseType {
                expected: response_kind_names::BULK_IGNITION_STATE,
                got: other.name(),
            }),
        }
    }

    fn expect_ignition_command_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::IgnitionCommandAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::IGNITION_COMMAND_ACK,
                got: other.name(),
            }),
        }
    }

    fn expect_sp_state(self) -> Result<SpState, BadResponseType> {
        match self {
            ResponseKind::SpState(state) => Ok(state),
            other => Err(BadResponseType {
                expected: response_kind_names::SP_STATE,
                got: other.name(),
            }),
        }
    }

    fn expect_serial_console_attach_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::SerialConsoleAttachAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::SP_STATE,
                got: other.name(),
            }),
        }
    }

    fn expect_serial_console_write_ack(self) -> Result<u64, BadResponseType> {
        match self {
            ResponseKind::SerialConsoleWriteAck {
                furthest_ingested_offset,
            } => Ok(furthest_ingested_offset),
            other => Err(BadResponseType {
                expected: response_kind_names::SP_STATE,
                got: other.name(),
            }),
        }
    }

    fn expect_serial_console_detach_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::SerialConsoleDetachAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::SP_STATE,
                got: other.name(),
            }),
        }
    }

    fn expect_update_prepare_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::UpdatePrepareAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::UPDATE_PREPARE_ACK,
                got: other.name(),
            }),
        }
    }

    fn expect_update_status(self) -> Result<UpdateStatus, BadResponseType> {
        match self {
            ResponseKind::UpdateStatus(status) => Ok(status),
            other => Err(BadResponseType {
                expected: response_kind_names::UPDATE_STATUS,
                got: other.name(),
            }),
        }
    }

    fn expect_update_chunk_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::UpdateChunkAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::UPDATE_CHUNK_ACK,
                got: other.name(),
            }),
        }
    }

    fn expect_update_abort_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::UpdateAbortAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::UPDATE_ABORT_ACK,
                got: other.name(),
            }),
        }
    }

    fn expect_sys_reset_prepare_ack(self) -> Result<(), BadResponseType> {
        match self {
            ResponseKind::SysResetPrepareAck => Ok(()),
            other => Err(BadResponseType {
                expected: response_kind_names::SYS_RESET_PREPARE_ACK,
                got: other.name(),
            }),
        }
    }
}

mod response_kind_names {
    pub(super) const DISCOVER: &str = "discover";
    pub(super) const IGNITION_STATE: &str = "ignition_state";
    pub(super) const BULK_IGNITION_STATE: &str = "bulk_ignition_state";
    pub(super) const IGNITION_COMMAND_ACK: &str = "ignition_command_ack";
    pub(super) const SP_STATE: &str = "sp_state";
    pub(super) const SERIAL_CONSOLE_ATTACH_ACK: &str =
        "serial_console_attach_ack";
    pub(super) const SERIAL_CONSOLE_WRITE_ACK: &str =
        "serial_console_write_ack";
    pub(super) const SERIAL_CONSOLE_DETACH_ACK: &str =
        "serial_console_detach_ack";
    pub(super) const UPDATE_PREPARE_ACK: &str = "update_prepare_ack";
    pub(super) const UPDATE_STATUS: &str = "update_status";
    pub(super) const UPDATE_ABORT_ACK: &str = "update_abort_ack";
    pub(super) const UPDATE_CHUNK_ACK: &str = "update_chunk_ack";
    pub(super) const SYS_RESET_PREPARE_ACK: &str = "sys_reset_prepare_ack";
}
