// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fetching transceiver state from the SP.

use gateway_types::component::SpIdentifier;
use iddqd::{IdOrdItem, IdOrdMap, id_ord_map, id_upcast};
use sled_agent_types::early_networking::SwitchSlot;
use slog::{Logger, debug, error};
use slog_error_chain::InlineErrorChain;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, Interval},
};
use transceiver_controller::{
    ConfigBuilder, Controller, Error, ModuleId, ModuleResult,
};
use transceiver_controller::{SpRequest, message::ExtendedStatus};
use wicket_common::inventory::{SpType, Transceiver};

/// Type alias for a map of all transceivers on each switch.
pub type TransceiverMap = IdOrdMap<SwitchTransceivers>;

/// Item in a [`TransceiverMap`].
///
/// This tracks the state of all transceivers on a single switch.
#[derive(Clone, Debug)]
pub struct SwitchTransceivers {
    /// The switch slot.
    pub switch: SwitchSlot,
    /// The list of transceivers on this switch.
    pub transceivers: Vec<Transceiver>,
    /// The last time we fetched transceivers from this switch.
    pub updated_at: Instant,
}

impl IdOrdItem for SwitchTransceivers {
    type Key<'a> = SwitchSlot;

    fn key(&self) -> Self::Key<'_> {
        self.switch
    }

    id_upcast!();
}

// Queue size for passing messages between transceiver fetch task.
const CHANNEL_CAPACITY: usize = 4;

// Duration on which we poll transceivers ourselves, independent of any
// requests.
const TRANSCEIVER_POLL_INTERVAL: Duration = Duration::from_secs(5);

// IP interface we use when polling transceivers on the local switch.
//
// NOTE: This always refers to _our_ switch, the one we're currently on,
// regardless of whether we're running on switch0 or switch1.
//
// We will need to change these if we address
// https://github.com/oxidecomputer/dendrite/issues/221.
const LOCAL_SWITCH_SP_INTERFACE: &str = "sidecar0";

// IP interface we use when polling transceivers on the other switch.
//
// NOTE: This always refers to _the other_ switch, the one we're not currently
// running on, regardless of whether we're running on switch0 or switch1.
const OTHER_SWITCH_SP_INTERFACE: &str = "sidecar1";

#[derive(Clone, Debug)]
pub enum GetTransceiversResponse {
    Response { transceivers: TransceiverMap },
    Unavailable,
}

/// Handle for interacting with the transceiver manager.
pub struct Handle {
    switch_slot_tx: watch::Sender<Option<SwitchSlot>>,
    transceivers: Arc<Mutex<GetTransceiversResponse>>,
}

impl Handle {
    /// Notify the transceiver manager that we've learned our switch slot.
    ///
    /// # Panics
    ///
    /// This panics if called with an `SpIdentifier` that doesn't have an
    /// `SpType::Switch`.
    pub(crate) fn set_local_switch_id(&self, switch: SpIdentifier) {
        let SpIdentifier { slot, typ: SpType::Switch } = switch else {
            panic!("Should only be called with SpType::Switch");
        };
        let slot = match slot {
            0 => SwitchSlot::Switch0,
            1 => SwitchSlot::Switch1,
            _ => unreachable!(),
        };
        self.switch_slot_tx
            .send(Some(slot))
            .expect("Should always have a receiver");
    }

    /// Get the current transceiver state, if we know it.
    pub(crate) fn get_transceivers(&self) -> GetTransceiversResponse {
        self.transceivers.lock().unwrap().clone()
    }
}

pub struct Manager {
    log: Logger,
    switch_slot_tx: watch::Sender<Option<SwitchSlot>>,
    switch_slot_rx: watch::Receiver<Option<SwitchSlot>>,
    transceivers: Arc<Mutex<GetTransceiversResponse>>,
}

impl Manager {
    pub(crate) fn new(log: &Logger) -> Self {
        let log =
            log.new(slog::o!("component" => "wicketd TransceiverManager"));
        let (switch_slot_tx, switch_slot_rx) = watch::channel(None);
        let transceivers =
            Arc::new(Mutex::new(GetTransceiversResponse::Unavailable));
        Self { log, transceivers, switch_slot_tx, switch_slot_rx }
    }

    pub(crate) fn get_handle(&self) -> Handle {
        Handle {
            switch_slot_tx: self.switch_slot_tx.clone(),
            transceivers: self.transceivers.clone(),
        }
    }

    pub(crate) async fn run(mut self) {
        // First, we need to wait until we know the switch slot.
        //
        // The watch Receiver was created with `None`, which is considered seen.
        // We've never called any other borrowing method between the creation
        // and here, so changed() will wait until we get something new.
        debug!(self.log, "waiting to learn our switch slot");
        let our_switch_slot = loop {
            if self.switch_slot_rx.changed().await.is_err() {
                slog::warn!(
                    self.log,
                    "failed to wait for new switch slot change \
                    notification, exiting";
                );
                return;
            };
            match *self.switch_slot_rx.borrow_and_update() {
                Some(loc) => break loc,
                None => continue,
            }
        };
        let other_switch_slot = our_switch_slot.other();
        debug!(
            self.log,
            "determined our switch locations, spawning transceiver fetch tasks";
            "our_switch" => ?our_switch_slot,
            "other_switch" => ?other_switch_slot,
        );

        // Now, spawn a task for each switch.
        //
        // The local switch always uses `sidecar0` as the interface and the
        // remote uses `sidecar1`. But we now know which _switch slot_ that maps
        // to.
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(fetch_transceivers_from_one_switch(
            self.log.clone(),
            tx.clone(),
            our_switch_slot,
            LOCAL_SWITCH_SP_INTERFACE,
        ));
        tokio::spawn(fetch_transceivers_from_one_switch(
            self.log.clone(),
            tx.clone(),
            other_switch_slot,
            OTHER_SWITCH_SP_INTERFACE,
        ));

        // Now, wait for updates from the fetching tasks and aggregate,
        // populate our own view of the transceivers from it.
        loop {
            let Some(TransceiverUpdate {
                switch_slot,
                transceivers: these_transceivers,
                updated_at,
            }) = rx.recv().await
            else {
                error!(self.log, "all transceiver fetch tasks have exited");
                return;
            };
            let update = SwitchTransceivers {
                switch: switch_slot,
                transceivers: these_transceivers,
                updated_at,
            };
            let mut transceivers_by_switch = self.transceivers.lock().unwrap();
            match &mut *transceivers_by_switch {
                GetTransceiversResponse::Response { transceivers } => {
                    transceivers.insert_overwrite(update);
                }
                GetTransceiversResponse::Unavailable => {
                    let all_transceivers = id_ord_map! { update };
                    *transceivers_by_switch =
                        GetTransceiversResponse::Response {
                            transceivers: all_transceivers,
                        };
                }
            }
        }
    }
}

// An update from one of the transceiver fetching tasks about the transceivers
// it has seen.
struct TransceiverUpdate {
    switch_slot: SwitchSlot,
    transceivers: Vec<Transceiver>,
    updated_at: Instant,
}

// Task fetching all transceiver state from one switch.
async fn fetch_transceivers_from_one_switch(
    log: Logger,
    mut tx: mpsc::Sender<TransceiverUpdate>,
    switch_slot: SwitchSlot,
    interface: &'static str,
) {
    let mut check_interval = tokio::time::interval(TRANSCEIVER_POLL_INTERVAL);
    debug!(
        log,
        "starting transceiver fetch task";
        "interface" => interface,
        "poll_interval" => ?TRANSCEIVER_POLL_INTERVAL,
    );

    // Spawn a task to swallow requests from the SP.
    let (sp_request_tx, sp_request_rx) = mpsc::channel(CHANNEL_CAPACITY);
    tokio::spawn(drop_sp_transceiver_requests(
        interface,
        log.clone(),
        sp_request_rx,
    ));

    // Enter the main loop.
    //
    // This loop consists of two inner ones, which:
    //
    // - build the transciever controller, and
    // - fetch the state of the transceivers and forward it
    //
    // These are in an outer loop because it's possible that the actual
    // `Controller` object we use to talk to the transceivers becomes unusable.
    // It binds a UDP port on the management network, over which it talks to the
    // switch's SPs for information about the transceivers.
    //
    // Unfortunately, we're racing with Dendrite here. `wicketd` is actively
    // using the management network to get transceiver state. Meanwhile, `dpd`
    // is using the management network to bootstrap the startup fo the switch
    // zone, specifically to fetch the base MAC addresses for all our switch
    // ports from the SP.
    //
    // That bootstrapping means that we can possibly bind a UDP port on an
    // address that Dendrite is about to tear down, when we get those real MAC
    // addresses. That renders unusable any controller built before that
    // bootstrapping process completes. The outer loop is to detect this case,
    // and rebuild the controller when we need to.
    loop {
        let controller = build_transceiver_controller(
            &log,
            interface,
            &mut check_interval,
            sp_request_tx.clone(),
        )
        .await;
        debug!(log, "created transceiver controller, starting poll loop");
        let err = poll_transceiver_state(
            controller,
            &log,
            &mut tx,
            switch_slot,
            interface,
            &mut check_interval,
        )
        .await;
        error!(
            log,
            "network error fetching transceiver state, \
            controller we be rebuilt";
            "interface" => interface,
            "switch_slot" => ?switch_slot,
            "error" => InlineErrorChain::new(&err),
        );
    }
}

// Poll transceiver state forever, forwarding updates.
//
// This only returns if polling fails because the transceiver controller's UDP
// socket appears broken. Other kinds of errors, like timeouts, are swallowed
// and the polling loop continues.
async fn poll_transceiver_state(
    controller: Controller,
    log: &Logger,
    tx: &mut mpsc::Sender<TransceiverUpdate>,
    switch_slot: SwitchSlot,
    interface: &'static str,
    check_interval: &mut Interval,
) -> Error {
    loop {
        match fetch_transceiver_state(&controller).await {
            Ok(transceivers) => {
                debug!(
                    log,
                    "fetch transceiver state";
                    "state" => ?transceivers,
                );
                let update = TransceiverUpdate {
                    switch_slot,
                    transceivers,
                    updated_at: Instant::now(),
                };
                if tx.try_send(update).is_err() {
                    error!(
                        log,
                        "failed to send new transceiver state to manager",
                    );
                }
            }
            Err(e) if is_network_error(&e) => return e,
            Err(e) => error!(
                log,
                "failed to fetch transceiver state";
                "interface" => interface,
                "error" => InlineErrorChain::new(&e),
            ),
        }
        check_interval.tick().await;
    }
}

// Return true if this transceiver error appears to be a networking error that
// requries we rebuild the transceiver controller itself.
fn is_network_error(e: &Error) -> bool {
    match e {
        // It's tempting to match on the error kind here. But it's also hard to
        // predict which errors are actually permanent. Since rebuilding the
        // controller is pretty cheap, we're being conservative.
        Error::Io(_) | Error::BadInterface(_) => true,
        Error::Protocol(_)
        | Error::MessageRequiresData
        | Error::MaxRetries(_)
        | Error::MaxFaultMessages(_)
        | Error::UnexpectedMessage(_)
        | Error::InvalidWriteData { .. }
        | Error::InvalidPowerStateTransition
        | Error::Mac(_)
        | Error::Transceiver(_)
        | Error::ByteOutOfRange(_) => false,
    }
}

// Create a transceiver controller, retrying forever until success.
async fn build_transceiver_controller(
    log: &Logger,
    interface: &str,
    check_interval: &mut Interval,
    sp_request_tx: mpsc::Sender<SpRequest>,
) -> Controller {
    // First, setup the transceiver controller.
    let controller = loop {
        check_interval.tick().await;
        // NOTE: We bind any ephemeral port here, since we cannot choose the
        // default (that's used by Dendrite). This doesn't affect functionality,
        // in any case, since the SP doesn't send us unsolicited messages.
        let config = match ConfigBuilder::new(interface).port(0).build() {
            Ok(c) => c,
            Err(e) => {
                error!(
                    log,
                    "failed to create transceiver controller configuration";
                    "interface" => interface,
                    "error" => InlineErrorChain::new(&e),
                );
                continue;
            }
        };

        match Controller::new(
            config,
            log.new(slog::o!("component" => "transceiver-controller")),
            sp_request_tx.clone(),
        )
        .await
        {
            Ok(c) => break c,
            Err(e) => {
                error!(
                    log,
                    "failed to create transceiver controller";
                    "interface" => interface,
                    "error" => InlineErrorChain::new(&e),
                );
                continue;
            }
        };
    };
    controller
}

// A loop that just drops any messages we get from the SP.
//
// There shouldn't be any such requests today, so we'll warn and drop them.
async fn drop_sp_transceiver_requests(
    interface: &'static str,
    log: Logger,
    mut sp_request_rx: mpsc::Receiver<SpRequest>,
) {
    loop {
        let Some(req) = sp_request_rx.recv().await else {
            debug!(log, "SP transceiver request channel closed, exiting");
            return;
        };
        slog::warn!(
            log,
            "received unexpected transceiver request from SP";
            "request" => ?req.request,
            "interface" => interface,
        );
        if req.response_tx.try_send(Ok(None)).is_ok() {
            debug!(
                log,
                "sent reply to transceiver controller to drop \
                the SP request"
            );
        } else {
            error!(log, "failed to send reply to transceiver controller");
        }
    }
}

async fn fetch_transceiver_state(
    controller: &Controller,
) -> Result<Vec<Transceiver>, Error> {
    // Start by fetching the status of all modules.
    //
    // Each operation is fallible, and each module can fail independently.
    // Nonetheless, ask for all the data from all _present_ modules. That lets
    // us collect as much information as we can about every module, even if
    // there are failures to access some of its data.
    let all_status = controller.extended_status(ModuleId::all()).await?;

    // From here, let's only address those which are present.
    let present_modules = ModuleId::from_index_iter(
        all_status
            .iter()
            .filter(|(_, st)| st.contains(ExtendedStatus::PRESENT))
            .map(|(p, _st)| p),
    )
    .unwrap();

    // Collect all available data.
    let all_vendor_info = controller.vendor_info(present_modules).await?;
    let all_power = controller.power(present_modules).await?;
    let all_datapaths = controller.datapath(present_modules).await?;
    let all_monitors = controller.monitors(present_modules).await?;

    // Now, combine everything.
    //
    // For each operation, we'll record either the successful data or the error.
    // This could lead to a lot of duplicated error messages, but at this point
    // we'd rather be explicit, and it's possible for errors to be different.
    let mut out =
        Vec::with_capacity(present_modules.selected_transceiver_count());
    for i in present_modules.to_indices() {
        let tr = Transceiver {
            port: format!("qsfp{i}"),
            power: result_copied(&all_power, i),
            vendor: result_cloned(&all_vendor_info, i),
            status: result_copied(&all_status, i),
            datapath: result_cloned(&all_datapaths, i),
            monitors: result_cloned(&all_monitors, i),
        };
        out.push(tr);
    }

    Ok(out)
}

fn result_copied<T: Copy>(res: &ModuleResult<T>, i: u8) -> Result<T, String> {
    res.nth(i).copied().ok_or_else(|| {
        res.failures
            .nth(i)
            .map(|e| e.to_string())
            .unwrap_or_else(|| String::from("Unknown failure"))
    })
}

fn result_cloned<T: Clone>(res: &ModuleResult<T>, i: u8) -> Result<T, String> {
    res.nth(i).cloned().ok_or_else(|| {
        res.failures
            .nth(i)
            .map(|e| e.to_string())
            .unwrap_or_else(|| String::from("Unknown failure"))
    })
}
