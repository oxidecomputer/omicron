// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::internal::shared::SwitchLocation;
use slog::o;
use slog::Logger;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::oneshot;
use wicket_common::preflight_check::EventBuffer;
use wicket_common::preflight_check::EventReport;
use wicket_common::rack_setup::UserSpecifiedRackNetworkConfig;

mod uplink;

#[derive(Debug)]
pub(crate) struct PreflightCheckerHandler {
    request_tx: flume::Sender<PreflightCheck>,
    uplink_event_buffer: Arc<Mutex<Option<EventBuffer>>>,
}

impl PreflightCheckerHandler {
    pub(crate) fn new(log: &Logger) -> Self {
        // We intentionally create a bounded channel of size 0 (aka a
        // "rendezvous channel"). We only ever want one preflight check to be in
        // progress, and this allows requests to start to fail if a check is
        // still running.
        let (request_tx, request_rx) = flume::bounded(0);
        let uplink_event_buffer = Arc::new(Mutex::new(None));

        tokio::spawn(preflight_task_main(
            request_rx,
            Arc::clone(&uplink_event_buffer),
            log.new(o!("component" => "PreflightChecker")),
        ));

        Self { request_tx, uplink_event_buffer }
    }

    pub(crate) async fn uplink_start(
        &self,
        network_config: UserSpecifiedRackNetworkConfig,
        dns_servers: Vec<IpAddr>,
        ntp_servers: Vec<String>,
        our_switch_location: SwitchLocation,
        dns_name_to_query: Option<String>,
    ) -> Result<(), PreflightCheckerBusy> {
        let (check_started_tx, check_started_rx) = oneshot::channel();

        // Attempt to message the task running `preflight_task_main`.
        // `self.request_tx` is a 0-size bounded channel, so this will fail if
        // the task isn't currently waiting for a message (i.e., it either
        // _just_ started, which is exceedingly unlikely, or it's busy handling
        // a previous request, which is the expected error here).
        self.request_tx
            .try_send(PreflightCheck::Uplink {
                network_config,
                dns_servers,
                ntp_servers,
                our_switch_location,
                dns_name_to_query,
                check_started_tx,
            })
            .map_err(|_err| PreflightCheckerBusy)?;

        // Don't return until the task tells us to: this allows it to create a
        // new, clear event report buffer, so if our client calls
        // `uplink_event_report` immediately after this function returns,
        // they won't see a stale report from a previous check (or no report at
        // all, if this is the first check being run).
        check_started_rx.await.unwrap();

        Ok(())
    }

    pub(crate) fn uplink_event_report(&self) -> Option<EventReport> {
        self.uplink_event_buffer
            .lock()
            .unwrap()
            .as_ref()
            .map(|event_buffer| event_buffer.generate_report())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("another preflight check is still running")]
pub(crate) struct PreflightCheckerBusy;

#[derive(Debug)]
enum PreflightCheck {
    Uplink {
        network_config: UserSpecifiedRackNetworkConfig,
        dns_servers: Vec<IpAddr>,
        ntp_servers: Vec<String>,
        our_switch_location: SwitchLocation,
        dns_name_to_query: Option<String>,
        check_started_tx: oneshot::Sender<()>,
    },
}

async fn preflight_task_main(
    request_rx: flume::Receiver<PreflightCheck>,
    uplink_event_buffer: Arc<Mutex<Option<EventBuffer>>>,
    log: Logger,
) {
    while let Ok(request) = request_rx.recv_async().await {
        match request {
            PreflightCheck::Uplink {
                network_config,
                dns_servers,
                ntp_servers,
                our_switch_location,
                dns_name_to_query,
                check_started_tx,
            } => {
                // New preflight check: create a new event buffer.
                *uplink_event_buffer.lock().unwrap() =
                    Some(EventBuffer::new(16));

                // We've cleared the shared event buffer; release our caller
                // (they can now lock and check the event buffer while we run
                // the preflight check below).
                _ = check_started_tx.send(());

                // Run the uplink check.
                uplink::run_local_uplink_preflight_check(
                    network_config,
                    dns_servers,
                    ntp_servers,
                    our_switch_location,
                    dns_name_to_query,
                    Arc::clone(&uplink_event_buffer),
                    &log,
                )
                .await;
            }
        }
    }
}
