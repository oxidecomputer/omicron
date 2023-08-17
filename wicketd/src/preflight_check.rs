// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_common::api::internal::shared::SwitchLocation;
use slog::o;
use slog::Logger;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use update_engine::events::EventReport;
use update_engine::GenericSpec;

mod uplink;

pub(crate) type UplinkEventReport =
    EventReport<GenericSpec<uplink::UplinkPreflightTerminalError>>;

#[derive(Debug)]
pub(crate) struct PreflightCheckerHandler {
    request_tx: flume::Sender<PreflightCheck>,
    uplink_event_buffer: Arc<Mutex<Option<uplink::EventBuffer>>>,
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

    pub(crate) fn uplink_start(
        &self,
        network_config: RackNetworkConfig,
        dns_servers: Vec<IpAddr>,
        ntp_servers: Vec<String>,
        our_switch_location: SwitchLocation,
    ) -> Result<(), PreflightCheckerBusy> {
        self.request_tx
            .try_send(PreflightCheck::Uplink {
                network_config,
                dns_servers,
                ntp_servers,
                our_switch_location,
            })
            .map_err(|_err| PreflightCheckerBusy)
    }

    pub(crate) fn uplink_event_report(&self) -> Option<UplinkEventReport> {
        self.uplink_event_buffer
            .lock()
            .unwrap()
            .as_ref()
            .map(|event_buffer| event_buffer.generate_report().into_generic())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("another preflight check is still running")]
pub(crate) struct PreflightCheckerBusy;

#[derive(Debug)]
enum PreflightCheck {
    Uplink {
        network_config: RackNetworkConfig,
        dns_servers: Vec<IpAddr>,
        ntp_servers: Vec<String>,
        our_switch_location: SwitchLocation,
    },
}

async fn preflight_task_main(
    request_rx: flume::Receiver<PreflightCheck>,
    uplink_event_buffer: Arc<Mutex<Option<uplink::EventBuffer>>>,
    log: Logger,
) {
    while let Ok(request) = request_rx.recv_async().await {
        match request {
            PreflightCheck::Uplink {
                network_config,
                dns_servers,
                ntp_servers,
                our_switch_location,
            } => {
                // New preflight check: create a new event buffer.
                *uplink_event_buffer.lock().unwrap() =
                    Some(uplink::EventBuffer::new(16));

                // Run the uplink check.
                uplink::run_local_uplink_preflight_check(
                    network_config,
                    dns_servers,
                    ntp_servers,
                    our_switch_location,
                    Arc::clone(&uplink_event_buffer),
                    &log,
                )
                .await;
            }
        }
    }
}
