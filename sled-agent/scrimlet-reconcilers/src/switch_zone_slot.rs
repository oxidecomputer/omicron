// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ScrimletStatus;
use crate::ThisSledSwitchZoneUnderlayIpAddr;
use gateway_client::Client;
use gateway_types::component::SpType;
use omicron_common::address::MGS_PORT;
use sled_agent_types::early_networking::SwitchSlot;
use slog::Logger;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::SetOnce;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;
use tokio::task::JoinHandle;

/// Newtype wrapper around [`SwitchSlot`]. This type is always the physical slot
/// of our own, local switch.
///
/// This information can only be determined by asking MGS inside our own switch
/// zone. An instance of this type can only be created if we are indeed a
/// scrimlet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ThisSledSwitchSlot(SwitchSlot);

impl ThisSledSwitchSlot {
    pub(crate) fn spawn_task_to_determine(
        scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        switch_slot_tx: Arc<SetOnce<Self>>,
        parent_log: &Logger,
    ) -> JoinHandle<()> {
        let baseurl = format!("http://[{switch_zone_underlay_ip}]:{MGS_PORT}");
        let client = Client::new(
            &baseurl,
            parent_log
                .new(slog::o!("component" => "ThisSledSwitchSlotMgsClient")),
        );
        let log = parent_log
            .new(slog::o!("component" => "ThisSledSwitchSlotDetermination"));

        tokio::spawn(async move {
            match determine_switch_slot(client, scrimlet_status_rx, &log).await
            {
                Ok(slot) => {
                    info!(
                        log, "determined this sled's switch slot";
                        "slot" => ?slot,
                    );

                    // `ThisSledSwitchSlot` cannot be constructed outside this
                    // module, so it's not possible for `switch_slot_tx` to
                    // already be set (unless someone calls this method twice
                    // with the same `SetOnce<_>`, which is a programmer error).
                    switch_slot_tx.set(slot).expect(
                        "only ThisSledSwitchSlot can populate this SetOnce",
                    );
                }
                Err(_recv_error) => {
                    error!(
                        log,
                        "failed to determine this sled's switch slot: input \
                         watch channel closed (unexpected except in tests)",
                    );
                }
            }
        })
    }
}

async fn determine_switch_slot(
    client: Client,
    mut scrimlet_status_rx: watch::Receiver<ScrimletStatus>,
    log: &Logger,
) -> Result<ThisSledSwitchSlot, RecvError> {
    const RETRY_TIMEOUT: Duration = Duration::from_secs(5);

    loop {
        // Wait until we become a scrimlet; there's no point in trying to
        // contact our switch zone if it doesn't exist.
        loop {
            let scrimlet_status = *scrimlet_status_rx.borrow_and_update();
            match scrimlet_status {
                ScrimletStatus::Scrimlet => break,
                ScrimletStatus::NotScrimlet => {
                    scrimlet_status_rx.changed().await?;
                    continue;
                }
            }
        }

        // We are a scrimlet - see if we know our own slot yet.
        match client.sp_local_switch_id().await.map(|resp| resp.into_inner()) {
            Ok(identity) => match (identity.type_, identity.slot) {
                (SpType::Switch, 0) => {
                    return Ok(ThisSledSwitchSlot(SwitchSlot::Switch0));
                }
                (SpType::Switch, 1) => {
                    return Ok(ThisSledSwitchSlot(SwitchSlot::Switch1));
                }
                (sp_type, sp_slot) => {
                    // We should never get any other response; if we do,
                    // something has gone very wrong with MGS. It's not likely
                    // retrying will fix this, but there isn't anything else we
                    // can do.
                    error!(
                        log,
                        "failed to determine this sled's switch slot: got \
                         unexpected identity; will retry";
                        "sp_type" => ?sp_type,
                        "sp_slot" => sp_slot,
                    );
                }
            },
            Err(err) => {
                warn!(
                    log,
                    "failed to determine this sled's switch slot; will retry";
                    InlineErrorChain::new(&err),
                );
            }
        }

        // Sleep briefly before retrying.
        tokio::time::sleep(RETRY_TIMEOUT).await;
    }
}
