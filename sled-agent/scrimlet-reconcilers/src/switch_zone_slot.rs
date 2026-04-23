// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DetermineSwitchSlotStatus;
use crate::ScrimletStatus;
use gateway_client::Client;
use gateway_client::ClientInfo;
use gateway_types::component::SpType;
use sled_agent_types::early_networking::SwitchSlot;
use slog::Logger;
use slog::error;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use tokio::sync::watch;
use tokio::sync::watch::error::RecvError;

/// Newtype wrapper around [`SwitchSlot`]. This type is always the physical slot
/// of our own, local switch.
///
/// This information can only be determined by asking MGS inside our own switch
/// zone. An instance of this type can only be created if we are indeed a
/// scrimlet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ThisSledSwitchSlot(SwitchSlot);

impl PartialEq<SwitchSlot> for ThisSledSwitchSlot {
    fn eq(&self, other: &SwitchSlot) -> bool {
        self.0 == *other
    }
}

impl PartialEq<ThisSledSwitchSlot> for SwitchSlot {
    fn eq(&self, other: &ThisSledSwitchSlot) -> bool {
        *self == other.0
    }
}

impl ThisSledSwitchSlot {
    const MGS_RETRY_TIMEOUT: Duration = Duration::from_secs(5);

    #[cfg(test)]
    pub(crate) const TEST_FAKE: Self = Self(SwitchSlot::Switch0);

    pub(crate) async fn determine_retrying_forever(
        determine_status_tx: watch::Sender<DetermineSwitchSlotStatus>,
        scrimlet_status_rx: &mut watch::Receiver<ScrimletStatus>,
        client: &Client,
        log: &Logger,
    ) -> Result<Self, RecvError> {
        loop {
            // Wait until we become a scrimlet; there's no point in trying to
            // contact our switch zone if it doesn't exist.
            loop {
                let scrimlet_status = *scrimlet_status_rx.borrow_and_update();
                match scrimlet_status {
                    ScrimletStatus::Scrimlet => break,
                    ScrimletStatus::NotScrimlet => {
                        determine_status_tx.send_modify(|status| {
                            *status = DetermineSwitchSlotStatus::NotScrimlet;
                        });
                        scrimlet_status_rx.changed().await?;
                        continue;
                    }
                }
            }

            // Update to our status to `ContactingMgs`, and carry forward any
            // error from a previous attempt.
            determine_status_tx.send_if_modified(|status| match status {
                DetermineSwitchSlotStatus::ContactingMgs { .. } => false,
                DetermineSwitchSlotStatus::NotScrimlet => {
                    *status = DetermineSwitchSlotStatus::ContactingMgs {
                        prev_attempt_err: None,
                    };
                    true
                }
                DetermineSwitchSlotStatus::WaitingToRetry {
                    prev_attempt_err,
                } => {
                    *status = DetermineSwitchSlotStatus::ContactingMgs {
                        prev_attempt_err: Some(prev_attempt_err.clone()),
                    };
                    true
                }
            });

            // We are a scrimlet - see if we know our own slot yet.
            let err = match client
                .sp_local_switch_id()
                .await
                .map(|resp| resp.into_inner())
            {
                Ok(identity) => match (identity.type_, identity.slot) {
                    (SpType::Switch, 0) => {
                        return Ok(ThisSledSwitchSlot(SwitchSlot::Switch0));
                    }
                    (SpType::Switch, 1) => {
                        return Ok(ThisSledSwitchSlot(SwitchSlot::Switch1));
                    }
                    (sp_type, sp_slot) => {
                        // We should never get any other response; if we do,
                        // something has gone very wrong with MGS. It's not
                        // likely retrying will fix this, but there isn't
                        // anything else we can do.
                        error!(
                            log,
                            "failed to determine this sled's switch slot: got \
                             unexpected identity; will retry";
                            "sp_type" => ?sp_type,
                            "sp_slot" => sp_slot,
                        );
                        format!(
                            "received invalid SP type/slot combo from MGS {}: \
                         {sp_type:?}/{sp_slot}",
                            client.baseurl()
                        )
                    }
                },
                Err(err) => {
                    let err = InlineErrorChain::new(&err);
                    warn!(
                        log,
                        "failed to determine this sled's switch slot; \
                         will retry";
                        &err,
                    );
                    err.to_string()
                }
            };

            determine_status_tx.send_modify(|status| {
                *status = DetermineSwitchSlotStatus::WaitingToRetry {
                    prev_attempt_err: err,
                };
            });

            // Sleep briefly before retrying.
            tokio::time::sleep(Self::MGS_RETRY_TIMEOUT).await;
        }
    }
}
