// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: module docs: powers off (A2) the sled a sled expunge request names, at the slot the caller found it in (from inventory), re-checking the slot's full BaseboardId through each MGS right before the command (RFD 747); the module does no retries of its own beyond failing over to the next MGS.

// TODO-RAINCLAUDE: every MGS call here is bounded only by the 15s connect and request timeouts that internal_http_client sets on clients built by GatewayClient::from_addr; a GatewayClient built some other way may have no timeout.

use crate::GatewayClient;
use gateway_types::component::SpType;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_types::sled_expunge::MgsPowerOffFailure;
use sled_hardware_types::BaseboardId;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::net::SocketAddrV6;

// TODO-RAINCLAUDE: error is the client error's chain, for the status and logs only, never for operator text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MgsPowerOffAttempt {
    pub mgs: SocketAddrV6,
    pub failure: MgsPowerOffFailure,
    pub error: String,
}

impl IdOrdItem for MgsPowerOffAttempt {
    type Key<'a> = SocketAddrV6;
    fn key(&self) -> Self::Key<'_> {
        self.mgs
    }
    id_upcast!();
}

// TODO-RAINCLAUDE: PoweredOff means MGS accepted the A2 command through mgs: MGS's power-off is synchronous, and an SP running message version 18 or later answers a request for the state it is already in with PowerStateUnchanged, which MGS also reports as success, so this is the one proof that the sled is off, and resending it after a crash is safe. SlotHoldsOtherBoard means mgs read a different BaseboardId at the slot (an SP with unprogrammed VPD reports an empty one, which is a mismatch too) and the mismatching MGS sent nothing; an earlier MGS's failed command may still have landed. NotPoweredOff holds every MGS's failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PowerOffAtSlotOutcome {
    PoweredOff { mgs: SocketAddrV6 },
    SlotHoldsOtherBoard { mgs: SocketAddrV6, found: BaseboardId },
    NotPoweredOff { failed_attempts: IdOrdMap<MgsPowerOffAttempt> },
}

enum MgsPowerOffResult {
    PoweredOff,
    SlotHoldsOtherBoard { found: BaseboardId },
    Failed { failure: MgsPowerOffFailure, error: String },
}

async fn power_off_through_mgs(
    log: &Logger,
    mgs: &GatewayClient,
    target: &BaseboardId,
    slot: u16,
) -> MgsPowerOffResult {
    // TODO-RAINCLAUDE: the slot comes from an inventory collection that can be stale (the sled may have moved or been replaced since), so its identity is re-checked through this MGS right before the command, comparing the full BaseboardId as nexus-mgs-updates' prechecks do. This narrows but doesn't close the window for powering off the wrong sled: a race remains between the re-check and the A2 command, and a delayed command from a paused Nexus can't be fenced by identity at all.
    let found = match mgs.client.sp_get(&SpType::Sled, slot).await {
        Ok(response) => {
            let state = response.into_inner();
            BaseboardId {
                part_number: state.model,
                serial_number: state.serial_number,
            }
        }
        Err(error) => {
            let error = InlineErrorChain::new(&error).to_string();
            warn!(
                log,
                "not powering off sled: could not re-check its slot's identity";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
                "error" => &error,
            );
            return MgsPowerOffResult::Failed {
                failure: MgsPowerOffFailure::RecheckFailed,
                error,
            };
        }
    };
    if found != *target {
        warn!(
            log,
            "not powering off sled: its slot holds another board";
            "mgs_addr" => %mgs.addr,
            "slot" => slot,
            "target" => %target,
            "found" => %found,
        );
        return MgsPowerOffResult::SlotHoldsOtherBoard { found };
    }

    match mgs
        .client
        .sp_power_state_set(
            &SpType::Sled,
            slot,
            gateway_client::types::PowerState::A2,
        )
        .await
    {
        Ok(_) => {
            info!(
                log,
                "powered off sled through MGS";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
            );
            MgsPowerOffResult::PoweredOff
        }
        Err(error) => {
            let error = InlineErrorChain::new(&error).to_string();
            warn!(
                log,
                "failed to power off sled through MGS";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
                "error" => &error,
            );
            // TODO-RAINCLAUDE: SP firmware older than message version 18 answers a request for the power state it is already in with SeqError, which MGS reports as a 503 `SpCommunicationFailed`, so such a sled already in A2 lands here on every attempt and its request stays in powering_off with a last_error.
            MgsPowerOffResult::Failed {
                failure: MgsPowerOffFailure::PowerOffFailed,
                error,
            }
        }
    }
}

// TODO-RAINCLAUDE: powers off the target at slot through one MGS at a time, in address order and each address once (so which MGS a power-off goes through is deterministic, and DNS naming one MGS twice is harmless), moving on to the next MGS only if that one failed; every MGS reaches the same SP, so one success is enough, and one MGS reading another board at the slot ends the attempt, since the caller's slot is then stale. A plain loop rather than nexus-mgs-updates' MgsClients::try_all_serially, which fails over only on communication errors and keeps only the last error.
pub async fn power_off_sled_at_slot(
    log: &Logger,
    clients: &[GatewayClient],
    target: &BaseboardId,
    slot: u16,
) -> PowerOffAtSlotOutcome {
    let mut ordered: Vec<&GatewayClient> = clients.iter().collect();
    ordered.sort_by_key(|client| client.addr);
    ordered.dedup_by_key(|client| client.addr);
    let mut failed_attempts = IdOrdMap::new();
    for mgs in ordered {
        match power_off_through_mgs(log, mgs, target, slot).await {
            MgsPowerOffResult::PoweredOff => {
                return PowerOffAtSlotOutcome::PoweredOff { mgs: mgs.addr };
            }
            MgsPowerOffResult::SlotHoldsOtherBoard { found } => {
                return PowerOffAtSlotOutcome::SlotHoldsOtherBoard {
                    mgs: mgs.addr,
                    found,
                };
            }
            MgsPowerOffResult::Failed { failure, error } => {
                failed_attempts.insert_overwrite(MgsPowerOffAttempt {
                    mgs: mgs.addr,
                    failure,
                    error,
                });
            }
        }
    }
    PowerOffAtSlotOutcome::NotPoweredOff { failed_attempts }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway_messages::SpPort;
    use gateway_test_utils::setup::GatewayTestContext;
    use gateway_types::component::PowerState;
    use omicron_test_utils::dev::tcp_proxy::ProxyTarget;
    use omicron_test_utils::dev::tcp_proxy::RetargetableTcpProxy;
    use omicron_test_utils::dev::test_setup_log;
    use sp_sim::SimulatedSp;

    async fn sim_gimlet(ctx: &GatewayTestContext, slot: usize) -> BaseboardId {
        let state = ctx.simrack.gimlets[slot].state().await;
        BaseboardId {
            part_number: state.model,
            serial_number: state.serial_number,
        }
    }

    fn sim_power_state(ctx: &GatewayTestContext, slot: usize) -> PowerState {
        let state = *ctx.simrack.gimlets[slot]
            .power_state_rx()
            .expect("sim gimlet has an SP")
            .borrow();
        PowerState::from(gateway_messages::PowerState::from(state))
    }

    #[tokio::test]
    async fn test_power_off_sled_at_slot() {
        let logctx = test_setup_log("test_power_off_sled_at_slot");
        let log = &logctx.log;
        let mgs = gateway_test_utils::setup::test_setup(
            "test_power_off_sled_at_slot",
            SpPort::One,
        )
        .await;
        let up = GatewayClient::from_addr(log, mgs.address());
        let refusing = RetargetableTcpProxy::bind(ProxyTarget::Refuse, log)
            .await
            .expect("bound a refusing TCP proxy");
        let down = GatewayClient::from_addr(log, refusing.local_addr());
        let target = sim_gimlet(&mgs, 1).await;

        assert_eq!(
            power_off_sled_at_slot(log, std::slice::from_ref(&up), &target, 0)
                .await,
            PowerOffAtSlotOutcome::SlotHoldsOtherBoard {
                mgs: up.addr,
                found: sim_gimlet(&mgs, 0).await,
            },
        );
        assert_eq!(sim_power_state(&mgs, 0), PowerState::A0, "slot 0 stays on");
        assert_eq!(sim_power_state(&mgs, 1), PowerState::A0, "slot 1 stays on");

        let clients = [down, up.clone()];
        assert_eq!(
            power_off_sled_at_slot(log, &clients, &target, 1).await,
            PowerOffAtSlotOutcome::PoweredOff { mgs: up.addr },
        );
        assert_eq!(sim_power_state(&mgs, 1), PowerState::A2);
        assert_eq!(
            sim_power_state(&mgs, 0),
            PowerState::A0,
            "the other sim gimlet stays on"
        );

        // TODO-RAINCLAUDE: resending A2 to an SP already in A2 succeeds (the SP answers PowerStateUnchanged, which MGS reports as success), which is what a request resumed after a crash relies on.
        assert_eq!(
            power_off_sled_at_slot(log, &clients, &target, 1).await,
            PowerOffAtSlotOutcome::PoweredOff { mgs: up.addr },
        );
        assert_eq!(sim_power_state(&mgs, 1), PowerState::A2);

        refusing.shutdown().await;
        mgs.teardown().await;
        logctx.cleanup_successful();
    }
}
