// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: module docs: settles the recorded cubby of a sled expunge request through each MGS of its rack in turn (RFD 747): re-check the cubby's full BaseboardId, leave a sled already in A2 or another board alone, cut our sled's power with an ignition power-off, and, when the cubby's SP does not answer, decide from the rack's shared ignition snapshot whether the cubby is unpowered or empty; the module does no retries of its own beyond failing over to the next MGS.

// TODO-RAINCLAUDE: every MGS call here is bounded only by the 15s connect and request timeouts that internal_http_client sets on clients built by GatewayClient::from_addr; a GatewayClient built some other way may have no timeout.

use crate::GatewayClient;
use gateway_client::types::IgnitionCommand;
use gateway_types::component::PowerState;
use gateway_types::component::SpType;
use gateway_types::ignition::SpIgnition;
use iddqd::IdOrdMap;
use nexus_types::internal_api::background::SledExpungeCubbyIgnition;
use nexus_types::internal_api::background::SledExpungeIgnitionSnapshot;
use nexus_types::internal_api::background::SledExpungeMgsIgnitionFailure;
use nexus_types::internal_api::background::SledExpungeMgsPowerOffFailure;
use nexus_types::sled_expunge::MgsPowerOffFailure;
use nexus_types::sled_expunge::is_empty_baseboard_id;
use sled_hardware_types::BaseboardId;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

// TODO-RAINCLAUDE: one rack's MGSes, in address order and each address once (so which MGS a request goes through is deterministic, and DNS naming one MGS twice is harmless), plus the rack's ignition snapshot: one GET /ignition per value, fetched lazily by the first power-off that cannot reach its cubby's SP and shared by every power-off that uses this value. The snapshot can be minutes old by the time a later request reads it, and it is consulted only after sp_get fails, never to decide that an answering SP is off. The OnceCell stores the fetch's result rather than using get_or_try_init, so a failed fetch is not retried by later callers; it is cancel-safe, so a caller timing out mid-fetch leaves the fetch to the next caller that needs it. ignition_started tells "never needed" from "cancelled before it finished".
pub struct SledPowerOffGateways {
    clients: Vec<GatewayClient>,
    ignition_started: AtomicBool,
    ignition: tokio::sync::OnceCell<IgnitionSnapshot>,
}

// TODO-RAINCLAUDE: fetch_sled_ignition's result as cached by SledPowerOffGateways.
enum IgnitionSnapshot {
    Fetched {
        mgs: SocketAddrV6,
        sleds: BTreeMap<u16, SpIgnition>,
        failed_attempts: IdOrdMap<SledExpungeMgsIgnitionFailure>,
    },
    Unavailable {
        failed_attempts: IdOrdMap<SledExpungeMgsIgnitionFailure>,
    },
}

impl SledPowerOffGateways {
    pub fn new(clients: &[GatewayClient]) -> Self {
        let mut clients = clients.to_vec();
        clients.sort_by_key(|client| client.addr);
        clients.dedup_by_key(|client| client.addr);
        Self {
            clients,
            ignition_started: AtomicBool::new(false),
            ignition: tokio::sync::OnceCell::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    // TODO-RAINCLAUDE: None when no power-off needed the ignition snapshot.
    pub fn ignition_status(&self) -> Option<SledExpungeIgnitionSnapshot> {
        match self.ignition.get() {
            Some(IgnitionSnapshot::Fetched {
                mgs,
                sleds: _,
                failed_attempts,
            }) => Some(SledExpungeIgnitionSnapshot::Fetched {
                mgs: *mgs,
                failed_attempts: failed_attempts.clone(),
            }),
            Some(IgnitionSnapshot::Unavailable { failed_attempts }) => {
                Some(SledExpungeIgnitionSnapshot::Failed {
                    failed_attempts: failed_attempts.clone(),
                })
            }
            None if self.ignition_started.load(Ordering::Relaxed) => {
                Some(SledExpungeIgnitionSnapshot::Unfinished)
            }
            None => None,
        }
    }

    async fn ignition_snapshot(&self, log: &Logger) -> &IgnitionSnapshot {
        self.ignition
            .get_or_init(|| {
                self.ignition_started.store(true, Ordering::Relaxed);
                self.fetch_ignition_snapshot(log)
            })
            .await
    }

    async fn fetch_ignition_snapshot(&self, log: &Logger) -> IgnitionSnapshot {
        match self.fetch_sled_ignition(log).await {
            Ok(SledIgnitionList { mgs, sleds, failed_attempts }) => {
                IgnitionSnapshot::Fetched {
                    mgs: mgs.addr,
                    sleds,
                    failed_attempts,
                }
            }
            Err(failed_attempts) => {
                IgnitionSnapshot::Unavailable { failed_attempts }
            }
        }
    }

    // TODO-RAINCLAUDE: one GET /ignition, through each MGS in address order until one answers, keeping only the sled cubbies; the error holds every MGS's failure. Unlike ignition_snapshot, this is not cached, so each call asks MGS again.
    pub async fn fetch_sled_ignition(
        &self,
        log: &Logger,
    ) -> Result<SledIgnitionList<'_>, IdOrdMap<SledExpungeMgsIgnitionFailure>>
    {
        let mut failed_attempts = IdOrdMap::new();
        for mgs in &self.clients {
            match mgs.client.ignition_list().await {
                Ok(response) => {
                    let mut sleds = BTreeMap::new();
                    for info in response.into_inner() {
                        match info.id.typ {
                            SpType::Sled => {
                                sleds.insert(info.id.slot, info.details);
                            }
                            SpType::Switch | SpType::Power => {}
                        }
                    }
                    info!(
                        log,
                        "fetched the rack's ignition state";
                        "mgs_addr" => %mgs.addr,
                    );
                    return Ok(SledIgnitionList {
                        mgs,
                        sleds,
                        failed_attempts,
                    });
                }
                Err(error) => {
                    let error = InlineErrorChain::new(&error).to_string();
                    warn!(
                        log,
                        "failed to fetch the rack's ignition state";
                        "mgs_addr" => %mgs.addr,
                        "error" => &error,
                    );
                    failed_attempts.insert_overwrite(
                        SledExpungeMgsIgnitionFailure { mgs: mgs.addr, error },
                    );
                }
            }
        }
        Err(failed_attempts)
    }
}

// TODO-RAINCLAUDE: sleds maps each sled cubby that mgs reported to its ignition state; failed_attempts holds the MGSes tried before it.
pub struct SledIgnitionList<'a> {
    pub mgs: &'a GatewayClient,
    pub sleds: BTreeMap<u16, SpIgnition>,
    pub failed_attempts: IdOrdMap<SledExpungeMgsIgnitionFailure>,
}

// TODO-RAINCLAUDE: what ignition says about a cubby whose SP did not answer. NotPresent: the cubby is absent or unpowered per the snapshot from mgs. Powered: it is present and powered, so the SP may still be running. Unknown: the snapshot is unavailable or has no entry for the cubby.
#[derive(Debug, PartialEq, Eq)]
enum IgnitionVerdict {
    NotPresent { mgs: SocketAddrV6, ignition: SledExpungeCubbyIgnition },
    Powered,
    Unknown { reason: String },
}

fn ignition_verdict(snapshot: &IgnitionSnapshot, slot: u16) -> IgnitionVerdict {
    let (mgs, sleds) = match snapshot {
        IgnitionSnapshot::Fetched { mgs, sleds, failed_attempts: _ } => {
            (*mgs, sleds)
        }
        IgnitionSnapshot::Unavailable { failed_attempts } => {
            return IgnitionVerdict::Unknown {
                reason: format!(
                    "no MGS answered the rack's ignition request ({} tried)",
                    failed_attempts.len(),
                ),
            };
        }
    };
    match sleds.get(&slot) {
        Some(SpIgnition::Absent) => IgnitionVerdict::NotPresent {
            mgs,
            ignition: SledExpungeCubbyIgnition::Absent,
        },
        Some(SpIgnition::Present { power: false, .. }) => {
            IgnitionVerdict::NotPresent {
                mgs,
                ignition: SledExpungeCubbyIgnition::Unpowered,
            }
        }
        Some(SpIgnition::Present { power: true, .. }) => {
            IgnitionVerdict::Powered
        }
        None => IgnitionVerdict::Unknown {
            reason: format!(
                "the ignition snapshot from {mgs} has no entry for sled \
                 cubby {slot}"
            ),
        },
    }
}

// TODO-RAINCLAUDE: how the recorded cubby was settled, through mgs. PoweredOff: mgs accepted an ignition power-off after reading our board there and not in A2; a successful ignition power-off means the cubby's power is already cut. AlreadyOff: mgs read our board in A2, so nothing was sent. OtherBoardPresent: mgs read `found` there, a different non-empty BaseboardId, so nothing was sent through mgs, though an earlier MGS's failed power-off may still have landed. NotPresent: no MGS reached the SP and the ignition snapshot from mgs shows the cubby absent or unpowered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CubbyPowerOffOutcome {
    PoweredOff { mgs: SocketAddrV6 },
    AlreadyOff { mgs: SocketAddrV6 },
    OtherBoardPresent { mgs: SocketAddrV6, found: BaseboardId },
    NotPresent { mgs: SocketAddrV6, ignition: SledExpungeCubbyIgnition },
}

// TODO-RAINCLAUDE: NotPoweredOff holds every MGS's failure; nothing is settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PowerOffAtSlotOutcome {
    Settled(CubbyPowerOffOutcome),
    NotPoweredOff { failed_attempts: IdOrdMap<SledExpungeMgsPowerOffFailure> },
}

// TODO-RAINCLAUDE: one MGS's attempt. Settled ends the loop. Unreachable is tentative: this MGS could not reach the SP and the snapshot shows the cubby absent or unpowered; conclude_unsettled decides.
#[derive(Debug)]
enum MgsPowerOffResult {
    Settled(CubbyPowerOffOutcome),
    Unsettled(UnsettledAttempt),
}

#[derive(Debug)]
enum UnsettledAttempt {
    Unreachable {
        ignition_mgs: SocketAddrV6,
        ignition: SledExpungeCubbyIgnition,
        error: String,
    },
    Failed {
        failure: MgsPowerOffFailure,
        error: String,
    },
}

async fn power_off_through_mgs(
    log: &Logger,
    gateways: &SledPowerOffGateways,
    mgs: &GatewayClient,
    target: &BaseboardId,
    slot: u16,
) -> MgsPowerOffResult {
    // TODO-RAINCLAUDE: the cubby was recorded when the request was created, and its sled may have moved or been replaced since, so its identity is re-checked through this MGS right before the power-off, comparing the full BaseboardId as nexus-mgs-updates' prechecks do. A race remains between the re-check and the power-off, and a delayed power-off from a paused Nexus can't be fenced by identity at all; both are accepted, since moving a sled cuts its power and the removal has already committed.
    let state = match mgs.client.sp_get(&SpType::Sled, slot).await {
        Ok(response) => response.into_inner(),
        Err(error) => {
            let error = InlineErrorChain::new(&error).to_string();
            return unreachable_sp_result(
                log, gateways, mgs, target, slot, error,
            )
            .await;
        }
    };
    let found = BaseboardId {
        part_number: state.model,
        serial_number: state.serial_number,
    };
    // TODO-RAINCLAUDE: an empty BaseboardId neither settles the cubby nor gets powered off; a later attempt retries.
    if is_empty_baseboard_id(&found) {
        warn!(
            log,
            "not powering off sled: its cubby's SP reported an empty \
             baseboard identity";
            "mgs_addr" => %mgs.addr,
            "slot" => slot,
            "target" => %target,
            "found" => %found,
        );
        return MgsPowerOffResult::Unsettled(UnsettledAttempt::Failed {
            failure: MgsPowerOffFailure::IdentityUnknown,
            error: format!(
                "the SP in cubby {slot} reported baseboard {found:?}"
            ),
        });
    }
    if found != *target {
        warn!(
            log,
            "not powering off sled: its cubby holds another board";
            "mgs_addr" => %mgs.addr,
            "slot" => slot,
            "target" => %target,
            "found" => %found,
        );
        return MgsPowerOffResult::Settled(
            CubbyPowerOffOutcome::OtherBoardPresent { mgs: mgs.addr, found },
        );
    }
    match state.power_state {
        PowerState::A2 => {
            info!(
                log,
                "not powering off sled: it is already in A2";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
            );
            return MgsPowerOffResult::Settled(
                CubbyPowerOffOutcome::AlreadyOff { mgs: mgs.addr },
            );
        }
        PowerState::A0 | PowerState::A1 => {}
    }

    match mgs
        .client
        .ignition_command(&SpType::Sled, slot, IgnitionCommand::PowerOff)
        .await
    {
        Ok(_) => {
            info!(
                log,
                "powered off sled through ignition";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
            );
            MgsPowerOffResult::Settled(CubbyPowerOffOutcome::PoweredOff {
                mgs: mgs.addr,
            })
        }
        Err(error) => {
            let error = InlineErrorChain::new(&error).to_string();
            warn!(
                log,
                "failed to power off sled through ignition";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
                "error" => &error,
            );
            MgsPowerOffResult::Unsettled(UnsettledAttempt::Failed {
                failure: MgsPowerOffFailure::IgnitionPowerOffFailed,
                error,
            })
        }
    }
}

// TODO-RAINCLAUDE: an SP that does not answer may be unpowered (including by Nexus itself, in an attempt that crashed before recording it), gone, or just unreachable through this MGS; ignition tells the first two from the third, and the caller settles only once no MGS reached the SP.
async fn unreachable_sp_result(
    log: &Logger,
    gateways: &SledPowerOffGateways,
    mgs: &GatewayClient,
    target: &BaseboardId,
    slot: u16,
    sp_error: String,
) -> MgsPowerOffResult {
    let snapshot = gateways.ignition_snapshot(log).await;
    match ignition_verdict(snapshot, slot) {
        IgnitionVerdict::NotPresent { mgs: ignition_mgs, ignition } => {
            info!(
                log,
                "sled's SP did not answer and ignition shows its cubby \
                 absent or unpowered";
                "mgs_addr" => %mgs.addr,
                "ignition_mgs_addr" => %ignition_mgs,
                "slot" => slot,
                "target" => %target,
                "ignition" => ?ignition,
                "error" => &sp_error,
            );
            MgsPowerOffResult::Unsettled(UnsettledAttempt::Unreachable {
                ignition_mgs,
                ignition,
                error: sp_error,
            })
        }
        IgnitionVerdict::Powered => {
            warn!(
                log,
                "not powering off sled: its SP did not answer but ignition \
                 shows its cubby powered";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
                "error" => &sp_error,
            );
            MgsPowerOffResult::Unsettled(UnsettledAttempt::Failed {
                failure: MgsPowerOffFailure::SpUnreachableCubbyPowered,
                error: sp_error,
            })
        }
        IgnitionVerdict::Unknown { reason } => {
            warn!(
                log,
                "not powering off sled: its SP did not answer and its \
                 cubby's ignition state is unknown";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
                "error" => &sp_error,
                "ignition" => &reason,
            );
            MgsPowerOffResult::Unsettled(UnsettledAttempt::Failed {
                failure: MgsPowerOffFailure::IgnitionStateUnknown,
                error: format!("{sp_error}; {reason}"),
            })
        }
    }
}

// TODO-RAINCLAUDE: settles the target's recorded cubby through one MGS at a time, in address order, moving on to the next MGS unless that one settled it; every MGS reaches the same SP, so one settled outcome is enough, and one MGS reading another board in the cubby ends the attempt. A plain loop rather than nexus-mgs-updates' MgsClients::try_all_serially, which fails over only on communication errors and keeps only the last error.
pub async fn power_off_sled_at_slot(
    log: &Logger,
    gateways: &SledPowerOffGateways,
    target: &BaseboardId,
    slot: u16,
) -> PowerOffAtSlotOutcome {
    let mut attempts = Vec::new();
    for mgs in &gateways.clients {
        match power_off_through_mgs(log, gateways, mgs, target, slot).await {
            MgsPowerOffResult::Settled(outcome) => {
                return PowerOffAtSlotOutcome::Settled(outcome);
            }
            MgsPowerOffResult::Unsettled(attempt) => {
                attempts.push((mgs.addr, attempt));
            }
        }
    }
    conclude_unsettled(attempts)
}

// TODO-RAINCLAUDE: the outcome once no MGS settled the cubby: NotPresent only if there was at least one MGS and every one of them failed to reach the SP while the snapshot showed the cubby absent or unpowered, since each switch has its own ignition and management links; every attempt sees the same shared snapshot, so the absent verdict still rests on that one GET /ignition (accepted under the one-snapshot-per-activation rule). Otherwise nothing is settled, and the tentative attempts are recorded as failures.
fn conclude_unsettled(
    attempts: Vec<(SocketAddrV6, UnsettledAttempt)>,
) -> PowerOffAtSlotOutcome {
    let mut not_present = None;
    let mut all_unreachable = !attempts.is_empty();
    for (_, attempt) in &attempts {
        match attempt {
            UnsettledAttempt::Unreachable {
                ignition_mgs,
                ignition,
                error: _,
            } => {
                not_present = Some(CubbyPowerOffOutcome::NotPresent {
                    mgs: *ignition_mgs,
                    ignition: *ignition,
                });
            }
            UnsettledAttempt::Failed { .. } => {
                all_unreachable = false;
            }
        }
    }
    if all_unreachable && let Some(outcome) = not_present {
        return PowerOffAtSlotOutcome::Settled(outcome);
    }
    let failed_attempts = attempts
        .into_iter()
        .map(|(mgs, attempt)| match attempt {
            UnsettledAttempt::Unreachable { error, .. } => {
                SledExpungeMgsPowerOffFailure {
                    mgs,
                    failure: MgsPowerOffFailure::SpUnreachableCubbyUnpowered,
                    error,
                }
            }
            UnsettledAttempt::Failed { failure, error } => {
                SledExpungeMgsPowerOffFailure { mgs, failure, error }
            }
        })
        .collect();
    PowerOffAtSlotOutcome::NotPoweredOff { failed_attempts }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway_messages::SpPort;
    use gateway_test_utils::setup::GatewayTestContext;
    use gateway_types::ignition::SpIgnitionSystemType;
    use omicron_test_utils::dev::tcp_proxy::ProxyTarget;
    use omicron_test_utils::dev::tcp_proxy::RetargetableTcpProxy;
    use omicron_test_utils::dev::test_setup_log;
    use sp_sim::Responsiveness;
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

    async fn ignition_power(mgs: &GatewayClient, slot: u16) -> bool {
        let ignition = mgs
            .client
            .ignition_list()
            .await
            .expect("listed ignition state")
            .into_inner();
        let info = ignition
            .iter()
            .find(|info| info.id.typ == SpType::Sled && info.id.slot == slot)
            .expect("ignition reports the sled cubby");
        match info.details {
            SpIgnition::Present { power, .. } => power,
            SpIgnition::Absent => {
                panic!("ignition reports cubby {slot} absent")
            }
        }
    }

    fn failures<I>(entries: I) -> IdOrdMap<SledExpungeMgsPowerOffFailure>
    where
        I: IntoIterator<Item = (SocketAddrV6, MgsPowerOffFailure)>,
    {
        entries
            .into_iter()
            .map(|(mgs, failure)| SledExpungeMgsPowerOffFailure {
                mgs,
                failure,
                error: String::new(),
            })
            .collect()
    }

    // TODO-RAINCLAUDE: error texts carry MGS and transport details, so the tests compare failures by MGS and failure kind only.
    fn without_errors(outcome: PowerOffAtSlotOutcome) -> PowerOffAtSlotOutcome {
        match outcome {
            PowerOffAtSlotOutcome::Settled(_) => outcome,
            PowerOffAtSlotOutcome::NotPoweredOff { failed_attempts } => {
                PowerOffAtSlotOutcome::NotPoweredOff {
                    failed_attempts: failed_attempts
                        .into_iter()
                        .map(|failure| SledExpungeMgsPowerOffFailure {
                            error: String::new(),
                            ..failure
                        })
                        .collect(),
                }
            }
        }
    }

    // TODO-RAINCLAUDE: one row of the decision table per step, against one sim rack (gimlets in cubbies 0 and 1). down refuses connections and sorts after up, except where the test wants it first, so each row also checks which MGS settled it.
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
        let sled0 = sim_gimlet(&mgs, 0).await;
        let sled1 = sim_gimlet(&mgs, 1).await;

        let gateways = SledPowerOffGateways::new(std::slice::from_ref(&up));
        assert_eq!(
            power_off_sled_at_slot(log, &gateways, &sled1, 0).await,
            PowerOffAtSlotOutcome::Settled(
                CubbyPowerOffOutcome::OtherBoardPresent {
                    mgs: up.addr,
                    found: sled0.clone(),
                }
            ),
        );
        assert_eq!(sim_power_state(&mgs, 0), PowerState::A0);
        assert!(ignition_power(&up, 0).await, "cubby 0 stays powered");

        up.client
            .sp_power_state_set(
                &SpType::Sled,
                0,
                gateway_client::types::PowerState::A2,
            )
            .await
            .expect("put sim gimlet 0 in A2");
        assert_eq!(
            power_off_sled_at_slot(log, &gateways, &sled0, 0).await,
            PowerOffAtSlotOutcome::Settled(CubbyPowerOffOutcome::AlreadyOff {
                mgs: up.addr,
            }),
        );
        assert!(ignition_power(&up, 0).await, "cubby 0 stays powered");

        // TODO-RAINCLAUDE: an SP that does not answer while ignition shows its cubby powered is a failure through every MGS; the refusing MGS cannot answer the ignition request either, so the shared snapshot comes from up, after one failed request through down if down sorts first.
        mgs.simrack.gimlets[1]
            .set_responsiveness(Responsiveness::Unresponsive)
            .await;
        let both = SledPowerOffGateways::new(&[up.clone(), down.clone()]);
        assert_eq!(
            without_errors(power_off_sled_at_slot(log, &both, &sled1, 1).await),
            PowerOffAtSlotOutcome::NotPoweredOff {
                failed_attempts: failures([
                    (down.addr, MgsPowerOffFailure::SpUnreachableCubbyPowered),
                    (up.addr, MgsPowerOffFailure::SpUnreachableCubbyPowered),
                ]),
            },
        );
        let down_first = down.addr < up.addr;
        match both.ignition_status() {
            Some(SledExpungeIgnitionSnapshot::Fetched {
                mgs: snapshot_mgs,
                failed_attempts,
            }) => {
                assert_eq!(snapshot_mgs, up.addr);
                assert_eq!(
                    failed_attempts
                        .iter()
                        .map(|failure| failure.mgs)
                        .collect::<Vec<_>>(),
                    if down_first { vec![down.addr] } else { vec![] },
                );
            }
            other => panic!("expected a fetched snapshot, found {other:?}"),
        }
        mgs.simrack.gimlets[1]
            .set_responsiveness(Responsiveness::Responsive)
            .await;

        // TODO-RAINCLAUDE: our board not in A2 is powered off through ignition, without needing the snapshot, and its SP then stops answering.
        let fresh = SledPowerOffGateways::new(std::slice::from_ref(&up));
        assert_eq!(
            power_off_sled_at_slot(log, &fresh, &sled1, 1).await,
            PowerOffAtSlotOutcome::Settled(CubbyPowerOffOutcome::PoweredOff {
                mgs: up.addr,
            }),
        );
        assert!(!ignition_power(&up, 1).await, "cubby 1 is unpowered");
        assert!(ignition_power(&up, 0).await, "cubby 0 stays powered");
        assert_eq!(fresh.ignition_status(), None, "no snapshot was needed");

        let retry = SledPowerOffGateways::new(&[up.clone(), down.clone()]);
        assert_eq!(
            power_off_sled_at_slot(log, &retry, &sled1, 1).await,
            PowerOffAtSlotOutcome::Settled(CubbyPowerOffOutcome::NotPresent {
                mgs: up.addr,
                ignition: SledExpungeCubbyIgnition::Unpowered,
            }),
        );

        let unreachable =
            SledPowerOffGateways::new(std::slice::from_ref(&down));
        assert_eq!(
            without_errors(
                power_off_sled_at_slot(log, &unreachable, &sled1, 1).await
            ),
            PowerOffAtSlotOutcome::NotPoweredOff {
                failed_attempts: failures([(
                    down.addr,
                    MgsPowerOffFailure::IgnitionStateUnknown
                )]),
            },
        );

        refusing.shutdown().await;
        mgs.teardown().await;
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: the sim rack has no MGS-configured cubby without a gimlet, so the Absent row and a snapshot without the cubby are checked against a constructed snapshot.
    #[test]
    fn test_ignition_verdict() {
        let mgs: SocketAddrV6 =
            "[::1]:12225".parse().expect("parsed an MGS address");
        let present = |power| SpIgnition::Present {
            id: SpIgnitionSystemType::Gimlet,
            power,
            ctrl_detect_0: true,
            ctrl_detect_1: true,
            flt_a3: false,
            flt_a2: false,
            flt_rot: false,
            flt_sp: false,
        };
        let snapshot = IgnitionSnapshot::Fetched {
            mgs,
            sleds: BTreeMap::from([
                (0, SpIgnition::Absent),
                (1, present(false)),
                (2, present(true)),
            ]),
            failed_attempts: IdOrdMap::new(),
        };
        assert_eq!(
            ignition_verdict(&snapshot, 0),
            IgnitionVerdict::NotPresent {
                mgs,
                ignition: SledExpungeCubbyIgnition::Absent,
            },
        );
        assert_eq!(
            ignition_verdict(&snapshot, 1),
            IgnitionVerdict::NotPresent {
                mgs,
                ignition: SledExpungeCubbyIgnition::Unpowered,
            },
        );
        assert_eq!(ignition_verdict(&snapshot, 2), IgnitionVerdict::Powered);
        let unavailable =
            IgnitionSnapshot::Unavailable { failed_attempts: IdOrdMap::new() };
        for verdict in
            [ignition_verdict(&snapshot, 3), ignition_verdict(&unavailable, 0)]
        {
            match verdict {
                IgnitionVerdict::Unknown { reason: _ } => {}
                IgnitionVerdict::NotPresent { .. }
                | IgnitionVerdict::Powered => {
                    panic!("expected an unknown verdict, got {verdict:?}")
                }
            }
        }
    }

    // TODO-RAINCLAUDE: the sim's ignition state is shared by both switches, so it cannot show one switch's ignition reporting the cubby unpowered while the other switch reaches the SP; the rule is checked on conclude_unsettled directly.
    #[test]
    fn test_conclude_unsettled() {
        let mgs0: SocketAddrV6 = "[::1]:1".parse().expect("parsed an address");
        let mgs1: SocketAddrV6 = "[::1]:2".parse().expect("parsed an address");
        let unreachable = || UnsettledAttempt::Unreachable {
            ignition_mgs: mgs0,
            ignition: SledExpungeCubbyIgnition::Unpowered,
            error: String::new(),
        };
        let failed = |failure| UnsettledAttempt::Failed {
            failure,
            error: String::new(),
        };

        assert_eq!(
            conclude_unsettled(vec![
                (mgs0, unreachable()),
                (mgs1, unreachable())
            ]),
            PowerOffAtSlotOutcome::Settled(CubbyPowerOffOutcome::NotPresent {
                mgs: mgs0,
                ignition: SledExpungeCubbyIgnition::Unpowered,
            }),
        );
        assert_eq!(
            conclude_unsettled(vec![
                (mgs0, unreachable()),
                (mgs1, failed(MgsPowerOffFailure::IgnitionPowerOffFailed)),
            ]),
            PowerOffAtSlotOutcome::NotPoweredOff {
                failed_attempts: failures([
                    (mgs0, MgsPowerOffFailure::SpUnreachableCubbyUnpowered),
                    (mgs1, MgsPowerOffFailure::IgnitionPowerOffFailed),
                ]),
            },
            "an MGS that reached the SP keeps the cubby unsettled",
        );
        assert_eq!(
            conclude_unsettled(vec![
                (mgs0, failed(MgsPowerOffFailure::IdentityUnknown)),
                (mgs1, unreachable()),
            ]),
            PowerOffAtSlotOutcome::NotPoweredOff {
                failed_attempts: failures([
                    (mgs0, MgsPowerOffFailure::IdentityUnknown),
                    (mgs1, MgsPowerOffFailure::SpUnreachableCubbyUnpowered),
                ]),
            },
        );
        assert_eq!(
            conclude_unsettled(Vec::new()),
            PowerOffAtSlotOutcome::NotPoweredOff {
                failed_attempts: IdOrdMap::new(),
            },
        );
    }
}
