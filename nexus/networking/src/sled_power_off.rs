// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: Locates and powers off the sled of a sled expunge request
// through the MGSes of its rack (RFD 747). There are two entry points:
// * `power_off_sled_at_slot` powers off the sled in one cubby.
// * `SledPowerOffGateways::live_sled_presence` is the rack-wide check. It finds
//   the cubby the sled is in, or concludes that the sled is in no powered sled
//   cubby.
//
// Nothing here retries a request, other than by failing over to the next MGS.

// TODO-RAINCLAUDE: This module sets no timeouts of its own. Every MGS call
// relies on the 15-second connect and request timeouts that
// `internal_http_client` sets on the clients `GatewayClient::from_addr` builds,
// so a `GatewayClient` built some other way may have no timeout at all.

use crate::GatewayClient;
use crate::LazyOnce;
use crate::LazyOnceState;
use crate::local_switch_slot;
use gateway_client::types::IgnitionCommand;
use gateway_client::types::SpIgnitionInfo;
use gateway_types::component::PowerState;
use gateway_types::component::SpType;
use iddqd::IdOrdMap;
use nexus_types::internal_api::background::SledExpungeIgnitionGap;
use nexus_types::internal_api::background::SledExpungeIgnitionSnapshot;
use nexus_types::internal_api::background::SledExpungeIgnitionUnavailable;
use nexus_types::internal_api::background::SledExpungeMgsFailure;
use nexus_types::internal_api::background::SledExpungeMgsPowerOffFailure;
use nexus_types::internal_api::background::SledExpungeOtherBoard;
use nexus_types::internal_api::background::SledExpungeSwitchIdentification;
use nexus_types::internal_api::background::SledExpungeSwitchMgs;
use nexus_types::internal_api::background::SledPoweredDown;
use nexus_types::sled_expunge::CubbyUnidentified;
use nexus_types::sled_expunge::MgsPowerOffFailure;
use nexus_types::sled_expunge::NonEmptyBaseboardId;
use nexus_types::sled_expunge::SledCubbyBoard;
use nexus_types::sled_expunge::SledCubbyIdentity;
use nexus_types::sled_expunge::SledCubbyIdentityEntry;
use nexus_types::sled_expunge::SledCubbyIgnition;
use nexus_types::sled_expunge::SledExpungeLivePresence;
use nexus_types::sled_expunge::sled_expunge_cubbies_to_identify;
use nexus_types::sled_expunge::sled_expunge_live_presence;
use sled_agent_types::early_networking::SwitchSlot;
use sled_hardware_types::BaseboardId;
use slog::Logger;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;

type MgsClientError = gateway_client::Error<gateway_client::types::Error>;

// TODO-RAINCLAUDE: One rack's MGSes, along with what has been learned about the
// rack through them. The MGSes are sorted by address and deduplicated, so that
// requests go through them in the same order every time and each attempt can be
// keyed by its MGS.
//
// Which MGS serves each switch, each switch's ignition list, and the rack-wide
// cubby scan are each computed at most once and shared by every caller. A later
// caller may see results that are minutes old.
pub struct SledPowerOffGateways {
    clients: Vec<GatewayClient>,
    identified: IdOrdMap<SledCubbyBoard>,
    switches: LazyOnce<SwitchAssignment>,
    cubby_scan: LazyOnce<CubbyScan>,
}

// TODO-RAINCLAUDE: Holds one value for each of the rack's two switches. This is
// a struct rather than a map so that neither switch can be left out.
#[derive(Debug)]
struct PerSwitch<T> {
    switch0: T,
    switch1: T,
}

impl<T> PerSwitch<T> {
    fn get(&self, switch: SwitchSlot) -> &T {
        match switch {
            SwitchSlot::Switch0 => &self.switch0,
            SwitchSlot::Switch1 => &self.switch1,
        }
    }

    fn map<U, F>(self, mut f: F) -> PerSwitch<U>
    where
        F: FnMut(SwitchSlot, T) -> U,
    {
        PerSwitch {
            switch0: f(SwitchSlot::Switch0, self.switch0),
            switch1: f(SwitchSlot::Switch1, self.switch1),
        }
    }
}

// TODO-RAINCLAUDE: Which MGS serves each switch, as each MGS reported it.
// `switch_unknown` holds the MGSes whose switch we could not learn, and nothing
// those MGSes report counts as evidence for either switch.
struct SwitchAssignment {
    switches: PerSwitch<SwitchMgs>,
    switch_unknown: IdOrdMap<SledExpungeMgsFailure>,
}

// TODO-RAINCLAUDE: The MGS that serves one switch, if exactly one does.
// `Conflict` means at least two MGSes said they serve this switch, which a
// correctly cabled rack can't produce, so none of them counts as the switch's
// evidence.
enum SwitchMgs {
    Found {
        mgs: GatewayClient,
        ignition: LazyOnce<SwitchIgnition>,
    },
    NotFound,
    Conflict {
        first: SocketAddrV6,
        second: SocketAddrV6,
        rest: BTreeSet<SocketAddrV6>,
    },
}

enum SwitchIgnition {
    Fetched { sleds: IdOrdMap<SledCubbyIgnition> },
    Failed { error: String },
}

enum CubbyScan {
    Scanned {
        switch0_mgs: SocketAddrV6,
        switch1_mgs: SocketAddrV6,
        identities: IdOrdMap<SledCubbyIdentityEntry>,
    },
    IgnitionUnavailable {
        switches: SledExpungeIgnitionUnavailable,
    },
}

impl SledPowerOffGateways {
    // TODO-RAINCLAUDE: `identified` holds the boards the caller already knows
    // to be in the rack's sled cubbies. The cubby scan takes those boards as
    // given rather than calling `sp_get`, but only for cubbies it would have
    // identified anyway, so `identified` never adds a cubby to the scan or
    // removes one.
    pub fn new(
        clients: &[GatewayClient],
        identified: IdOrdMap<SledCubbyBoard>,
    ) -> Self {
        let mut clients = clients.to_vec();
        clients.sort_by_key(|client| client.addr);
        clients.dedup_by_key(|client| client.addr);
        Self {
            clients,
            identified,
            switches: LazyOnce::new(),
            cubby_scan: LazyOnce::new(),
        }
    }

    // TODO-RAINCLAUDE: Returns which MGS serves each switch, for the
    // `sled_expunge` task's status. This is `None` when nothing has asked which
    // switch each MGS serves, so that the rack is left out of the status.
    pub fn switch_status(&self) -> Option<SledExpungeSwitchIdentification> {
        let assignment = match self.switches.state() {
            LazyOnceState::NotStarted => return None,
            LazyOnceState::Unfinished => {
                return Some(SledExpungeSwitchIdentification::Unfinished);
            }
            LazyOnceState::Done(assignment) => assignment,
        };
        Some(SledExpungeSwitchIdentification::Identified {
            switch_unknown: assignment.switch_unknown.clone(),
            switch0: switch_mgs_status(&assignment.switches.switch0),
            switch1: switch_mgs_status(&assignment.switches.switch1),
        })
    }

    async fn switch_assignment(&self, log: &Logger) -> &SwitchAssignment {
        self.switches
            .get_or_init(|| identify_switches(log, &self.clients))
            .await
    }

    async fn switch_ignition(
        &self,
        log: &Logger,
        switch: SwitchSlot,
    ) -> Result<
        (SocketAddrV6, &IdOrdMap<SledCubbyIgnition>),
        SledExpungeIgnitionGap,
    > {
        match self.switch_assignment(log).await.switches.get(switch) {
            SwitchMgs::Found { mgs, ignition } => {
                match ignition
                    .get_or_init(|| fetch_sled_ignition(log, switch, mgs))
                    .await
                {
                    SwitchIgnition::Fetched { sleds } => Ok((mgs.addr, sleds)),
                    SwitchIgnition::Failed { error: _ } => {
                        Err(SledExpungeIgnitionGap::FetchFailed {
                            mgs: mgs.addr,
                        })
                    }
                }
            }
            SwitchMgs::NotFound => Err(SledExpungeIgnitionGap::NoMgs),
            SwitchMgs::Conflict { .. } => {
                Err(SledExpungeIgnitionGap::MgsConflict)
            }
        }
    }

    // TODO-RAINCLAUDE: Runs the rack-wide check for one sled. This returns an
    // error when either switch's ignition list is unavailable, because ruling a
    // sled out of the rack needs both lists.
    pub async fn live_sled_presence(
        &self,
        log: &Logger,
        target: &BaseboardId,
    ) -> Result<SledExpungeLivePresence, SledExpungeIgnitionUnavailable> {
        match self.cubby_scan(log).await {
            CubbyScan::Scanned { switch0_mgs, switch1_mgs, identities } => {
                Ok(sled_expunge_live_presence(
                    target,
                    *switch0_mgs,
                    *switch1_mgs,
                    identities,
                ))
            }
            CubbyScan::IgnitionUnavailable { switches } => Err(*switches),
        }
    }

    async fn cubby_scan(&self, log: &Logger) -> &CubbyScan {
        self.cubby_scan.get_or_init(|| self.scan_cubbies(log)).await
    }

    async fn scan_cubbies(&self, log: &Logger) -> CubbyScan {
        let (switch0, switch1) = futures::join!(
            self.switch_ignition(log, SwitchSlot::Switch0),
            self.switch_ignition(log, SwitchSlot::Switch1),
        );
        let ((switch0_mgs, switch0), (switch1_mgs, switch1)) =
            match SledExpungeIgnitionUnavailable::from_switches(
                switch0, switch1,
            ) {
                Ok(both) => both,
                Err(switches) => {
                    return CubbyScan::IgnitionUnavailable { switches };
                }
            };
        let to_identify = sled_expunge_cubbies_to_identify(switch0, switch1);
        let (known, unknown): (BTreeSet<u16>, BTreeSet<u16>) = to_identify
            .sp_get
            .into_iter()
            .partition(|slot| self.identified.contains_key(slot));
        let mut identities =
            identify_cubbies(log, &self.clients, &unknown).await;
        let known = known.into_iter().map(|slot| {
            let board = self
                .identified
                .get(&slot)
                .expect("known was partitioned on being in identified");
            SledCubbyIdentityEntry {
                slot,
                identity: SledCubbyIdentity::Known(board.baseboard_id.clone()),
            }
        });
        let unidentifiable =
            to_identify.unidentifiable.into_iter().map(|cubby| {
                SledCubbyIdentityEntry {
                    slot: cubby.slot,
                    identity: SledCubbyIdentity::Unidentified(cubby.reason),
                }
            });
        for entry in known.chain(unidentifiable) {
            identities.insert_unique(entry).expect(
                "known, unknown and unidentifiable cubbies are disjoint",
            );
        }
        CubbyScan::Scanned { switch0_mgs, switch1_mgs, identities }
    }
}

fn switch_mgs_status(mgs: &SwitchMgs) -> SledExpungeSwitchMgs {
    match mgs {
        SwitchMgs::Found { mgs, ignition } => SledExpungeSwitchMgs::Found {
            mgs: mgs.addr,
            ignition: match ignition.state() {
                LazyOnceState::NotStarted => {
                    SledExpungeIgnitionSnapshot::NotNeeded
                }
                LazyOnceState::Unfinished => {
                    SledExpungeIgnitionSnapshot::Unfinished
                }
                LazyOnceState::Done(SwitchIgnition::Fetched { sleds: _ }) => {
                    SledExpungeIgnitionSnapshot::Fetched
                }
                LazyOnceState::Done(SwitchIgnition::Failed { error }) => {
                    SledExpungeIgnitionSnapshot::Failed { error: error.clone() }
                }
            },
        },
        SwitchMgs::NotFound => SledExpungeSwitchMgs::NotFound,
        SwitchMgs::Conflict { first, second, rest } => {
            SledExpungeSwitchMgs::Conflict {
                first: *first,
                second: *second,
                rest: rest.clone(),
            }
        }
    }
}

async fn identify_switches(
    log: &Logger,
    clients: &[GatewayClient],
) -> SwitchAssignment {
    let answers =
        futures::future::join_all(clients.iter().map(|mgs| async move {
            (mgs, mgs.client.sp_local_switch_id().await)
        }))
        .await;
    let mut claims: PerSwitch<Vec<&GatewayClient>> =
        PerSwitch { switch0: Vec::new(), switch1: Vec::new() };
    let mut switch_unknown = IdOrdMap::new();
    for (mgs, answer) in answers {
        let error = match answer {
            Ok(response) => match local_switch_slot(&response.into_inner()) {
                Ok(SwitchSlot::Switch0) => {
                    claims.switch0.push(mgs);
                    continue;
                }
                Ok(SwitchSlot::Switch1) => {
                    claims.switch1.push(mgs);
                    continue;
                }
                Err(error) => error.to_string(),
            },
            Err(error) => InlineErrorChain::new(&error).to_string(),
        };
        warn!(
            log,
            "could not learn which switch an MGS serves; nothing it reports \
             counts as a switch's evidence";
            "mgs_addr" => %mgs.addr,
            "error" => &error,
        );
        switch_unknown
            .insert_unique(SledExpungeMgsFailure { mgs: mgs.addr, error })
            .expect("SledPowerOffGateways::new drops duplicate MGS addresses");
    }
    let switches = claims.map(|switch, claimants| match claimants.as_slice() {
        [] => {
            warn!(
                log,
                "no discovered MGS serves this switch";
                "switch" => ?switch,
            );
            SwitchMgs::NotFound
        }
        [mgs] => {
            info!(
                log,
                "found the MGS that serves this switch";
                "switch" => ?switch,
                "mgs_addr" => %mgs.addr,
            );
            SwitchMgs::Found { mgs: (*mgs).clone(), ignition: LazyOnce::new() }
        }
        [first, second, rest @ ..] => {
            let rest: BTreeSet<_> = rest.iter().map(|mgs| mgs.addr).collect();
            error!(
                log,
                "more than one MGS says it serves this switch; none of them \
                 counts as the switch's evidence";
                "switch" => ?switch,
                "first_mgs_addr" => %first.addr,
                "second_mgs_addr" => %second.addr,
                "other_mgs_addrs" => ?rest,
            );
            SwitchMgs::Conflict { first: first.addr, second: second.addr, rest }
        }
    });
    SwitchAssignment { switches, switch_unknown }
}

// TODO-RAINCLAUDE: Fetches one switch's ignition list through that switch's own
// MGS. A failure isn't retried through the other switch's MGS, because each MGS
// reads only its own switch's ignition controller.
async fn fetch_sled_ignition(
    log: &Logger,
    switch: SwitchSlot,
    mgs: &GatewayClient,
) -> SwitchIgnition {
    let result = match mgs.client.ignition_list().await {
        Ok(response) => sled_cubby_ignition(response.into_inner()),
        Err(error) => Err(InlineErrorChain::new(&error).to_string()),
    };
    match result {
        Ok(sleds) => {
            info!(
                log,
                "fetched the switch's ignition state";
                "switch" => ?switch,
                "mgs_addr" => %mgs.addr,
            );
            SwitchIgnition::Fetched { sleds }
        }
        Err(error) => {
            warn!(
                log,
                "failed to fetch the switch's ignition state";
                "switch" => ?switch,
                "mgs_addr" => %mgs.addr,
                "error" => &error,
            );
            SwitchIgnition::Failed { error }
        }
    }
}

// TODO-RAINCLAUDE: Returns the sled cubbies in one switch's ignition list. A
// list that names a sled cubby twice can't be trusted for any cubby, so the
// whole list is rejected.
fn sled_cubby_ignition(
    list: Vec<SpIgnitionInfo>,
) -> Result<IdOrdMap<SledCubbyIgnition>, String> {
    let mut sleds = IdOrdMap::new();
    for info in list {
        match info.id.typ {
            SpType::Sled => {
                let slot = info.id.slot;
                sleds
                    .insert_unique(SledCubbyIgnition {
                        slot,
                        ignition: info.details,
                    })
                    .map_err(|_| {
                        format!(
                            "the ignition list names sled cubby {slot} more \
                             than once"
                        )
                    })?;
            }
            SpType::Switch | SpType::Power => {}
        }
    }
    Ok(sleds)
}

// TODO-RAINCLAUDE: Identifies the board in each of the given sled cubbies. All
// of the cubbies are queried at once, which is fine because a rack has at most
// 32 sled cubbies.
async fn identify_cubbies(
    log: &Logger,
    clients: &[GatewayClient],
    slots: &BTreeSet<u16>,
) -> IdOrdMap<SledCubbyIdentityEntry> {
    let entries = futures::future::join_all(
        slots.iter().map(|slot| identify_cubby(log, clients, *slot)),
    )
    .await;
    IdOrdMap::from_iter_unique(entries)
        .expect("slots is a set, so each cubby is identified once")
}

// TODO-RAINCLAUDE: Identifies the board in one sled cubby by asking each MGS in
// turn until one reads a board there. A board read through either switch is
// conclusive. If no MGS reads one, only the first MGS's reason is kept, because
// `SledCubbyIdentity::Unidentified` carries a single reason.
async fn identify_cubby(
    log: &Logger,
    clients: &[GatewayClient],
    slot: u16,
) -> SledCubbyIdentityEntry {
    let mut first_unidentified = None;
    for mgs in clients {
        match sp_get_cubby(log, mgs, slot).await {
            Ok((baseboard_id, _power_state)) => {
                return SledCubbyIdentityEntry {
                    slot,
                    identity: SledCubbyIdentity::Known(baseboard_id),
                };
            }
            Err(reason) => {
                first_unidentified.get_or_insert(reason);
            }
        }
    }
    SledCubbyIdentityEntry {
        slot,
        identity: SledCubbyIdentity::Unidentified(first_unidentified.expect(
            "a cubby is identified only once both switches' ignition lists came through MGSes of the rack, so it has at least one MGS",
        )),
    }
}

// TODO-RAINCLAUDE: Reads the board in one sled cubby through one MGS. Each
// failure is logged here, because `identify_cubby` keeps only the first MGS's
// reason.
async fn sp_get_cubby(
    log: &Logger,
    mgs: &GatewayClient,
    slot: u16,
) -> Result<(NonEmptyBaseboardId, PowerState), CubbyUnidentified> {
    let state = match mgs.client.sp_get(&SpType::Sled, slot).await {
        Ok(response) => response.into_inner(),
        Err(error) => {
            let kind = classify_sp_get_error(&error);
            let error = InlineErrorChain::new(&error).to_string();
            return Err(match kind {
                SpGetFailure::SpUnreachable => {
                    info!(
                        log,
                        "MGS could not reach the SP in a sled cubby";
                        "mgs_addr" => %mgs.addr,
                        "slot" => slot,
                        "error" => &error,
                    );
                    CubbyUnidentified::SpUnreachable { error }
                }
                SpGetFailure::MgsRequestFailed => {
                    warn!(
                        log,
                        "reading a sled cubby's SP state through MGS failed";
                        "mgs_addr" => %mgs.addr,
                        "slot" => slot,
                        "error" => &error,
                    );
                    CubbyUnidentified::MgsRequestFailed { error }
                }
            });
        }
    };
    let reported = BaseboardId {
        part_number: state.model,
        serial_number: state.serial_number,
    };
    match NonEmptyBaseboardId::new(reported) {
        Ok(baseboard_id) => Ok((baseboard_id, state.power_state)),
        Err(reported) => {
            warn!(
                log,
                "a sled cubby's SP reported an empty baseboard identity";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "reported" => %reported.baseboard_id(),
            );
            Err(CubbyUnidentified::EmptyIdentity { reported })
        }
    }
}

// TODO-RAINCLAUDE: Why an `sp_get` through MGS failed:
// * `SpUnreachable` means MGS itself answered that it could not communicate
//   with the SP.
// * `MgsRequestFailed` is anything else, including not reaching MGS at all. It
//   says nothing about the SP, so it must never count as evidence that the SP
//   is gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpGetFailure {
    SpUnreachable,
    MgsRequestFailed,
}

// TODO-RAINCLAUDE: Classifies an `sp_get` error. Only an error response from
// MGS with the `SpCommunicationFailed` code counts as `SpUnreachable`.
//
// `SpCommunicationFailed` is also MGS's catch-all for failures that have
// nothing to do with the SP. Counting those as `SpUnreachable` is safe, because
// `SpUnreachable` on its own never leads us to conclude that a cubby is empty.
fn classify_sp_get_error(error: &MgsClientError) -> SpGetFailure {
    match error {
        gateway_client::Error::ErrorResponse(response) => {
            match response.error_code.as_deref() {
                Some("SpCommunicationFailed") => SpGetFailure::SpUnreachable,
                Some(_) | None => SpGetFailure::MgsRequestFailed,
            }
        }
        gateway_client::Error::InvalidRequest(_)
        | gateway_client::Error::CommunicationError(_)
        | gateway_client::Error::InvalidUpgrade(_)
        | gateway_client::Error::ResponseBodyError(_)
        | gateway_client::Error::InvalidResponsePayload(_, _)
        | gateway_client::Error::UnexpectedResponse(_)
        | gateway_client::Error::Custom(_) => SpGetFailure::MgsRequestFailed,
    }
}

// TODO-RAINCLAUDE: How `power_off_sled_at_slot` ended:
// * `PoweredDown` means some MGS read our board in the cubby, and either
//   powered it off or found it already in A2.
// * `NotPoweredOff` means some MGS read our board in the cubby, but none of
//   them powered it off.
// * `NotLocated` means no MGS read our board in the cubby. That alone never
//   means the sled is gone, so the caller runs the rack-wide check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PowerOffAtSlotOutcome {
    PoweredDown(SledPoweredDown),
    NotPoweredOff {
        failed_attempts: IdOrdMap<SledExpungeMgsPowerOffFailure>,
    },
    NotLocated {
        other_board: Option<SledExpungeOtherBoard>,
        failed_attempts: IdOrdMap<SledExpungeMgsPowerOffFailure>,
    },
}

async fn power_off_through_mgs(
    log: &Logger,
    mgs: &GatewayClient,
    target: &BaseboardId,
    slot: u16,
) -> Result<SledPoweredDown, MgsPowerOffFailure> {
    // TODO-RAINCLAUDE: We re-check the cubby's full `BaseboardId` right before
    // the power-off, because the sled may have been moved or replaced since its
    // cubby was recorded or found.
    //
    // A sled can still be swapped between this check and the power-off. We
    // accept that race, because moving a sled cuts its power anyway, and the
    // removal has already reached `Committing`.
    let (found, power_state) = sp_get_cubby(log, mgs, slot)
        .await
        .map_err(|reason| MgsPowerOffFailure::Unidentified { reason })?;
    if found.as_baseboard_id() != target {
        warn!(
            log,
            "not powering off sled: its cubby holds another board";
            "mgs_addr" => %mgs.addr,
            "slot" => slot,
            "target" => %target,
            "found" => %found,
        );
        return Err(MgsPowerOffFailure::OtherBoardPresent { found });
    }
    match power_state {
        PowerState::A2 => {
            info!(
                log,
                "not powering off sled: it is already in A2";
                "mgs_addr" => %mgs.addr,
                "slot" => slot,
                "target" => %target,
            );
            return Ok(SledPoweredDown::AlreadyOff { mgs: mgs.addr });
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
            Ok(SledPoweredDown::PoweredOff { mgs: mgs.addr })
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
            Err(MgsPowerOffFailure::IgnitionPowerOffFailed { error })
        }
    }
}

// TODO-RAINCLAUDE: Powers off the sled in one cubby, trying one MGS at a time.
// Every MGS reaches the same SP, so one MGS powering the sled down is enough.
// This is a plain loop rather than nexus-mgs-updates'
// `MgsClients::try_all_serially`, because that fails over only on communication
// errors and returns only the last error.
pub async fn power_off_sled_at_slot(
    log: &Logger,
    gateways: &SledPowerOffGateways,
    target: &BaseboardId,
    slot: u16,
) -> PowerOffAtSlotOutcome {
    let mut so_far =
        AttemptsSoFar::NotLocated { failed_attempts: IdOrdMap::new() };
    for mgs in &gateways.clients {
        let failure = match power_off_through_mgs(log, mgs, target, slot).await
        {
            Ok(powered_down) => {
                return PowerOffAtSlotOutcome::PoweredDown(powered_down);
            }
            Err(failure) => failure,
        };
        so_far = match (so_far, failure) {
            (
                AttemptsSoFar::NotLocated { failed_attempts },
                MgsPowerOffFailure::OtherBoardPresent { found },
            ) => {
                return PowerOffAtSlotOutcome::NotLocated {
                    other_board: Some(SledExpungeOtherBoard {
                        mgs: mgs.addr,
                        found,
                    }),
                    failed_attempts,
                };
            }
            (
                AttemptsSoFar::NotLocated { mut failed_attempts },
                failure @ MgsPowerOffFailure::Unidentified { .. },
            ) => {
                insert_attempt(&mut failed_attempts, mgs, failure);
                AttemptsSoFar::NotLocated { failed_attempts }
            }
            (
                AttemptsSoFar::NotLocated { mut failed_attempts }
                | AttemptsSoFar::Located { mut failed_attempts },
                failure @ MgsPowerOffFailure::IgnitionPowerOffFailed { .. },
            ) => {
                insert_attempt(&mut failed_attempts, mgs, failure);
                AttemptsSoFar::Located { failed_attempts }
            }
            (
                AttemptsSoFar::Located { mut failed_attempts },
                failure @ MgsPowerOffFailure::OtherBoardPresent { .. },
            ) => {
                insert_attempt(&mut failed_attempts, mgs, failure);
                return PowerOffAtSlotOutcome::NotPoweredOff {
                    failed_attempts,
                };
            }
            (
                AttemptsSoFar::Located { mut failed_attempts },
                failure @ MgsPowerOffFailure::Unidentified { .. },
            ) => {
                insert_attempt(&mut failed_attempts, mgs, failure);
                AttemptsSoFar::Located { failed_attempts }
            }
        };
    }
    match so_far {
        AttemptsSoFar::NotLocated { failed_attempts } => {
            PowerOffAtSlotOutcome::NotLocated {
                other_board: None,
                failed_attempts,
            }
        }
        AttemptsSoFar::Located { failed_attempts } => {
            PowerOffAtSlotOutcome::NotPoweredOff { failed_attempts }
        }
    }
}

enum AttemptsSoFar {
    NotLocated { failed_attempts: IdOrdMap<SledExpungeMgsPowerOffFailure> },
    Located { failed_attempts: IdOrdMap<SledExpungeMgsPowerOffFailure> },
}

fn insert_attempt(
    failed_attempts: &mut IdOrdMap<SledExpungeMgsPowerOffFailure>,
    mgs: &GatewayClient,
    failure: MgsPowerOffFailure,
) {
    failed_attempts
        .insert_unique(SledExpungeMgsPowerOffFailure { mgs: mgs.addr, failure })
        .expect("SledPowerOffGateways::new drops duplicate MGS addresses");
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway_test_utils::setup::TwoSwitchSimRack;
    use gateway_types::component::SpIdentifier;
    use gateway_types::ignition::SpIgnition;
    use gateway_types::ignition::SpIgnitionSystemType;
    use nexus_types::internal_api::background::SledExpungeRackAbsence;
    use omicron_test_utils::dev::tcp_proxy::ProxyTarget;
    use omicron_test_utils::dev::tcp_proxy::RetargetableTcpProxy;
    use omicron_test_utils::dev::test_setup_log;
    use sp_sim::Responsiveness;
    use sp_sim::SimulatedSp;

    async fn sim_gimlet(sim: &TwoSwitchSimRack, slot: usize) -> BaseboardId {
        let state = sim.simrack.gimlets[slot].state().await;
        BaseboardId {
            part_number: state.model,
            serial_number: state.serial_number,
        }
    }

    fn sim_power_state(sim: &TwoSwitchSimRack, slot: usize) -> PowerState {
        let state = *sim.simrack.gimlets[slot]
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

    fn non_empty(baseboard_id: BaseboardId) -> NonEmptyBaseboardId {
        NonEmptyBaseboardId::new(baseboard_id)
            .expect("sim gimlets report non-empty baseboards")
    }

    // TODO-RAINCLAUDE: Clears the error text from a failure. Error texts carry
    // MGS and transport details, so the tests compare attempts only by MGS, by
    // failure kind, and by any board found.
    fn without_error(failure: MgsPowerOffFailure) -> MgsPowerOffFailure {
        match failure {
            MgsPowerOffFailure::Unidentified {
                reason: CubbyUnidentified::MgsRequestFailed { error: _ },
            } => request_failed(),
            MgsPowerOffFailure::Unidentified {
                reason: CubbyUnidentified::SpUnreachable { error: _ },
            } => unreachable(),
            MgsPowerOffFailure::IgnitionPowerOffFailed { error: _ } => {
                MgsPowerOffFailure::IgnitionPowerOffFailed {
                    error: String::new(),
                }
            }
            failure @ (MgsPowerOffFailure::Unidentified {
                reason:
                    CubbyUnidentified::EmptyIdentity { .. }
                    | CubbyUnidentified::UnexpectedSystemType { .. },
            }
            | MgsPowerOffFailure::OtherBoardPresent { .. }) => failure,
        }
    }

    fn attempts_without_errors(
        attempts: IdOrdMap<SledExpungeMgsPowerOffFailure>,
    ) -> IdOrdMap<SledExpungeMgsPowerOffFailure> {
        attempts
            .into_iter()
            .map(|attempt| SledExpungeMgsPowerOffFailure {
                mgs: attempt.mgs,
                failure: without_error(attempt.failure),
            })
            .collect()
    }

    fn without_errors(outcome: PowerOffAtSlotOutcome) -> PowerOffAtSlotOutcome {
        match outcome {
            PowerOffAtSlotOutcome::PoweredDown(_) => outcome,
            PowerOffAtSlotOutcome::NotPoweredOff { failed_attempts } => {
                PowerOffAtSlotOutcome::NotPoweredOff {
                    failed_attempts: attempts_without_errors(failed_attempts),
                }
            }
            PowerOffAtSlotOutcome::NotLocated {
                other_board,
                failed_attempts,
            } => PowerOffAtSlotOutcome::NotLocated {
                other_board,
                failed_attempts: attempts_without_errors(failed_attempts),
            },
        }
    }

    fn attempts<I>(entries: I) -> IdOrdMap<SledExpungeMgsPowerOffFailure>
    where
        I: IntoIterator<Item = (SocketAddrV6, MgsPowerOffFailure)>,
    {
        entries
            .into_iter()
            .map(|(mgs, failure)| SledExpungeMgsPowerOffFailure {
                mgs,
                failure,
            })
            .collect()
    }

    fn unreachable() -> MgsPowerOffFailure {
        MgsPowerOffFailure::Unidentified {
            reason: CubbyUnidentified::SpUnreachable { error: String::new() },
        }
    }

    fn request_failed() -> MgsPowerOffFailure {
        MgsPowerOffFailure::Unidentified {
            reason: CubbyUnidentified::MgsRequestFailed {
                error: String::new(),
            },
        }
    }

    // TODO-RAINCLAUDE: Shows how `power_off_sled_at_slot` handles another board
    // in the cubby, a sled already in A2, an SP that no MGS can reach, and
    // failing over to a second MGS.
    #[tokio::test]
    async fn test_power_off_sled_at_slot() {
        let logctx = test_setup_log("test_power_off_sled_at_slot");
        let log = &logctx.log;
        let sim =
            TwoSwitchSimRack::start("test_power_off_sled_at_slot", log, 2)
                .await;
        let up0 = GatewayClient::from_addr(log, sim.mgs0.address());
        let up1 = GatewayClient::from_addr(log, sim.mgs1.address());
        let refusing = RetargetableTcpProxy::bind(ProxyTarget::Refuse, log)
            .await
            .expect("bound a refusing TCP proxy");
        let down = GatewayClient::from_addr(log, refusing.local_addr());
        let sled0 = sim_gimlet(&sim, 0).await;
        let sled1 = sim_gimlet(&sim, 1).await;

        let both = SledPowerOffGateways::new(
            &[up0.clone(), up1.clone()],
            IdOrdMap::new(),
        );
        let first = if up0.addr < up1.addr { &up0 } else { &up1 };
        assert_eq!(
            power_off_sled_at_slot(log, &both, &sled1, 0).await,
            PowerOffAtSlotOutcome::NotLocated {
                other_board: Some(SledExpungeOtherBoard {
                    mgs: first.addr,
                    found: non_empty(sled0.clone()),
                }),
                failed_attempts: IdOrdMap::new(),
            },
        );
        assert_eq!(sim_power_state(&sim, 0), PowerState::A0);
        assert!(ignition_power(&up0, 0).await, "cubby 0 stays powered");

        let only_up0 = SledPowerOffGateways::new(
            std::slice::from_ref(&up0),
            IdOrdMap::new(),
        );
        up0.client
            .sp_power_state_set(
                &SpType::Sled,
                0,
                gateway_client::types::PowerState::A2,
            )
            .await
            .expect("put sim gimlet 0 in A2");
        assert_eq!(
            power_off_sled_at_slot(log, &only_up0, &sled0, 0).await,
            PowerOffAtSlotOutcome::PoweredDown(SledPoweredDown::AlreadyOff {
                mgs: up0.addr,
            }),
        );
        assert!(ignition_power(&up0, 0).await, "cubby 0 stays powered");

        sim.simrack.gimlets[1]
            .set_responsiveness(Responsiveness::Unresponsive)
            .await;
        let all = SledPowerOffGateways::new(
            &[up0.clone(), up1.clone(), down.clone(), up0.clone()],
            IdOrdMap::new(),
        );
        assert_eq!(
            without_errors(power_off_sled_at_slot(log, &all, &sled1, 1).await),
            PowerOffAtSlotOutcome::NotLocated {
                other_board: None,
                failed_attempts: attempts([
                    (down.addr, request_failed()),
                    (up0.addr, unreachable()),
                    (up1.addr, unreachable()),
                ]),
            },
        );
        sim.simrack.gimlets[1]
            .set_responsiveness(Responsiveness::Responsive)
            .await;

        // TODO-RAINCLAUDE: `SledPowerOffGateways` tries MGSes in address order,
        // so the proxy with the lower address is left refusing connections, and
        // the other one forwards to switch 0's MGS. That way the first MGS
        // tried always fails and the second one powers the sled off, whichever
        // way the two addresses sort.
        let proxy_a = RetargetableTcpProxy::bind(ProxyTarget::Refuse, log)
            .await
            .expect("bound a TCP proxy");
        let proxy_b = RetargetableTcpProxy::bind(ProxyTarget::Refuse, log)
            .await
            .expect("bound a TCP proxy");
        let (down_proxy, up_proxy) =
            if proxy_a.local_addr() < proxy_b.local_addr() {
                (&proxy_a, &proxy_b)
            } else {
                (&proxy_b, &proxy_a)
            };
        up_proxy
            .set_target(ProxyTarget::Forward(sim.mgs0.address().into()))
            .await;
        let down_first = SledPowerOffGateways::new(
            &[
                GatewayClient::from_addr(log, up_proxy.local_addr()),
                GatewayClient::from_addr(log, down_proxy.local_addr()),
            ],
            IdOrdMap::new(),
        );
        assert_eq!(
            power_off_sled_at_slot(log, &down_first, &sled1, 1).await,
            PowerOffAtSlotOutcome::PoweredDown(SledPoweredDown::PoweredOff {
                mgs: up_proxy.local_addr(),
            }),
        );
        assert!(!ignition_power(&up0, 1).await, "cubby 1 is unpowered");
        assert!(ignition_power(&up0, 0).await, "cubby 0 stays powered");
        assert_eq!(
            without_errors(
                power_off_sled_at_slot(log, &only_up0, &sled1, 1).await
            ),
            PowerOffAtSlotOutcome::NotLocated {
                other_board: None,
                failed_attempts: attempts([(up0.addr, unreachable())]),
            },
        );
        for gateways in [&both, &only_up0, &all, &down_first] {
            assert_eq!(
                gateways.switch_status(),
                None,
                "powering off needs no switch evidence",
            );
        }

        refusing.shutdown().await;
        proxy_a.shutdown().await;
        proxy_b.shutdown().await;
        sim.teardown().await.expect("simulated rack was torn down cleanly");
        logctx.cleanup_successful();
    }

    // TODO-RAINCLAUDE: Shows each result of `live_sled_presence`, and the
    // errors for a switch with no MGS and for a switch that two MGSes claim.
    // `flaky` is a proxy in front of switch 1's MGS, so passing it alongside
    // the real one makes two MGSes claim switch 1.
    #[tokio::test]
    async fn test_live_sled_presence() {
        let logctx = test_setup_log("test_live_sled_presence");
        let log = &logctx.log;
        let sim =
            TwoSwitchSimRack::start("test_live_sled_presence", log, 2).await;
        let up0 = GatewayClient::from_addr(log, sim.mgs0.address());
        let up1 = GatewayClient::from_addr(log, sim.mgs1.address());
        let flaky_proxy = RetargetableTcpProxy::bind(
            ProxyTarget::Forward(sim.mgs1.address().into()),
            log,
        )
        .await
        .expect("bound a TCP proxy for switch 1's MGS");
        let flaky = GatewayClient::from_addr(log, flaky_proxy.local_addr());
        let sled0 = sim_gimlet(&sim, 0).await;
        let sled1 = sim_gimlet(&sim, 1).await;
        let missing = BaseboardId {
            part_number: sled0.part_number.clone(),
            serial_number: "not-in-the-rack".to_string(),
        };

        let both = SledPowerOffGateways::new(
            &[up0.clone(), up1.clone()],
            IdOrdMap::new(),
        );
        assert_eq!(
            both.live_sled_presence(log, &sled1).await,
            Ok(SledExpungeLivePresence::InCubby { slot: 1 }),
        );
        assert_eq!(
            both.live_sled_presence(log, &missing).await,
            Ok(SledExpungeLivePresence::Gone {
                absence: SledExpungeRackAbsence {
                    switch0_mgs: up0.addr,
                    switch1_mgs: up1.addr,
                    other_boards: [(0, sled0.clone()), (1, sled1.clone())]
                        .into_iter()
                        .map(|(slot, board)| SledCubbyBoard {
                            slot,
                            baseboard_id: non_empty(board),
                        })
                        .collect(),
                },
            }),
        );
        let fetched = |mgs: &GatewayClient| SledExpungeSwitchMgs::Found {
            mgs: mgs.addr,
            ignition: SledExpungeIgnitionSnapshot::Fetched,
        };
        assert_eq!(
            both.switch_status(),
            Some(SledExpungeSwitchIdentification::Identified {
                switch_unknown: IdOrdMap::new(),
                switch0: fetched(&up0),
                switch1: fetched(&up1),
            }),
        );

        sim.simrack.gimlets[1]
            .set_responsiveness(Responsiveness::Unresponsive)
            .await;
        let unresponsive = SledPowerOffGateways::new(
            &[up0.clone(), up1.clone()],
            IdOrdMap::new(),
        );
        match unresponsive.live_sled_presence(log, &missing).await {
            Ok(SledExpungeLivePresence::Unidentified { cubbies }) => {
                let slots: Vec<_> =
                    cubbies
                        .iter()
                        .map(|cubby| {
                            match &cubby.reason {
                        CubbyUnidentified::SpUnreachable { error: _ } => {
                            cubby.slot
                        }
                        reason @ (CubbyUnidentified::EmptyIdentity { .. }
                        | CubbyUnidentified::MgsRequestFailed { .. }
                        | CubbyUnidentified::UnexpectedSystemType {
                            ..
                        }) => {
                            panic!(
                                "expected cubby {} unreachable, found \
                                 {reason:?}",
                                cubby.slot,
                            )
                        }
                    }
                        })
                        .collect();
                assert_eq!(slots, vec![1]);
            }
            other @ (Ok(
                SledExpungeLivePresence::InCubby { .. }
                | SledExpungeLivePresence::Gone { .. },
            )
            | Err(_)) => {
                panic!("expected cubby 1 unidentified, found {other:?}")
            }
        }
        // TODO-RAINCLAUDE: A cubby the caller has already identified needs no
        // `sp_get`, so the unresponsive SP in cubby 1 no longer stops us from
        // ruling the missing board out.
        let identified: IdOrdMap<_> = [SledCubbyBoard {
            slot: 1,
            baseboard_id: non_empty(sled1.clone()),
        }]
        .into_iter()
        .collect();
        let with_identified =
            SledPowerOffGateways::new(&[up0.clone(), up1.clone()], identified);
        assert_eq!(
            with_identified.live_sled_presence(log, &missing).await,
            Ok(SledExpungeLivePresence::Gone {
                absence: SledExpungeRackAbsence {
                    switch0_mgs: up0.addr,
                    switch1_mgs: up1.addr,
                    other_boards: [(0, sled0.clone()), (1, sled1.clone())]
                        .into_iter()
                        .map(|(slot, board)| SledCubbyBoard {
                            slot,
                            baseboard_id: non_empty(board),
                        })
                        .collect(),
                },
            }),
        );
        sim.simrack.gimlets[1]
            .set_responsiveness(Responsiveness::Responsive)
            .await;

        let only_up0 = SledPowerOffGateways::new(
            std::slice::from_ref(&up0),
            IdOrdMap::new(),
        );
        assert_eq!(
            only_up0.live_sled_presence(log, &missing).await,
            Err(SledExpungeIgnitionUnavailable::Switch1 {
                switch1: SledExpungeIgnitionGap::NoMgs,
            }),
        );
        assert_eq!(
            only_up0.switch_status(),
            Some(SledExpungeSwitchIdentification::Identified {
                switch_unknown: IdOrdMap::new(),
                switch0: fetched(&up0),
                switch1: SledExpungeSwitchMgs::NotFound,
            }),
        );

        let conflict = SledPowerOffGateways::new(
            &[up0.clone(), up1.clone(), flaky.clone()],
            IdOrdMap::new(),
        );
        assert_eq!(
            conflict.live_sled_presence(log, &missing).await,
            Err(SledExpungeIgnitionUnavailable::Switch1 {
                switch1: SledExpungeIgnitionGap::MgsConflict,
            }),
        );
        let (first, second) = if up1.addr < flaky.addr {
            (up1.addr, flaky.addr)
        } else {
            (flaky.addr, up1.addr)
        };
        assert_eq!(
            conflict.switch_status(),
            Some(SledExpungeSwitchIdentification::Identified {
                switch_unknown: IdOrdMap::new(),
                switch0: fetched(&up0),
                switch1: SledExpungeSwitchMgs::Conflict {
                    first,
                    second,
                    rest: BTreeSet::new(),
                },
            }),
        );

        flaky_proxy.shutdown().await;
        sim.teardown().await.expect("simulated rack was torn down cleanly");
        logctx.cleanup_successful();
    }

    #[test]
    fn test_sled_cubby_ignition() {
        let entry = |typ, slot, power| SpIgnitionInfo {
            id: SpIdentifier { typ, slot },
            details: SpIgnition::Present {
                id: SpIgnitionSystemType::Gimlet,
                power,
                ctrl_detect_0: true,
                ctrl_detect_1: true,
                flt_a3: false,
                flt_a2: false,
                flt_rot: false,
                flt_sp: false,
            },
        };
        let list = vec![
            entry(SpType::Sled, 0, true),
            entry(SpType::Switch, 0, true),
            entry(SpType::Power, 0, true),
            entry(SpType::Sled, 1, false),
        ];
        let sleds =
            sled_cubby_ignition(list.clone()).expect("each sled cubby once");
        assert_eq!(
            sleds.iter().map(|cubby| cubby.slot).collect::<Vec<_>>(),
            vec![0, 1],
        );
        let mut duplicated = list;
        duplicated.push(entry(SpType::Sled, 1, true));
        assert_eq!(
            sled_cubby_ignition(duplicated),
            Err("the ignition list names sled cubby 1 more than once"
                .to_string()),
        );
    }

    #[test]
    fn test_classify_sp_get_error() {
        let response = |error_code: Option<&str>| {
            gateway_client::Error::ErrorResponse(
                gateway_client::ResponseValue::new(
                    gateway_client::types::Error {
                        error_code: error_code.map(str::to_string),
                        message: String::new(),
                        request_id: String::new(),
                    },
                    reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    reqwest::header::HeaderMap::new(),
                ),
            )
        };
        let cases = [
            (
                response(Some("SpCommunicationFailed")),
                SpGetFailure::SpUnreachable,
            ),
            (
                response(Some("DiscoveryNotYetComplete")),
                SpGetFailure::MgsRequestFailed,
            ),
            (response(Some("Timeout")), SpGetFailure::MgsRequestFailed),
            (response(None), SpGetFailure::MgsRequestFailed),
            (
                gateway_client::Error::InvalidRequest(String::new()),
                SpGetFailure::MgsRequestFailed,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(classify_sp_get_error(&error), expected, "{error}");
        }
    }
}
