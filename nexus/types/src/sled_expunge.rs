// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: Checks and types for creating sled expunge requests and
// carrying them out (RFD 747).
//
// Nothing in this module reads the database or talks to MGS, so each rule can
// be tested on its own.

use crate::external_api::sled::SledPolicy;
use crate::external_api::sled::SledProvisionPolicy;
use crate::external_api::sled::SledState;
use crate::internal_api::background::SledExpungeRackAbsence;
use crate::inventory::Collection;
use crate::inventory::SpType;
use crate::trust_quorum::RemoveSledError;
use crate::trust_quorum::TrustQuorumConfig;
use crate::trust_quorum::TrustQuorumConfigState;
use chrono::DateTime;
use chrono::Utc;
use gateway_types::ignition::SpIgnition;
use gateway_types::ignition::SpIgnitionSystemType;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::SledRole;
use sled_hardware_types::BaseboardId;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddrV6;
use trust_quorum_types::membership::TRUST_QUORUM_MIN_MEMBERS;
use trust_quorum_types::membership::TrustQuorumMembersError;
use trust_quorum_types::types::Epoch;

// TODO-RAINCLAUDE: The sled cubbies that are cabled to the rack's switches. A
// sled in one of these cubbies is a scrimlet.
//
// These values mirror the `local_sled` values in `smf/mgs/config.toml`. Note
// that the simulated and test MGS configs use slots 0 and 1 instead, so a
// simulated rack's scrimlets are not in these cubbies.
pub const SWITCH_ATTACHED_SLED_CUBBIES: [u16; 2] = [14, 16];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SledExpungeRequestFinishedState {
    Done,
    Failed,
}

impl SledExpungeRequestFinishedState {
    pub fn kind(&self) -> SledExpungeRequestStateKind {
        match self {
            Self::Done => SledExpungeRequestStateKind::Done,
            Self::Failed => SledExpungeRequestStateKind::Failed,
        }
    }
}

impl fmt::Display for SledExpungeRequestFinishedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FinishedSledExpungeRequest {
    pub id: SledExpungeRequestUuid,
    pub state: SledExpungeRequestFinishedState,
}

// TODO-RAINCLAUDE: The refusal returned by
// `check_sled_expunge_policy_and_state`.
//
// This is its own type rather than an `Error` because the error message names
// the sled's most recent finished request. The caller only has to read that
// request if the check fails, and then passes it to `into_error`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub struct SledNotExpungeable {
    sled_id: SledUuid,
    policy: SledPolicy,
    state: SledState,
}

impl SledNotExpungeable {
    pub fn into_error(
        self,
        latest_finished_request: Option<FinishedSledExpungeRequest>,
    ) -> Error {
        let Self { sled_id, policy, state } = self;
        let finished = match latest_finished_request {
            Some(FinishedSledExpungeRequest { id, state }) => format!(
                " The sled's most recent expunge request, {id}, is {state}; \
                 use sled_expunge_request_view to see its details."
            ),
            None => String::new(),
        };
        match policy {
            SledPolicy::Expunged => Error::conflict(format!(
                "Cannot expunge sled {sled_id}: it is already expunged (its \
                 state is {state}).{finished}"
            )),
            SledPolicy::InService { .. } => Error::conflict(format!(
                "Cannot expunge sled {sled_id}: its policy is {policy} and its \
                 state is {state}, but only a sled that is in service and \
                 active can be expunged.{finished}"
            )),
        }
    }
}

// TODO-RAINCLAUDE: Checks that the sled is in service and active, which is the
// only kind of sled that can be expunged.
//
// A non-provisionable sled is accepted. Creating a request makes the sled
// non-provisionable, so a request that fails leaves it that way, and the
// operator must be able to submit another one.
pub fn check_sled_expunge_policy_and_state(
    sled_id: SledUuid,
    policy: SledPolicy,
    state: SledState,
) -> Result<(), SledNotExpungeable> {
    match (policy, state) {
        (
            SledPolicy::InService {
                provision_policy:
                    SledProvisionPolicy::Provisionable
                    | SledProvisionPolicy::NonProvisionable,
            },
            SledState::Active,
        ) => Ok(()),
        (SledPolicy::InService { .. }, SledState::Decommissioned)
        | (
            SledPolicy::Expunged,
            SledState::Active | SledState::Decommissioned,
        ) => Err(SledNotExpungeable { sled_id, policy, state }),
    }
}

// TODO-RAINCLAUDE: Checks that the sled row has a part number and a serial
// number.
//
// The power-off step finds the sled by its baseboard, and it treats a board
// that reports an empty part or serial number as unknown. So a request with an
// empty one could never find its sled. The operator can't fix the sled row, so
// we return a 409 that points them at Oxide support.
pub fn check_sled_expunge_baseboard_recorded(
    sled_id: SledUuid,
    baseboard_id: &BaseboardId,
) -> Result<(), Error> {
    if !is_empty_baseboard_id(baseboard_id) {
        return Ok(());
    }
    Err(Error::conflict(format!(
        "Cannot expunge sled {sled_id}: it has no recorded part or serial \
         number (part number {:?}, serial number {:?}), so it can't be \
         expunged through this API. Contact Oxide support.",
        baseboard_id.part_number, baseboard_id.serial_number,
    )))
}

// TODO-RAINCLAUDE: Checks that the operator confirmed the request by repeating
// the sled's serial number.
//
// The two serial numbers are compared exactly, with no normalization.
pub fn check_sled_expunge_serial_acknowledged(
    sled_id: SledUuid,
    sled_serial: &str,
    acknowledged_serial: &str,
) -> Result<(), Error> {
    if sled_serial == acknowledged_serial {
        return Ok(());
    }
    Err(Error::invalid_request(format!(
        "Cannot expunge sled {sled_id}: the serial number given to confirm \
         the request ({acknowledged_serial:?}) does not match the sled's \
         serial number. Check that this is the sled you mean to expunge \
         (its serial number is in the sled's baseboard details, e.g. \
         oxide system hardware sled view), then retry with that serial \
         number."
    )))
}

// TODO-RAINCLAUDE: What inventory and MGS say about whether a sled is a
// scrimlet.
//
// The fields are private, so the only way to build one is `from_collection`,
// optionally followed by `with_mgs_sp_slot`. This keeps the code that
// interprets the collection in one place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SledExpungeScrimletEvidence {
    inventory: ScrimletEvidence,
    mgs_sp_slot: Option<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScrimletEvidence {
    NoCollection,
    Collection { sled_agent_role: Option<SledRole>, sp_record: SpRecord },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpRecord {
    NoRecord,
    NotSledType(NonSledSpType),
    Slot(u16),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "snake_case")]
enum NonSledSpType {
    Switch,
    Power,
}

impl SledExpungeScrimletEvidence {
    pub fn from_collection(
        latest_collection: Option<&Collection>,
        sled_id: SledUuid,
        baseboard_id: &BaseboardId,
    ) -> Self {
        let Some(collection) = latest_collection else {
            return Self {
                inventory: ScrimletEvidence::NoCollection,
                mgs_sp_slot: None,
            };
        };
        let sled_agent_role = collection
            .sled_agents
            .get(&sled_id)
            .map(|sled_agent| sled_agent.sled_role);
        let sp_record = match collection.sp_for(baseboard_id) {
            Ok(sp) => match sp.sp_type {
                SpType::Sled => SpRecord::Slot(sp.sp_slot),
                SpType::Switch => SpRecord::NotSledType(NonSledSpType::Switch),
                SpType::Power => SpRecord::NotSledType(NonSledSpType::Power),
            },
            Err(_not_in_inventory) => SpRecord::NoRecord,
        };
        Self {
            inventory: ScrimletEvidence::Collection {
                sled_agent_role,
                sp_record,
            },
            mgs_sp_slot: None,
        }
    }

    pub fn with_mgs_sp_slot(self, sp_slot: u16) -> Self {
        Self { mgs_sp_slot: Some(sp_slot), ..self }
    }
}

// TODO-RAINCLAUDE: Refuses to expunge a scrimlet, because this API doesn't
// support that.
//
// The rules are, in order:
// * If the latest collection's sled agent record says the sled is a scrimlet,
//   refuse.
// * If the latest collection or MGS found the sled's SP in a switch-attached
//   cubby, refuse. One source is enough, whatever the other one says, because a
//   sled that might be in a switch-attached cubby might be a scrimlet.
// * If there is no sled agent record, and neither source found the SP in a sled
//   cubby, the sled row's role (`sled_row_role`) decides.
//
// The SP's cubby is the only physical evidence here, because the inventory role
// and the sled row's role both come from whether the sled saw a Tofino.
pub fn check_sled_expunge_not_scrimlet(
    sled_id: SledUuid,
    evidence: &SledExpungeScrimletEvidence,
    sled_row_role: SledRole,
) -> Result<(), Error> {
    let refuse = |why: &str| {
        Error::conflict(format!(
            "Cannot expunge sled {sled_id}: it is a scrimlet (it manages a \
             switch), since {why}. Expunging a scrimlet through this API is \
             not supported. Contact Oxide support."
        ))
    };
    let by_sled_row = |why: &str| match sled_row_role {
        SledRole::Scrimlet => Err(refuse(why)),
        SledRole::Gimlet => Ok(()),
    };

    let SledExpungeScrimletEvidence { inventory, mgs_sp_slot } = *evidence;
    let (sled_agent_role, sp_record) = match inventory {
        ScrimletEvidence::NoCollection => (None, None),
        ScrimletEvidence::Collection { sled_agent_role, sp_record } => {
            (sled_agent_role, Some(sp_record))
        }
    };
    let sled_sp_slot = match sp_record {
        Some(SpRecord::Slot(slot)) => Some(slot),
        Some(SpRecord::NoRecord | SpRecord::NotSledType(_)) | None => None,
    };
    match sled_agent_role {
        Some(SledRole::Scrimlet) => {
            return Err(refuse(
                "the latest inventory collection reports its role as scrimlet",
            ));
        }
        Some(SledRole::Gimlet) | None => {}
    }
    if let Some(slot) = sled_sp_slot
        && SWITCH_ATTACHED_SLED_CUBBIES.contains(&slot)
    {
        return Err(refuse(&format!(
            "the latest inventory collection found its service processor in \
             switch-attached cubby {slot}"
        )));
    }
    if let Some(slot) = mgs_sp_slot
        && SWITCH_ATTACHED_SLED_CUBBIES.contains(&slot)
    {
        return Err(refuse(&format!(
            "MGS found its service processor in switch-attached cubby {slot}"
        )));
    }
    let (None, None) = (sled_agent_role, mgs_sp_slot) else {
        return Ok(());
    };
    match sp_record {
        None => by_sled_row(
            "it is recorded as a scrimlet and there is no inventory \
             collection",
        ),
        Some(SpRecord::NoRecord) => by_sled_row(
            "it is recorded as a scrimlet and the latest inventory collection \
             has neither a sled agent record nor a service processor record \
             for it",
        ),
        Some(SpRecord::NotSledType(sp_type)) => by_sled_row(&format!(
            "it is recorded as a scrimlet and the latest inventory collection \
             has no sled agent record for it, and records the service \
             processor with its baseboard as a {sp_type} service processor, \
             not a sled one",
        )),
        Some(SpRecord::Slot(_)) => Ok(()),
    }
}

fn config_state_label(state: TrustQuorumConfigState) -> &'static str {
    match state {
        TrustQuorumConfigState::Preparing => "preparing",
        TrustQuorumConfigState::PreparingLrtqUpgrade => {
            "preparing an LRTQ upgrade"
        }
        TrustQuorumConfigState::Committing => "committing",
        TrustQuorumConfigState::Committed => "committed",
        TrustQuorumConfigState::CommittedPartially => "partially committed",
        TrustQuorumConfigState::Aborted => "aborted",
    }
}

// TODO-RAINCLAUDE: Turns a refusal from
// `ProposedTrustQuorumConfig::remove_sled` into an error worded for the expunge
// request. The sled must be a member of the trust quorum, and removing it must
// leave the trust quorum with at least the minimum number of members.
//
// `config` is the last committed configuration. Creation has already refused
// the request if a reconfiguration is in flight.
pub fn sled_expunge_removal_refusal(
    sled_id: SledUuid,
    config: &TrustQuorumConfig,
    refusal: RemoveSledError,
) -> Error {
    match refusal {
        RemoveSledError::NotMember { sled } => Error::invalid_request(format!(
            "Cannot expunge sled {sled_id}: its baseboard {sled} is not a \
             member of the last committed trust quorum configuration for \
             rack {} (epoch {}, {}). This API only expunges sleds that are trust \
             quorum members. If this sled has already left the trust quorum, \
             contact Oxide support to expunge it.",
            config.rack_id,
            config.epoch,
            config_state_label(config.state),
        )),
        RemoveSledError::InvalidMembership {
            rack_id,
            sled: _,
            error: TrustQuorumMembersError::TooFew { num_members: remaining },
        } => Error::conflict(format!(
            "Cannot expunge sled {sled_id}: the trust quorum for rack \
             {rack_id} has {} sleds and must keep at least \
             {TRUST_QUORUM_MIN_MEMBERS}, so this sled can't be removed \
             from it. Contact Oxide support.",
            remaining + 1,
        )),
        RemoveSledError::InvalidMembership {
            rack_id,
            sled: _,
            error: error @ TrustQuorumMembersError::TooMany { .. },
        } => Error::internal_error(&format!(
            "Cannot expunge sled {sled_id}: removing it from the trust \
             quorum for rack {rack_id} would leave too many members; the \
             latest committed configuration is larger than any write path \
             allows: {}",
            InlineErrorChain::new(&error),
        )),
    }
}

// TODO-RAINCLAUDE: How one MGS's attempt to power off the sled in a cubby ended
// without doing so.
//
// Note that `IgnitionPowerOffFailed` doesn't mean the sled is still on. The
// power-off may have taken effect anyway, because the reply can be lost, and
// MGS can still deliver the request after the client times out.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MgsPowerOffFailure {
    Unidentified { reason: CubbyUnidentified },
    OtherBoardPresent { found: NonEmptyBaseboardId },
    IgnitionPowerOffFailed { error: String },
}

impl fmt::Display for MgsPowerOffFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unidentified { reason } => match reason {
                CubbyUnidentified::SpUnreachable { error } => write!(
                    f,
                    "SP unreachable through this MGS; nothing sent: {error}"
                ),
                CubbyUnidentified::MgsRequestFailed { error } => write!(
                    f,
                    "reading the SP's state through this MGS failed; nothing \
                     sent: {error}"
                ),
                CubbyUnidentified::EmptyIdentity { reported } => write!(
                    f,
                    "SP reported an empty baseboard identity ({}); nothing \
                     sent",
                    reported.baseboard_id(),
                ),
                CubbyUnidentified::UnexpectedSystemType { system_type } => {
                    write!(
                        f,
                        "ignition reports system type {system_type:?}; \
                         nothing sent"
                    )
                }
            },
            Self::OtherBoardPresent { found } => {
                write!(f, "SP reported another board ({found}); nothing sent")
            }
            Self::IgnitionPowerOffFailed { error } => {
                write!(f, "ignition power-off failed: {error}")
            }
        }
    }
}

// TODO-RAINCLAUDE: Returns true if the part number or the serial number is
// empty.
//
// An SP whose VPD hasn't been programmed reports an empty part or serial
// number. That says nothing about which board it is, so we treat it as an
// unknown identity, and never as a different board.
pub fn is_empty_baseboard_id(baseboard_id: &BaseboardId) -> bool {
    baseboard_id.part_number.is_empty() || baseboard_id.serial_number.is_empty()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SledExpungeRequest {
    pub id: SledExpungeRequestUuid,
    pub rack_id: RackUuid,
    pub sled_id: SledUuid,
    pub baseboard_id: BaseboardId,
    pub tq_epoch: Epoch,
    pub state: SledExpungeRequestState,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeRequestState {
    WaitingForTrustQuorum {
        cubby: Option<SledExpungeRecordedCubby>,
        last_error: Option<String>,
    },
    PoweringOff {
        cubby: Option<SledExpungeRecordedCubby>,
        last_error: Option<String>,
    },
    Expunging {
        power_off: SledExpungePowerOff,
        last_error: Option<String>,
    },
    Done {
        power_off: SledExpungePowerOff,
        time_completed: DateTime<Utc>,
    },
    Failed {
        power_off: SledExpungePowerOffProgress,
        failure_reason: String,
        time_completed: DateTime<Utc>,
    },
}

impl SledExpungeRequestState {
    pub fn kind(&self) -> SledExpungeRequestStateKind {
        match self {
            Self::WaitingForTrustQuorum { .. } => {
                SledExpungeRequestStateKind::WaitingForTrustQuorum
            }
            Self::PoweringOff { .. } => {
                SledExpungeRequestStateKind::PoweringOff
            }
            Self::Expunging { .. } => SledExpungeRequestStateKind::Expunging,
            Self::Done { .. } => SledExpungeRequestStateKind::Done,
            Self::Failed { .. } => SledExpungeRequestStateKind::Failed,
        }
    }

    pub fn recorded_cubby(&self) -> Option<SledExpungeRecordedCubby> {
        match self {
            Self::WaitingForTrustQuorum { cubby, last_error: _ }
            | Self::PoweringOff { cubby, last_error: _ } => *cubby,
            Self::Expunging { power_off, last_error: _ }
            | Self::Done { power_off, time_completed: _ } => power_off.cubby(),
            Self::Failed {
                power_off,
                failure_reason: _,
                time_completed: _,
            } => power_off.cubby(),
        }
    }

    pub fn last_error(&self) -> Option<&str> {
        match self {
            Self::WaitingForTrustQuorum { cubby: _, last_error }
            | Self::PoweringOff { cubby: _, last_error }
            | Self::Expunging { power_off: _, last_error } => {
                last_error.as_deref()
            }
            Self::Done { power_off: _, time_completed: _ }
            | Self::Failed {
                power_off: _,
                failure_reason: _,
                time_completed: _,
            } => None,
        }
    }

    pub fn settled_power_off(&self) -> Option<SledExpungePowerOff> {
        match self {
            Self::WaitingForTrustQuorum { cubby: _, last_error: _ }
            | Self::PoweringOff { cubby: _, last_error: _ }
            | Self::Failed {
                power_off: SledExpungePowerOffProgress::Pending { cubby: _ },
                failure_reason: _,
                time_completed: _,
            } => None,
            Self::Expunging { power_off, last_error: _ }
            | Self::Done { power_off, time_completed: _ }
            | Self::Failed {
                power_off: SledExpungePowerOffProgress::Settled(power_off),
                failure_reason: _,
                time_completed: _,
            } => Some(*power_off),
        }
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum::VariantArray,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SledExpungeRequestStateKind {
    WaitingForTrustQuorum,
    PoweringOff,
    Expunging,
    Done,
    Failed,
}

impl SledExpungeRequestStateKind {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;

    pub const UNFINISHED: &'static [Self] =
        &[Self::WaitingForTrustQuorum, Self::PoweringOff, Self::Expunging];

    pub fn is_finished(&self) -> bool {
        self.as_unfinished().is_none()
    }

    pub fn as_unfinished(&self) -> Option<SledExpungeRequestUnfinishedState> {
        match self {
            Self::WaitingForTrustQuorum => {
                Some(SledExpungeRequestUnfinishedState::WaitingForTrustQuorum)
            }
            Self::PoweringOff => {
                Some(SledExpungeRequestUnfinishedState::PoweringOff)
            }
            Self::Expunging => {
                Some(SledExpungeRequestUnfinishedState::Expunging)
            }
            Self::Done | Self::Failed => None,
        }
    }

    pub fn as_finished(&self) -> Option<SledExpungeRequestFinishedState> {
        match self {
            Self::Done => Some(SledExpungeRequestFinishedState::Done),
            Self::Failed => Some(SledExpungeRequestFinishedState::Failed),
            Self::WaitingForTrustQuorum
            | Self::PoweringOff
            | Self::Expunging => None,
        }
    }

    pub fn label(&self) -> &'static str {
        self.into()
    }
}

impl fmt::Display for SledExpungeRequestStateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SledExpungeRecordedCubby {
    pub slot: u16,
    pub source: SledExpungeSpSlotSource,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SledExpungeSpSlotSource {
    Inventory,
    LiveScan,
    FoundAfterAbsent,
    Relocated,
}

// TODO-RAINCLAUDE: How the powering-off step ended, together with the cubby
// that the outcome is about.
//
// The outcome and the cubby are in one type because each outcome either needs a
// cubby or can't have one:
//
// * `Cubby` is for the four outcomes that come from checking a cubby
//   (`PoweredOff`, `AlreadyOff`, `OtherBoardPresent` and `NotPresent`), and it
//   carries that cubby.
// * `AbsentAtCreation` means the sled was never found, so there is no cubby.
//
// The database enforces the same pairing. The
// `power_off_outcome_matches_sp_slot` CHECK requires `sp_slot` to be NULL
// exactly when the outcome is `absent_at_creation`, and
// `sp_slot_source_iff_sp_slot` keeps `sp_slot_source` in step with `sp_slot`.
// The row decode in db-model returns an error for a row that breaks either one,
// so a decoded request always has a valid pairing.
//
// The advance to `expunging` is also a compare-and-swap on `sp_slot` and
// `sp_slot_source`, not only on the state. This matters because an outcome is
// decided for one particular cubby. For example, suppose the task decides
// `PoweredOff` for cubby 3, and another Nexus records cubby 7 for the request
// before the advance is written. Without the compare-and-swap, the row would
// say that cubby 7 was powered off. With it, the advance matches no row and
// returns `NotInExpectedState`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungePowerOff {
    Cubby {
        cubby: SledExpungeRecordedCubby,
        outcome: SledExpungeCubbyPowerOffOutcome,
    },
    AbsentAtCreation,
}

impl SledExpungePowerOff {
    pub fn cubby(&self) -> Option<SledExpungeRecordedCubby> {
        match self {
            Self::Cubby { cubby, outcome: _ } => Some(*cubby),
            Self::AbsentAtCreation => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Cubby { cubby: _, outcome } => outcome.label(),
            Self::AbsentAtCreation => "absent_at_creation",
        }
    }
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, strum::VariantArray, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum SledExpungeCubbyPowerOffOutcome {
    PoweredOff,
    AlreadyOff,
    OtherBoardPresent,
    NotPresent,
}

impl SledExpungeCubbyPowerOffOutcome {
    pub fn label(&self) -> &'static str {
        self.into()
    }
}

// TODO-RAINCLAUDE: How far the power-off step got, for a request that failed.
//
// A request can fail before the power-off step settles. For example, its
// removal can be aborted while it is still `waiting_for_trust_quorum`. That
// request has no outcome, but it still has whatever cubby was recorded for it
// by then (possibly none). `Pending` carries that cubby, so that it isn't lost
// when the row is decoded.
//
// A request that fails while it is `expunging` already has an outcome, and
// `Settled` carries it. The outcome includes its own cubby.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungePowerOffProgress {
    Pending { cubby: Option<SledExpungeRecordedCubby> },
    Settled(SledExpungePowerOff),
}

impl SledExpungePowerOffProgress {
    pub fn cubby(&self) -> Option<SledExpungeRecordedCubby> {
        match self {
            Self::Pending { cubby } => *cubby,
            Self::Settled(power_off) => power_off.cubby(),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, strum::VariantArray)]
pub enum SledExpungeRequestUnfinishedState {
    WaitingForTrustQuorum,
    PoweringOff,
    Expunging,
}

impl SledExpungeRequestUnfinishedState {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;

    pub fn kind(&self) -> SledExpungeRequestStateKind {
        match self {
            Self::WaitingForTrustQuorum => {
                SledExpungeRequestStateKind::WaitingForTrustQuorum
            }
            Self::PoweringOff => SledExpungeRequestStateKind::PoweringOff,
            Self::Expunging => SledExpungeRequestStateKind::Expunging,
        }
    }
}

impl fmt::Display for SledExpungeRequestUnfinishedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

// TODO-RAINCLAUDE: Where request creation found the sled, which decides the new
// request's `sp_slot` and `sp_slot_source`.
//
// * `Present` records the cubby the sled's SP was found in, and whether
//   inventory or the live scan found it.
// * `Gone` means the sled was found in no cubby, so no cubby is recorded.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeCreationPresence {
    Present { sp_slot: u16, source: SledExpungeCreationSpSlotSource },
    Gone,
}

impl SledExpungeCreationPresence {
    pub fn recorded_cubby(&self) -> Option<SledExpungeRecordedCubby> {
        match self {
            Self::Present { sp_slot, source } => {
                Some(SledExpungeRecordedCubby {
                    slot: *sp_slot,
                    source: (*source).into(),
                })
            }
            Self::Gone => None,
        }
    }
}

// TODO-RAINCLAUDE: Creation only looks in inventory and then does the live
// scan, so these are the only two sources a new request's cubby can have.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeCreationSpSlotSource {
    Inventory,
    LiveScan,
}

impl From<SledExpungeCreationSpSlotSource> for SledExpungeSpSlotSource {
    fn from(source: SledExpungeCreationSpSlotSource) -> Self {
        match source {
            SledExpungeCreationSpSlotSource::Inventory => Self::Inventory,
            SledExpungeCreationSpSlotSource::LiveScan => Self::LiveScan,
        }
    }
}

// TODO-RAINCLAUDE: A cubby that the powering-off step found, together with the
// cubby the row recorded before it. The previous slot is what
// `sled_expunge_request_record_sp_slot` compares against, and it also decides
// the source. The constructors refuse a relocation to the same cubby, because
// that would change the source without changing the slot.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SledExpungeSpSlotChange {
    previous_sp_slot: Option<u16>,
    sp_slot: u16,
}

impl SledExpungeSpSlotChange {
    pub fn found_after_absent(found: u16) -> Self {
        Self { previous_sp_slot: None, sp_slot: found }
    }

    pub fn relocated(previous: u16, found: u16) -> Option<Self> {
        if previous == found {
            None
        } else {
            Some(Self { previous_sp_slot: Some(previous), sp_slot: found })
        }
    }

    pub fn previous_sp_slot(&self) -> Option<u16> {
        self.previous_sp_slot
    }

    pub fn sp_slot(&self) -> u16 {
        self.sp_slot
    }

    pub fn source(&self) -> SledExpungeSpSlotSource {
        match self.previous_sp_slot {
            None => SledExpungeSpSlotSource::FoundAfterAbsent,
            Some(_) => SledExpungeSpSlotSource::Relocated,
        }
    }

    pub fn recorded_cubby(&self) -> SledExpungeRecordedCubby {
        SledExpungeRecordedCubby { slot: self.sp_slot, source: self.source() }
    }
}

// TODO-RAINCLAUDE: What the caller of `sled_expunge_request_create` learned
// about where the sled is.
//
// `NotProbed` means the caller's own policy and state check refused the sled,
// so it skipped the probe. The transaction is expected to refuse the sled for
// the same reason, and it returns an internal error if its own policy and state
// check passes instead.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeCreationProbe {
    Probed(SledExpungeCreationPresence),
    NotProbed,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeRequestAdvance {
    ToPoweringOff,
    ToExpunging { power_off: SledExpungePowerOff },
}

impl SledExpungeRequestAdvance {
    pub fn from_state(&self) -> SledExpungeRequestUnfinishedState {
        match self {
            Self::ToPoweringOff => {
                SledExpungeRequestUnfinishedState::WaitingForTrustQuorum
            }
            Self::ToExpunging { .. } => {
                SledExpungeRequestUnfinishedState::PoweringOff
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeRequestTransition {
    Advance(SledExpungeRequestAdvance),
    Done,
    Failed { from: SledExpungeRequestUnfinishedState, failure_reason: String },
}

impl SledExpungeRequestTransition {
    pub fn from_state(&self) -> SledExpungeRequestUnfinishedState {
        match self {
            Self::Advance(advance) => advance.from_state(),
            Self::Done => SledExpungeRequestUnfinishedState::Expunging,
            Self::Failed { from, .. } => *from,
        }
    }

    pub fn cubby_precondition(&self) -> SledExpungeCubbyPrecondition {
        match self {
            Self::Advance(SledExpungeRequestAdvance::ToExpunging {
                power_off,
            }) => SledExpungeCubbyPrecondition::Equals(power_off.cubby()),
            Self::Advance(SledExpungeRequestAdvance::ToPoweringOff)
            | Self::Done
            | Self::Failed { .. } => SledExpungeCubbyPrecondition::Any,
        }
    }
}

// TODO-RAINCLAUDE: The recorded cubby (`sp_slot` and `sp_slot_source`) a
// transition requires the row to have, in addition to its `from` state.
// `Equals(None)` requires both columns to be NULL.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeCubbyPrecondition {
    Any,
    Equals(Option<SledExpungeRecordedCubby>),
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
#[serde(try_from = "BaseboardId", into = "BaseboardId")]
pub struct NonEmptyBaseboardId(BaseboardId);

// TODO-RAINCLAUDE: A `BaseboardId` with an empty part or serial number, which
// doesn't identify a board.
//
// This is the error returned by `NonEmptyBaseboardId::new`. It can only be
// built by that function or by deserializing, which runs the same check, so
// holding one proves that the `BaseboardId` is empty.
#[derive(
    Clone, Debug, PartialEq, Eq, thiserror::Error, Deserialize, Serialize,
)]
#[serde(try_from = "BaseboardId", into = "BaseboardId")]
#[error("baseboard {0:?} has an empty part or serial number")]
pub struct EmptyBaseboardIdError(BaseboardId);

impl EmptyBaseboardIdError {
    pub fn baseboard_id(&self) -> &BaseboardId {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("baseboard {0} has a non-empty part and serial number")]
pub struct NotEmptyBaseboardIdError(NonEmptyBaseboardId);

impl TryFrom<BaseboardId> for EmptyBaseboardIdError {
    type Error = NotEmptyBaseboardIdError;

    fn try_from(baseboard_id: BaseboardId) -> Result<Self, Self::Error> {
        match NonEmptyBaseboardId::new(baseboard_id) {
            Ok(non_empty) => Err(NotEmptyBaseboardIdError(non_empty)),
            Err(empty) => Ok(empty),
        }
    }
}

impl From<EmptyBaseboardIdError> for BaseboardId {
    fn from(error: EmptyBaseboardIdError) -> Self {
        error.0
    }
}

impl NonEmptyBaseboardId {
    pub fn new(
        baseboard_id: BaseboardId,
    ) -> Result<Self, EmptyBaseboardIdError> {
        if is_empty_baseboard_id(&baseboard_id) {
            Err(EmptyBaseboardIdError(baseboard_id))
        } else {
            Ok(Self(baseboard_id))
        }
    }

    pub fn as_baseboard_id(&self) -> &BaseboardId {
        &self.0
    }
}

impl TryFrom<BaseboardId> for NonEmptyBaseboardId {
    type Error = EmptyBaseboardIdError;

    fn try_from(baseboard_id: BaseboardId) -> Result<Self, Self::Error> {
        Self::new(baseboard_id)
    }
}

impl From<NonEmptyBaseboardId> for BaseboardId {
    fn from(baseboard_id: NonEmptyBaseboardId) -> Self {
        baseboard_id.0
    }
}

impl fmt::Display for NonEmptyBaseboardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SledCubbyIgnition {
    pub slot: u16,
    pub ignition: SpIgnition,
}

impl IdOrdItem for SledCubbyIgnition {
    type Key<'a> = u16;

    fn key(&self) -> Self::Key<'_> {
        self.slot
    }

    id_upcast!();
}

// TODO-RAINCLAUDE: The sled cubbies whose board must be identified before a
// sled can be ruled out of the rack, computed from both switches' ignition
// lists.
//
// There are two sets:
// * `sp_get` holds the cubbies to identify with an `sp_get`. A cubby is
//   included if either switch shows it powered, because each switch reaches the
//   cubby over its own ignition link. A cubby that only one switch lists is
//   also included, because the other switch can't vouch for it. An unknown
//   system type is treated as a sled, since it may be a newer kind of sled.
// * `unidentifiable` holds the cubbies where a switch reports a sidecar or PSC
//   system type. A sled cubby should never report one, so the ignition list
//   can't be trusted for that cubby, and it counts as unidentified without an
//   `sp_get`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SledExpungeCubbiesToIdentify {
    pub sp_get: BTreeSet<u16>,
    pub unidentifiable: IdOrdMap<UnidentifiedCubby>,
}

pub fn sled_expunge_cubbies_to_identify(
    switch0: &IdOrdMap<SledCubbyIgnition>,
    switch1: &IdOrdMap<SledCubbyIgnition>,
) -> SledExpungeCubbiesToIdentify {
    let slots: BTreeSet<u16> =
        switch0.iter().chain(switch1.iter()).map(|cubby| cubby.slot).collect();
    let mut sp_get = BTreeSet::new();
    let mut unidentifiable = IdOrdMap::new();
    for slot in slots {
        let views = [switch0.get(&slot), switch1.get(&slot)];
        let unexpected = views
            .iter()
            .flatten()
            .find_map(|cubby| non_sled_system_type(&cubby.ignition));
        if let Some(system_type) = unexpected {
            unidentifiable
                .insert_unique(UnidentifiedCubby {
                    slot,
                    reason: CubbyUnidentified::UnexpectedSystemType {
                        system_type,
                    },
                })
                .expect("slots is a set, so each cubby is visited once");
            continue;
        }
        let needed = match views {
            [Some(cubby0), Some(cubby1)] => {
                ignition_powered(&cubby0.ignition)
                    || ignition_powered(&cubby1.ignition)
            }
            [Some(_), None] | [None, Some(_)] => true,
            [None, None] => false,
        };
        if needed {
            sp_get.insert(slot);
        }
    }
    SledExpungeCubbiesToIdentify { sp_get, unidentifiable }
}

fn ignition_powered(ignition: &SpIgnition) -> bool {
    match ignition {
        SpIgnition::Absent | SpIgnition::Present { power: false, .. } => false,
        SpIgnition::Present { power: true, .. } => true,
    }
}

fn non_sled_system_type(ignition: &SpIgnition) -> Option<SpIgnitionSystemType> {
    match ignition {
        SpIgnition::Absent => None,
        SpIgnition::Present { id, .. } => match id {
            SpIgnitionSystemType::Gimlet
            | SpIgnitionSystemType::Cosmo
            | SpIgnitionSystemType::Unknown { .. } => None,
            SpIgnitionSystemType::Sidecar | SpIgnitionSystemType::Psc => {
                Some(*id)
            }
        },
    }
}

// TODO-RAINCLAUDE: Why the board in a sled cubby could not be identified.
//
// Some notes:
// * `SpUnreachable` means MGS said it couldn't reach the SP. `MgsRequestFailed`
//   covers every other failure, such as MGS itself being unreachable, so it
//   says nothing about the SP.
// * `UnexpectedSystemType` comes only from the ignition lists (see
//   `sled_expunge_cubbies_to_identify`). Powering off never produces it.
// * The `error` strings are for logs and omdb only.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CubbyUnidentified {
    SpUnreachable { error: String },
    MgsRequestFailed { error: String },
    EmptyIdentity { reported: EmptyBaseboardIdError },
    UnexpectedSystemType { system_type: SpIgnitionSystemType },
}

impl fmt::Display for CubbyUnidentified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpUnreachable { error } => {
                write!(f, "SP unreachable: {error}")
            }
            Self::MgsRequestFailed { error } => {
                write!(f, "MGS request failed: {error}")
            }
            Self::EmptyIdentity { reported } => write!(
                f,
                "SP reported an empty board identity ({})",
                reported.baseboard_id(),
            ),
            Self::UnexpectedSystemType { system_type } => write!(
                f,
                "ignition reports system type {system_type:?}; not identified"
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SledCubbyIdentity {
    Known(NonEmptyBaseboardId),
    Unidentified(CubbyUnidentified),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SledCubbyIdentityEntry {
    pub slot: u16,
    pub identity: SledCubbyIdentity,
}

impl IdOrdItem for SledCubbyIdentityEntry {
    type Key<'a> = u16;

    fn key(&self) -> Self::Key<'_> {
        self.slot
    }

    id_upcast!();
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct SledCubbyBoard {
    pub slot: u16,
    pub baseboard_id: NonEmptyBaseboardId,
}

impl IdOrdItem for SledCubbyBoard {
    type Key<'a> = u16;

    fn key(&self) -> Self::Key<'_> {
        self.slot
    }

    id_upcast!();
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct UnidentifiedCubby {
    pub slot: u16,
    pub reason: CubbyUnidentified,
}

impl IdOrdItem for UnidentifiedCubby {
    type Key<'a> = u16;

    fn key(&self) -> Self::Key<'_> {
        self.slot
    }

    id_upcast!();
}

// TODO-RAINCLAUDE: A set of sled cubbies whose board could not be identified.
//
// The set is never empty, so a sled that couldn't be confirmed always comes
// with at least one cubby that could hold it. Both `new` and deserializing
// enforce this.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    try_from = "IdOrdMap<UnidentifiedCubby>",
    into = "IdOrdMap<UnidentifiedCubby>"
)]
pub struct SledExpungeUnidentifiedCubbies(IdOrdMap<UnidentifiedCubby>);

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a list of unidentified sled cubbies must not be empty")]
pub struct NoUnidentifiedCubbiesError;

impl SledExpungeUnidentifiedCubbies {
    pub fn new(
        cubbies: IdOrdMap<UnidentifiedCubby>,
    ) -> Result<Self, NoUnidentifiedCubbiesError> {
        if cubbies.is_empty() {
            Err(NoUnidentifiedCubbiesError)
        } else {
            Ok(Self(cubbies))
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &UnidentifiedCubby> {
        self.0.iter()
    }
}

impl TryFrom<IdOrdMap<UnidentifiedCubby>> for SledExpungeUnidentifiedCubbies {
    type Error = NoUnidentifiedCubbiesError;

    fn try_from(
        cubbies: IdOrdMap<UnidentifiedCubby>,
    ) -> Result<Self, Self::Error> {
        Self::new(cubbies)
    }
}

impl From<SledExpungeUnidentifiedCubbies> for IdOrdMap<UnidentifiedCubby> {
    fn from(cubbies: SledExpungeUnidentifiedCubbies) -> Self {
        cubbies.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeLivePresence {
    InCubby { slot: u16 },
    Gone { absence: SledExpungeRackAbsence },
    Unidentified { cubbies: SledExpungeUnidentifiedCubbies },
}

// TODO-RAINCLAUDE: Decides where a sled is in the rack, given what was found in
// each sled cubby. Request creation and the power-off step both use this rule.
//
// The rule is:
// * If a cubby holds our board, the sled is in that cubby. This is true even if
//   other cubbies are unidentified, because a board can only be in one cubby.
// * Otherwise, if any cubby is unidentified, the sled could be in it, so the
//   result is `Unidentified`.
// * The sled is `Gone` only if every cubby was identified as holding some other
//   board.
pub fn sled_expunge_live_presence(
    baseboard_id: &BaseboardId,
    switch0_mgs: SocketAddrV6,
    switch1_mgs: SocketAddrV6,
    identities: &IdOrdMap<SledCubbyIdentityEntry>,
) -> SledExpungeLivePresence {
    let mut other_boards = IdOrdMap::new();
    let mut unidentified = IdOrdMap::new();
    for entry in identities {
        match &entry.identity {
            SledCubbyIdentity::Known(found) => {
                if found.as_baseboard_id() == baseboard_id {
                    return SledExpungeLivePresence::InCubby {
                        slot: entry.slot,
                    };
                }
                other_boards
                    .insert_unique(SledCubbyBoard {
                        slot: entry.slot,
                        baseboard_id: found.clone(),
                    })
                    .expect("identities holds one entry per slot");
            }
            SledCubbyIdentity::Unidentified(reason) => {
                unidentified
                    .insert_unique(UnidentifiedCubby {
                        slot: entry.slot,
                        reason: reason.clone(),
                    })
                    .expect("identities holds one entry per slot");
            }
        }
    }
    match SledExpungeUnidentifiedCubbies::new(unidentified) {
        Ok(cubbies) => SledExpungeLivePresence::Unidentified { cubbies },
        Err(NoUnidentifiedCubbiesError) => SledExpungeLivePresence::Gone {
            absence: SledExpungeRackAbsence {
                switch0_mgs,
                switch1_mgs,
                other_boards,
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO-RAINCLAUDE: The expected status code and message of each refusal are
    // written out as literals here, rather than being built by the code under
    // test. This means that a change to a refusal's status or wording fails
    // this test.
    #[test]
    fn test_sled_expunge_refusal_status_codes() {
        use omicron_uuid_kinds::RackUuid;
        use trust_quorum_types::membership::TRUST_QUORUM_MAX_MEMBERS;
        use trust_quorum_types::membership::TrustQuorumMembers;

        let status = |error: &Error| {
            dropshot::HttpError::from(error.clone()).status_code.as_u16()
        };
        let sled_id = SledUuid::new_v4();

        let serial = check_sled_expunge_serial_acknowledged(
            sled_id, "serial-a", "serial-b",
        )
        .expect_err("a different serial is refused");
        assert_eq!(status(&serial), 400);
        assert_eq!(
            serial,
            Error::invalid_request(format!(
                "Cannot expunge sled {sled_id}: the serial number given to \
                 confirm the request (\"serial-b\") does not match the \
                 sled's serial number. Check that this is the sled you mean \
                 to expunge (its serial number is in the sled's baseboard \
                 details, e.g. oxide system hardware sled view), then retry \
                 with that serial number."
            )),
        );

        let rack_id = RackUuid::new_v4();
        let members: std::collections::BTreeSet<BaseboardId> = (0
            ..TRUST_QUORUM_MIN_MEMBERS)
            .map(|i| BaseboardId {
                part_number: "913-0000019".to_string(),
                serial_number: format!("member-{i}"),
            })
            .collect();
        let coordinator = members.first().expect("at least one member").clone();
        let config = TrustQuorumConfig::new_rss_committed_config(
            rack_id,
            TrustQuorumMembers::new(members)
                .expect("the minimum number of members is a valid size"),
            coordinator.clone(),
        );
        let sled = BaseboardId {
            part_number: "913-0000019".to_string(),
            serial_number: "not-a-member".to_string(),
        };

        let not_member = sled_expunge_removal_refusal(
            sled_id,
            &config,
            RemoveSledError::NotMember { sled: sled.clone() },
        );
        assert_eq!(status(&not_member), 400);
        assert_eq!(
            not_member,
            Error::invalid_request(format!(
                "Cannot expunge sled {sled_id}: its baseboard {sled} is not a \
                 member of the last committed trust quorum configuration for \
                 rack {rack_id} (epoch {}, committed). This API only expunges \
                 sleds that are trust quorum members. If this sled has \
                 already left the trust quorum, contact Oxide support to \
                 expunge it.",
                config.epoch,
            )),
        );

        let too_few = sled_expunge_removal_refusal(
            sled_id,
            &config,
            RemoveSledError::InvalidMembership {
                rack_id,
                sled: coordinator.clone(),
                error: TrustQuorumMembersError::TooFew { num_members: 2 },
            },
        );
        assert_eq!(status(&too_few), 409);
        assert_eq!(
            too_few,
            Error::conflict(format!(
                "Cannot expunge sled {sled_id}: the trust quorum for rack \
                 {rack_id} has 3 sleds and must keep at least \
                 {TRUST_QUORUM_MIN_MEMBERS}, so this sled can't be removed \
                 from it. Contact Oxide support."
            )),
        );

        let too_many = sled_expunge_removal_refusal(
            sled_id,
            &config,
            RemoveSledError::InvalidMembership {
                rack_id,
                sled: coordinator,
                error: TrustQuorumMembersError::TooMany {
                    num_members: TRUST_QUORUM_MAX_MEMBERS + 1,
                },
            },
        );
        assert_eq!(status(&too_many), 500);
        assert_eq!(
            too_many,
            Error::internal_error(&format!(
                "Cannot expunge sled {sled_id}: removing it from the trust \
                 quorum for rack {rack_id} would leave too many members; the \
                 latest committed configuration is larger than any write \
                 path allows: Invalid trust quorum membership size ({}): \
                 must be between {TRUST_QUORUM_MIN_MEMBERS} and \
                 {TRUST_QUORUM_MAX_MEMBERS} sleds.",
                TRUST_QUORUM_MAX_MEMBERS + 1,
            )),
        );
    }

    #[test]
    fn test_check_sled_expunge_policy_and_state() {
        let sled_id = SledUuid::new_v4();
        let policy = SledPolicy::InService {
            provision_policy: SledProvisionPolicy::NonProvisionable,
        };
        let refusal = check_sled_expunge_policy_and_state(
            sled_id,
            policy,
            SledState::Decommissioned,
        )
        .expect_err("a decommissioned sled is refused");
        assert_eq!(
            refusal.into_error(None),
            Error::conflict(format!(
                "Cannot expunge sled {sled_id}: its policy is not provisionable \
                 and its state is decommissioned, but only a sled that is in \
                 service and active can be expunged."
            )),
        );
        let refusal = check_sled_expunge_policy_and_state(
            sled_id,
            SledPolicy::Expunged,
            SledState::Decommissioned,
        )
        .expect_err("an expunged sled is refused");
        assert_eq!(
            refusal.into_error(None),
            Error::conflict(format!(
                "Cannot expunge sled {sled_id}: it is already expunged (its \
                 state is decommissioned)."
            )),
        );
    }

    #[test]
    fn test_sled_not_expungeable_names_finished_request() {
        let sled_id = SledUuid::new_v4();
        let refusal = check_sled_expunge_policy_and_state(
            sled_id,
            SledPolicy::Expunged,
            SledState::Active,
        )
        .expect_err("an expunged sled is refused");

        for (state, state_text) in [
            (SledExpungeRequestFinishedState::Done, "done"),
            (SledExpungeRequestFinishedState::Failed, "failed"),
        ] {
            let id = SledExpungeRequestUuid::new_v4();
            assert_eq!(
                refusal
                    .into_error(Some(FinishedSledExpungeRequest { id, state })),
                Error::conflict(format!(
                    "Cannot expunge sled {sled_id}: it is already expunged \
                     (its state is active). The sled's most recent expunge \
                     request, {id}, is {state_text}; use \
                     sled_expunge_request_view to see its details."
                )),
                "{state:?}",
            );
        }
    }

    #[test]
    fn test_check_sled_expunge_baseboard_recorded() {
        let sled_id = SledUuid::new_v4();
        let baseboard = |part: &str, serial: &str| BaseboardId {
            part_number: part.to_string(),
            serial_number: serial.to_string(),
        };
        assert_eq!(
            check_sled_expunge_baseboard_recorded(
                sled_id,
                &baseboard("913-0000019", "BRM42220001"),
            ),
            Ok(()),
        );
        for (part, serial) in
            [("", "BRM42220001"), ("913-0000019", ""), ("", "")]
        {
            assert_eq!(
                check_sled_expunge_baseboard_recorded(
                    sled_id,
                    &baseboard(part, serial),
                ),
                Err(Error::conflict(format!(
                    "Cannot expunge sled {sled_id}: it has no recorded part \
                     or serial number (part number {part:?}, serial number \
                     {serial:?}), so it can't be expunged through this API. \
                     Contact Oxide support."
                ))),
                "part {part:?}, serial {serial:?}",
            );
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum ScrimletExpected {
        Allowed,
        ByRole,
        BySlot(u16),
        ByMgsSlot(u16),
        BySledRow,
        BySledRowNonSledSp(NonSledSpType),
        BySledRowNoCollection,
    }

    #[test]
    fn test_check_sled_expunge_not_scrimlet() {
        use ScrimletExpected::*;
        use SledRole::{Gimlet, Scrimlet};

        let sled_id = SledUuid::new_v4();
        let refused = |why: &str| {
            Err(Error::conflict(format!(
                "Cannot expunge sled {sled_id}: it is a scrimlet (it manages a \
                 switch), since {why}. Expunging a scrimlet through this API \
                 is not supported. Contact Oxide support."
            )))
        };
        let no_collection = SledExpungeScrimletEvidence {
            inventory: ScrimletEvidence::NoCollection,
            mgs_sp_slot: None,
        };
        let collection = |sled_agent_role, sled_sp_slot: Option<u16>| {
            let sp_record = match sled_sp_slot {
                Some(slot) => SpRecord::Slot(slot),
                None => SpRecord::NoRecord,
            };
            SledExpungeScrimletEvidence {
                inventory: ScrimletEvidence::Collection {
                    sled_agent_role,
                    sp_record,
                },
                mgs_sp_slot: None,
            }
        };
        let non_sled_sp = |sp_type| SledExpungeScrimletEvidence {
            inventory: ScrimletEvidence::Collection {
                sled_agent_role: None,
                sp_record: SpRecord::NotSledType(sp_type),
            },
            mgs_sp_slot: None,
        };
        let cases = [
            (no_collection, Gimlet, Allowed),
            (no_collection, Scrimlet, BySledRowNoCollection),
            (collection(None, None), Scrimlet, BySledRow),
            (collection(None, Some(14)), Gimlet, BySlot(14)),
            (collection(Some(Gimlet), Some(16)), Gimlet, BySlot(16)),
            (collection(Some(Scrimlet), Some(3)), Gimlet, ByRole),
            (collection(Some(Gimlet), None), Scrimlet, Allowed),
            (collection(None, Some(3)), Scrimlet, Allowed),
            (
                collection(None, None).with_mgs_sp_slot(14),
                Gimlet,
                ByMgsSlot(14),
            ),
            (no_collection.with_mgs_sp_slot(16), Gimlet, ByMgsSlot(16)),
            (collection(None, None).with_mgs_sp_slot(3), Scrimlet, Allowed),
            (
                collection(None, Some(3)).with_mgs_sp_slot(14),
                Gimlet,
                ByMgsSlot(14),
            ),
            (
                collection(Some(Gimlet), Some(3)).with_mgs_sp_slot(16),
                Gimlet,
                ByMgsSlot(16),
            ),
            (
                non_sled_sp(NonSledSpType::Switch),
                Scrimlet,
                BySledRowNonSledSp(NonSledSpType::Switch),
            ),
            (non_sled_sp(NonSledSpType::Power), Gimlet, Allowed),
            (
                non_sled_sp(NonSledSpType::Power).with_mgs_sp_slot(14),
                Gimlet,
                ByMgsSlot(14),
            ),
        ];

        for (evidence, sled_row_role, expected) in cases {
            let expected = match expected {
                Allowed => Ok(()),
                ByRole => refused(
                    "the latest inventory collection reports its role as \
                     scrimlet",
                ),
                BySlot(slot) => refused(&format!(
                    "the latest inventory collection found its service \
                     processor in switch-attached cubby {slot}"
                )),
                ByMgsSlot(slot) => refused(&format!(
                    "MGS found its service processor in switch-attached \
                     cubby {slot}"
                )),
                BySledRow => refused(
                    "it is recorded as a scrimlet and the latest inventory \
                     collection has neither a sled agent record nor a \
                     service processor record for it",
                ),
                BySledRowNonSledSp(sp_type) => refused(&format!(
                    "it is recorded as a scrimlet and the latest inventory \
                     collection has no sled agent record for it, and records \
                     the service processor with its baseboard as a {sp_type} \
                     service processor, not a sled one"
                )),
                BySledRowNoCollection => refused(
                    "it is recorded as a scrimlet and there is no inventory \
                     collection",
                ),
            };
            assert_eq!(
                check_sled_expunge_not_scrimlet(
                    sled_id,
                    &evidence,
                    sled_row_role,
                ),
                expected,
                "{evidence:?}, sled row {sled_row_role:?}",
            );
        }
    }

    #[test]
    fn test_unfinished_states_consistent() {
        for &kind in SledExpungeRequestStateKind::ALL {
            assert_eq!(
                SledExpungeRequestStateKind::UNFINISHED.contains(&kind),
                !kind.is_finished(),
                "SledExpungeRequestStateKind::{kind:?}: UNFINISHED and \
                 is_finished() disagree",
            );
        }
    }

    #[test]
    fn test_unfinished_state_enum_matches_unfinished() {
        let converted: Vec<SledExpungeRequestStateKind> =
            SledExpungeRequestUnfinishedState::ALL
                .iter()
                .map(|state| state.kind())
                .collect();
        assert_eq!(converted, SledExpungeRequestStateKind::UNFINISHED);
    }

    fn ignition(id: SpIgnitionSystemType, power: bool) -> SpIgnition {
        SpIgnition::Present {
            id,
            power,
            ctrl_detect_0: true,
            ctrl_detect_1: true,
            flt_a3: false,
            flt_a2: false,
            flt_rot: false,
            flt_sp: false,
        }
    }

    #[test]
    fn test_sled_expunge_cubbies_to_identify() {
        let view = |cubbies: Vec<(u16, SpIgnition)>| -> IdOrdMap<_> {
            cubbies
                .into_iter()
                .map(|(slot, ignition)| SledCubbyIgnition { slot, ignition })
                .collect()
        };
        let unexpected = |slots: Vec<(u16, SpIgnitionSystemType)>| {
            slots
                .into_iter()
                .map(|(slot, system_type)| UnidentifiedCubby {
                    slot,
                    reason: CubbyUnidentified::UnexpectedSystemType {
                        system_type,
                    },
                })
                .collect::<IdOrdMap<_>>()
        };
        let switch0 = view(vec![
            (0, SpIgnition::Absent),
            (1, ignition(SpIgnitionSystemType::Gimlet, false)),
            (2, ignition(SpIgnitionSystemType::Gimlet, true)),
            (3, ignition(SpIgnitionSystemType::Cosmo, true)),
            (4, ignition(SpIgnitionSystemType::Unknown { id: 9 }, true)),
            (5, ignition(SpIgnitionSystemType::Sidecar, true)),
            (6, ignition(SpIgnitionSystemType::Psc, false)),
            (7, SpIgnition::Absent),
        ]);
        let switch0_unpowered = view(
            (0..8)
                .map(|slot| {
                    (slot, ignition(SpIgnitionSystemType::Gimlet, false))
                })
                .collect(),
        );
        let expected = SledExpungeCubbiesToIdentify {
            sp_get: BTreeSet::from([2, 3, 4]),
            unidentifiable: unexpected(vec![
                (5, SpIgnitionSystemType::Sidecar),
                (6, SpIgnitionSystemType::Psc),
            ]),
        };
        assert_eq!(
            sled_expunge_cubbies_to_identify(&switch0, &switch0_unpowered),
            expected,
        );
        assert_eq!(
            sled_expunge_cubbies_to_identify(&switch0_unpowered, &switch0),
            expected,
        );
        let empty = IdOrdMap::new();
        assert_eq!(
            sled_expunge_cubbies_to_identify(&empty, &empty),
            SledExpungeCubbiesToIdentify {
                sp_get: BTreeSet::new(),
                unidentifiable: IdOrdMap::new(),
            },
        );

        assert_eq!(
            sled_expunge_cubbies_to_identify(&switch0, &empty),
            SledExpungeCubbiesToIdentify {
                sp_get: BTreeSet::from([0, 1, 2, 3, 4, 7]),
                unidentifiable: expected.unidentifiable.clone(),
            },
        );
        let switch1 = view(vec![
            (0, ignition(SpIgnitionSystemType::Gimlet, true)),
            (1, ignition(SpIgnitionSystemType::Gimlet, true)),
            (2, SpIgnition::Absent),
            (3, SpIgnition::Absent),
            (4, SpIgnition::Absent),
            (5, SpIgnition::Absent),
            (6, SpIgnition::Absent),
            (7, SpIgnition::Absent),
            (8, SpIgnition::Absent),
            (9, ignition(SpIgnitionSystemType::Gimlet, false)),
        ]);
        assert_eq!(
            sled_expunge_cubbies_to_identify(&switch0, &switch1),
            SledExpungeCubbiesToIdentify {
                sp_get: BTreeSet::from([0, 1, 2, 3, 4, 8, 9]),
                unidentifiable: expected.unidentifiable.clone(),
            },
        );
    }

    #[test]
    fn test_sled_expunge_live_presence() {
        let board = |serial: &str| BaseboardId {
            part_number: "913-0000019".to_string(),
            serial_number: serial.to_string(),
        };
        let ours = board("ours");
        let switch0_mgs: SocketAddrV6 =
            "[fd00:1122:3344:101::2]:12225".parse().expect("parsed an address");
        let switch1_mgs: SocketAddrV6 =
            "[fd00:1122:3344:102::2]:12225".parse().expect("parsed an address");
        let known = |serial: &str| {
            SledCubbyIdentity::Known(
                NonEmptyBaseboardId::new(board(serial))
                    .expect("test boards are non-empty"),
            )
        };
        let unreachable =
            CubbyUnidentified::SpUnreachable { error: "no SP".to_string() };
        let mgs_failed = CubbyUnidentified::MgsRequestFailed {
            error: "connection refused".to_string(),
        };
        let empty = CubbyUnidentified::EmptyIdentity {
            reported: NonEmptyBaseboardId::new(board(""))
                .expect_err("an empty serial number is empty"),
        };
        let entries = |identities: Vec<(u16, SledCubbyIdentity)>| {
            identities
                .into_iter()
                .map(|(slot, identity)| SledCubbyIdentityEntry {
                    slot,
                    identity,
                })
                .collect::<IdOrdMap<_>>()
        };
        let gone = |slots: Vec<(u16, &str)>| SledExpungeLivePresence::Gone {
            absence: SledExpungeRackAbsence {
                switch0_mgs,
                switch1_mgs,
                other_boards: slots
                    .into_iter()
                    .map(|(slot, serial)| SledCubbyBoard {
                        slot,
                        baseboard_id: NonEmptyBaseboardId::new(board(serial))
                            .expect("test boards are non-empty"),
                    })
                    .collect(),
            },
        };
        let unidentified = |cubbies: Vec<(u16, CubbyUnidentified)>| {
            SledExpungeLivePresence::Unidentified {
                cubbies: SledExpungeUnidentifiedCubbies::new(
                    cubbies
                        .into_iter()
                        .map(|(slot, reason)| UnidentifiedCubby {
                            slot,
                            reason,
                        })
                        .collect(),
                )
                .expect("expected unidentified cubbies are non-empty"),
            }
        };
        let unidentified_entry = |reason: &CubbyUnidentified| {
            SledCubbyIdentity::Unidentified(reason.clone())
        };

        let cases = [
            (vec![], gone(vec![])),
            (
                vec![(2, known("other")), (3, known("another"))],
                gone(vec![(2, "other"), (3, "another")]),
            ),
            (
                vec![(2, unidentified_entry(&unreachable)), (5, known("ours"))],
                SledExpungeLivePresence::InCubby { slot: 5 },
            ),
            (
                vec![
                    (2, known("other")),
                    (3, unidentified_entry(&unreachable)),
                    (4, unidentified_entry(&mgs_failed)),
                    (6, unidentified_entry(&empty)),
                ],
                unidentified(vec![
                    (3, unreachable.clone()),
                    (4, mgs_failed.clone()),
                    (6, empty.clone()),
                ]),
            ),
        ];
        for (identities, expected) in cases {
            let identities = entries(identities);
            assert_eq!(
                sled_expunge_live_presence(
                    &ours,
                    switch0_mgs,
                    switch1_mgs,
                    &identities,
                ),
                expected,
                "{identities:?}",
            );
        }
    }

    #[test]
    fn test_sled_expunge_non_empty_deserialize() {
        let empty = BaseboardId {
            part_number: "913-0000019".to_string(),
            serial_number: String::new(),
        };
        let reported = NonEmptyBaseboardId::new(empty.clone())
            .expect_err("an empty serial number is empty");
        assert_eq!(reported.baseboard_id(), &empty);
        let error = serde_json::from_str::<NonEmptyBaseboardId>(
            r#"{"part_number":"913-0000019","serial_number":""}"#,
        )
        .expect_err("an empty serial number does not deserialize");
        assert!(
            error.to_string().starts_with(
                "baseboard BaseboardId { part_number: \"913-0000019\", \
                 serial_number: \"\" } has an empty part or serial number"
            ),
            "{error}",
        );
        assert_eq!(
            serde_json::from_str::<EmptyBaseboardIdError>(
                r#"{"part_number":"913-0000019","serial_number":""}"#,
            )
            .expect("an empty serial number deserializes as empty"),
            reported,
        );
        let error = serde_json::from_str::<EmptyBaseboardIdError>(
            r#"{"part_number":"913-0000019","serial_number":"BRM42220001"}"#,
        )
        .expect_err("a non-empty baseboard does not deserialize as empty");
        assert!(
            error.to_string().starts_with(
                "baseboard 913-0000019:BRM42220001 has a non-empty part and \
                 serial number"
            ),
            "{error}",
        );
        let error = serde_json::from_str::<SledExpungeUnidentifiedCubbies>(
            "[]",
        )
        .expect_err(
            "an empty list of unidentified cubbies does not deserialize",
        );
        assert!(
            error.to_string().starts_with(
                "a list of unidentified sled cubbies must not be empty"
            ),
            "{error}",
        );
    }
}
