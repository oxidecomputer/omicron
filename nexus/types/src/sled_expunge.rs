// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: module docs: the pure checks that creating a sled expunge request runs (RFD 747), in the order the creation transaction runs them.

use crate::external_api::sled::SledPolicy;
use crate::external_api::sled::SledProvisionPolicy;
use crate::external_api::sled::SledState;
use crate::inventory::Collection;
use crate::inventory::SpType;
use crate::trust_quorum::RemoveSledError;
use crate::trust_quorum::TrustQuorumConfig;
use crate::trust_quorum::TrustQuorumConfigState;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::SledRole;
use sled_hardware_types::BaseboardId;
use slog_error_chain::InlineErrorChain;
use std::fmt;
use trust_quorum_types::membership::TRUST_QUORUM_MIN_MEMBERS;
use trust_quorum_types::membership::TrustQuorumMembersError;

// TODO-RAINCLAUDE: the sled cubbies cabled to the rack's switches, so a sled in one of them is a scrimlet; mirrors the `local_sled` values in smf/mgs/config.toml. The sim and test MGS configs put the switch-attached sleds in slots 0 and 1, so the slot branch of the scrimlet rule never fires in the test harness.
pub const SWITCH_ATTACHED_SLED_CUBBIES: [u16; 2] = [14, 16];

#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum FinishedSledExpungeRequestState {
    Done,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FinishedSledExpungeRequest {
    pub id: SledExpungeRequestUuid,
    pub state: FinishedSledExpungeRequestState,
}

// TODO-RAINCLAUDE: the refusal from check_sled_expunge_policy_and_state; turning it into an error takes the sled's most recent finished request, which the caller reads only once the check has failed.
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

// TODO-RAINCLAUDE: RFD 747 creation check 2.2. A non-provisionable sled is accepted, so a request can be resubmitted after a failed one left the sled non-provisionable.
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

// TODO-RAINCLAUDE: RFD 747 creation check 2.3, the operator's acknowledgment: echoing the sled's serial number. The form of the acknowledgment is to be revisited with the team. Compared exactly against the serial number in the sled row, with no normalization.
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
         serial number. Check that this is the sled you mean to expunge, \
         then retry with its serial number."
    )))
}

// TODO-RAINCLAUDE: what the latest inventory collection says about whether a sled is a scrimlet; the fields are private so that it can only be built from a collection (or its absence), so callers can't misread the collection. Only a sled-type SP counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SledExpungeScrimletEvidence(ScrimletEvidence);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScrimletEvidence {
    NoCollection,
    Collection { sled_agent_role: Option<SledRole>, sled_sp_slot: Option<u16> },
}

impl SledExpungeScrimletEvidence {
    pub fn from_collection(
        latest_collection: Option<&Collection>,
        sled_id: SledUuid,
        baseboard_id: &BaseboardId,
    ) -> Self {
        let Some(collection) = latest_collection else {
            return Self(ScrimletEvidence::NoCollection);
        };
        let sled_agent_role = collection
            .sled_agents
            .get(&sled_id)
            .map(|sled_agent| sled_agent.sled_role);
        let sled_sp_slot = match collection.sp_for(baseboard_id) {
            Ok(sp) => match sp.sp_type {
                SpType::Sled => Some(sp.sp_slot),
                SpType::Switch | SpType::Power => None,
            },
            Err(_not_in_inventory) => None,
        };
        Self(ScrimletEvidence::Collection { sled_agent_role, sled_sp_slot })
    }
}

// TODO-RAINCLAUDE: BIG MARKER. RFD 747 creation check 2.4, the v1 scrimlet rule, to be iterated on with the team: reject if the latest collection's sled agent record says Scrimlet, or if the latest collection has the sled's SP in a switch-attached cubby; only when the collection has neither a sled agent record nor an SP record for the sled (or there is no collection) does the sled row's is_scrimlet (sled_row_role) decide. No older collections are consulted. Known gap: a scrimlet that is dead (no sled agent record and no SP answering) and has already been swapped into another cubby still has is_scrimlet = true, so it is rejected and needs the support path. A stale latest collection can also misjudge a sled moved since it was taken. And the inventory role and is_scrimlet both derive from Tofino presence, so a scrimlet whose Sidecar is off and whose SP is missing from the latest collection passes as a gimlet; only the SP slot is physical evidence.
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

    let (sled_agent_role, sled_sp_slot) = match evidence.0 {
        ScrimletEvidence::NoCollection => {
            return by_sled_row(
                "it is recorded as a scrimlet and there is no inventory \
                 collection",
            );
        }
        ScrimletEvidence::Collection { sled_agent_role, sled_sp_slot } => {
            (sled_agent_role, sled_sp_slot)
        }
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
    let (None, None) = (sled_agent_role, sled_sp_slot) else {
        return Ok(());
    };
    by_sled_row(
        "it is recorded as a scrimlet and the latest inventory collection has \
         neither a sled agent record nor a service processor record for it",
    )
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

// TODO-RAINCLAUDE: RFD 747 creation check 2.6 (membership, 400) and the minimum size (409), worded for the expunge request from ProposedTrustQuorumConfig::remove_sled's refusal; config is the last committed config of the base tq_load_reconfiguration_base_conn returned, so an in-flight reconfiguration was already refused.
pub fn sled_expunge_removal_refusal(
    sled_id: SledUuid,
    config: &TrustQuorumConfig,
    refusal: RemoveSledError,
) -> Error {
    match refusal {
        RemoveSledError::NotMember { sled } => Error::invalid_request(format!(
            "Cannot expunge sled {sled_id}: its baseboard {sled} is not a \
             member of the latest trust quorum configuration for rack {} \
             (epoch {}, {}). This API only expunges sleds that are trust \
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

// TODO-RAINCLAUDE: the stage at which one MGS failed to power the sled off; the caller moves on to the next MGS, and failing that, a later activation retries. RecheckFailed means the slot's identity could not be read, so nothing was sent. PowerOffFailed means the A2 command failed and may have landed: the SP's reply can be lost after it acted, and after a client-side timeout MGS's detached handler and the per-SP serial queue can still deliver the command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MgsPowerOffFailure {
    RecheckFailed,
    PowerOffFailed,
}

impl fmt::Display for MgsPowerOffFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecheckFailed => {
                f.write_str("identity re-check failed; nothing sent")
            }
            Self::PowerOffFailed => f.write_str("power-off command failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            (FinishedSledExpungeRequestState::Done, "done"),
            (FinishedSledExpungeRequestState::Failed, "failed"),
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

    #[derive(Clone, Copy, Debug)]
    enum ScrimletExpected {
        Allowed,
        ByRole,
        BySlot(u16),
        BySledRow,
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
        let no_collection =
            SledExpungeScrimletEvidence(ScrimletEvidence::NoCollection);
        let collection = |sled_agent_role, sled_sp_slot| {
            SledExpungeScrimletEvidence(ScrimletEvidence::Collection {
                sled_agent_role,
                sled_sp_slot,
            })
        };
        // TODO-RAINCLAUDE: one row per branch of the rule, plus the two precedence rows where inventory evidence overrides a Scrimlet sled row (an inventory Gimlet record, and an SP in a cubby that is not switch-attached).
        let cases = [
            (no_collection, Gimlet, Allowed),
            (no_collection, Scrimlet, BySledRowNoCollection),
            (collection(None, None), Scrimlet, BySledRow),
            (collection(None, Some(14)), Gimlet, BySlot(14)),
            (collection(Some(Gimlet), Some(16)), Gimlet, BySlot(16)),
            (collection(Some(Scrimlet), Some(3)), Gimlet, ByRole),
            (collection(Some(Gimlet), None), Scrimlet, Allowed),
            (collection(None, Some(3)), Scrimlet, Allowed),
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
                BySledRow => refused(
                    "it is recorded as a scrimlet and the latest inventory \
                     collection has neither a sled agent record nor a \
                     service processor record for it",
                ),
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
}
