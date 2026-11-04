// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum helpers for Nexus tests.
// TODO-RAINCLAUDE: the trust quorum config the harness's RSS handoff commits as epoch 1 (fake members by default, the live sim sleds with SimTrustQuorumMode::Fake), and accessors for tests started with the fake.

use crate::ControlPlaneTestContext;
use crate::ControlPlaneTestContextSledAgent;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_for_baseboard;
use nexus_test_interface::NexusServer;
use nexus_types::identity::Asset;
use nexus_types::internal_api::params::InitialTrustQuorumConfig;
use nexus_types::trust_quorum::TRUST_QUORUM_MAX_MEMBERS;
use nexus_types::trust_quorum::TRUST_QUORUM_MIN_MEMBERS;
use nexus_types::trust_quorum::TrustQuorumConfig;
use nexus_types::trust_quorum::TrustQuorumConfigState;
use omicron_sled_agent::sim::SimTrustQuorumMode;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use trust_quorum_types::types::Epoch;

const FAKE_TRUST_QUORUM_SIM_SLEDS: RangeInclusive<usize> =
    TRUST_QUORUM_MIN_MEMBERS..=TRUST_QUORUM_MAX_MEMBERS;

fn fake_trust_quorum_sim_sleds_message(sim_sleds: usize) -> String {
    format!(
        "the fake sim trust quorum needs {} to {} harness sim sleds (4 or \
         more sim sleds for one removal), but the harness has {sim_sleds}: \
         use extra_sled_agents = {} to {}",
        FAKE_TRUST_QUORUM_SIM_SLEDS.start(),
        FAKE_TRUST_QUORUM_SIM_SLEDS.end(),
        FAKE_TRUST_QUORUM_SIM_SLEDS.start() - 1,
        FAKE_TRUST_QUORUM_SIM_SLEDS.end() - 1,
    )
}

pub(crate) fn harness_initial_trust_quorum_config(
    sim_trust_quorum: SimTrustQuorumMode,
    sled_agents: &[ControlPlaneTestContextSledAgent],
) -> InitialTrustQuorumConfig {
    match sim_trust_quorum {
        SimTrustQuorumMode::Unimplemented => fake_member_config(),
        SimTrustQuorumMode::Fake => sim_sled_config(sled_agents),
    }
}

fn fake_member_config() -> InitialTrustQuorumConfig {
    // Insert a fake trust quorum config such that existing
    // sleds will never be present.
    //
    // The purpose of this is solely for expunge testing. We
    // don't do any trust quorum testing in `NexusServer` based
    // tests, but we do test expunging sleds. The second part
    // of exunging a sled, as used in these tests, relies on
    // the sled not being part of the current trust quorum.
    // This gets checked in the production nexus code path. By
    // inserting an fake config as the latest committed config
    // we can ensure that no sled used by this test will every
    // be present in a trust quorum config and that the existing
    // expunge tests will pass.
    let coordinator = BaseboardId {
        part_number: "_test_fake_tq_9adfa".into(),
        serial_number: "a1".into(),
    };
    InitialTrustQuorumConfig {
        // We need at least 3 members
        members: BTreeSet::from([
            coordinator.clone(),
            BaseboardId {
                part_number: "_test_fake_9adfa".into(),
                serial_number: "a2".into(),
            },
            BaseboardId {
                part_number: "_test_fake_tq_9adfa".into(),
                serial_number: "a3".into(),
            },
        ]),
        // Coordinator must be one of the members
        coordinator,
    }
}

// TODO-RAINCLAUDE: every harness sim sled has registered with Nexus before the handoff (NexusRegistration::WaitForCompletion), and rack initialization inserts the members' hw_baseboard_id rows itself, so no inventory collection is needed.
fn sim_sled_config(
    sled_agents: &[ControlPlaneTestContextSledAgent],
) -> InitialTrustQuorumConfig {
    assert!(
        FAKE_TRUST_QUORUM_SIM_SLEDS.contains(&sled_agents.len()),
        "{}",
        fake_trust_quorum_sim_sleds_message(sled_agents.len()),
    );
    let members: BTreeSet<_> = sled_agents
        .iter()
        .map(|sled_agent| sled_agent.sled_agent().baseboard_id())
        .collect();
    assert_eq!(
        members.len(),
        sled_agents.len(),
        "every sim sled has a distinct baseboard",
    );
    let coordinator =
        members.first().expect("the harness has at least one sim sled").clone();
    InitialTrustQuorumConfig { members, coordinator }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimTrustQuorumMember {
    pub sled_id: SledUuid,
    pub baseboard_id: BaseboardId,
}

impl IdOrdItem for SimTrustQuorumMember {
    type Key<'a> = SledUuid;
    fn key(&self) -> Self::Key<'_> {
        self.sled_id
    }
    id_upcast!();
}

#[derive(Clone, Debug)]
pub struct InitialTrustQuorum {
    pub config: TrustQuorumConfig,
    pub members_by_sled: IdOrdMap<SimTrustQuorumMember>,
}

pub async fn initial_trust_quorum<N: NexusServer>(
    cptestctx: &ControlPlaneTestContext<N>,
) -> InitialTrustQuorum {
    match cptestctx.sim_trust_quorum {
        SimTrustQuorumMode::Fake => {}
        SimTrustQuorumMode::Unimplemented => panic!(
            "the harness was started without the fake sim trust quorum, so \
             its trust quorum members are fake baseboards: use \
             ControlPlaneBuilder::with_sim_trust_quorum"
        ),
    }

    let members_by_sled =
        IdOrdMap::from_iter_unique(cptestctx.all_sled_agents().map(|server| {
            SimTrustQuorumMember {
                sled_id: server.sled_agent.id,
                baseboard_id: server.sled_agent.baseboard_id(),
            }
        }))
        .expect("every sim sled has a distinct sled ID");

    let datastore = cptestctx.server.datastore();
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(slog::o!("component" => "tq-fixture")),
        datastore.clone(),
    );
    let config = datastore
        .tq_get_config(
            &opctx,
            authz::TrustQuorumConfig::for_rack_id(crate::RACK_UUID),
            Epoch(1),
        )
        .await
        .expect("loaded the trust quorum configuration at epoch 1")
        .expect(
            "RSS handoff committed a trust quorum configuration at epoch 1",
        );
    assert_eq!(config.state, TrustQuorumConfigState::Committed);
    assert_eq!(
        config.members.keys().cloned().collect::<BTreeSet<_>>(),
        members_by_sled
            .iter()
            .map(|member| member.baseboard_id.clone())
            .collect::<BTreeSet<_>>(),
        "the epoch 1 members are the harness's sim sleds",
    );

    InitialTrustQuorum { config, members_by_sled }
}

#[derive(Clone, Debug)]
pub struct CommissionedFakeMember {
    pub baseboard_id: BaseboardId,
    pub sled_id: SledUuid,
}

/// Insert a fake trust quorum member for testing.
///
/// This function:
///
/// * Obtains a baseboard ID from trust quorum.
/// * Upserts an in-service sled row corresponding to the baseboard.
pub async fn commission_fake_trust_quorum_member<N: NexusServer>(
    cptestctx: &ControlPlaneTestContext<N>,
) -> CommissionedFakeMember {
    match cptestctx.sim_trust_quorum {
        SimTrustQuorumMode::Unimplemented => {}
        SimTrustQuorumMode::Fake => panic!(
            "the harness was started with the fake sim trust quorum, so its \
             trust quorum members are already in-service sim sleds"
        ),
    }

    let datastore = cptestctx.server.datastore();
    let opctx = OpContext::for_tests(
        cptestctx.logctx.log.new(slog::o!("component" => "tq-fixture")),
        datastore.clone(),
    );
    let config = datastore
        .tq_get_config(
            &opctx,
            authz::TrustQuorumConfig::for_rack_id(crate::RACK_UUID),
            Epoch(1),
        )
        .await
        .expect("loaded the trust quorum configuration at epoch 1")
        .expect(
            "RSS handoff committed a trust quorum configuration at epoch 1",
        );
    let baseboard_id = config
        .members
        .first_key_value()
        .expect("the epoch 1 trust quorum configuration has members")
        .0
        .clone();
    let sled_id =
        sled_upsert_for_baseboard(datastore, crate::RACK_UUID, &baseboard_id)
            .await
            .id();
    CommissionedFakeMember { baseboard_id, sled_id }
}
