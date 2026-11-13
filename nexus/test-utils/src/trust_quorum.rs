// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum helpers for Nexus tests.

use crate::ControlPlaneTestContext;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pub_test_utils::helpers::sled_upsert_for_baseboard;
use nexus_test_interface::NexusServer;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use trust_quorum_types::types::Epoch;

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
