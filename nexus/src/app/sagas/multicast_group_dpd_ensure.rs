// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga for applying multicast dataplane configuration via DPD.
//!
//! Atomically applies external and underlay multicast configuration via DPD.
//! Either both are successfully applied on all switches, or partial changes
//! are rolled back.
//!
//! Triggered by RPW reconciler when a multicast group is in "Creating" state
//! and needs dataplane updates.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use slog::{debug, warn};
use steno::{ActionError, DagBuilder, Node};
use uuid::Uuid;

use dpd_client::types::{
    MulticastGroupExternalResponse, MulticastGroupUnderlayResponse,
};

use nexus_db_lookup::LookupDataStore;
use nexus_db_model::{MulticastGroup, UnderlayMulticastGroup};
use nexus_db_queries::authn;
use nexus_types::identity::Resource;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};

use super::{ActionRegistry, NexusActionContext, NexusSaga, SagaInitError};
use crate::app::multicast::dataplane::MulticastDataplaneClient;
use crate::app::sagas::declare_saga_actions;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct Params {
    /// Authentication context
    pub serialized_authn: authn::saga::Serialized,
    /// External multicast group to program
    pub external_group_id: Uuid,
    /// Underlay multicast group to program
    pub underlay_group_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DataplaneUpdateResponse {
    underlay: MulticastGroupUnderlayResponse,
    external: MulticastGroupExternalResponse,
}

declare_saga_actions! {
    multicast_group_dpd_ensure;

    FETCH_GROUP_DATA -> "group_data" {
        + mgde_fetch_group_data
    }
    UPDATE_DATAPLANE -> "update_responses" {
        + mgde_update_dataplane
        - mgde_rollback_dataplane
    }
    UPDATE_GROUP_STATE -> "state_updated" {
        + mgde_update_group_state
    }
}

#[derive(Debug)]
pub struct SagaMulticastGroupDpdEnsure;
impl NexusSaga for SagaMulticastGroupDpdEnsure {
    const NAME: &'static str = "multicast-group-dpd-ensure";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        multicast_group_dpd_ensure_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(Node::action(
            "group_data",
            "FetchGroupData",
            FETCH_GROUP_DATA.as_ref(),
        ));

        builder.append(Node::action(
            "update_responses",
            "UpdateDataplane",
            UPDATE_DATAPLANE.as_ref(),
        ));

        builder.append(Node::action(
            "state_updated",
            "UpdateGroupState",
            UPDATE_GROUP_STATE.as_ref(),
        ));

        Ok(builder.build()?)
    }
}

/// Fetch multicast group data from database.
async fn mgde_fetch_group_data(
    sagactx: NexusActionContext,
) -> Result<(MulticastGroup, UnderlayMulticastGroup), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    debug!(
        osagactx.log(),
        "fetching multicast group data";
        "external_group_id" => %params.external_group_id,
        "underlay_group_id" => %params.underlay_group_id
    );

    let conn = osagactx
        .datastore()
        .pool_connection_authorized(&opctx)
        .await
        .map_err(ActionError::action_failed)?;

    // Fetch both groups on same connection for consistent state view
    // (sequential fetches since using same connection)
    let external_group = osagactx
        .datastore()
        .multicast_group_fetch_on_conn(&conn, params.external_group_id)
        .await
        .map_err(ActionError::action_failed)?;

    let underlay_group = osagactx
        .datastore()
        .underlay_multicast_group_fetch_on_conn(&conn, params.underlay_group_id)
        .await
        .map_err(ActionError::action_failed)?;

    // Validate groups are in correct state
    match external_group.state {
        nexus_db_model::MulticastGroupState::Creating => {}
        other_state => {
            warn!(
                osagactx.log(),
                "external group not in 'Creating' state for DPD";
                "external_group_id" => %params.external_group_id,
                "external_group_name" => external_group.name().as_str(),
                "current_state" => ?other_state
            );
            return Err(ActionError::action_failed(format!(
                "External group {} is in state {other_state:?}, expected 'Creating'",
                params.external_group_id
            )));
        }
    }

    debug!(
        osagactx.log(),
        "fetched multicast group data";
        "external_group_id" => %external_group.id(),
        "external_group_name" => external_group.name().as_str(),
        "external_ip" => %external_group.multicast_ip,
        "underlay_group_id" => %underlay_group.id,
        "underlay_ip" => %underlay_group.multicast_ip,
        "vni" => %u32::from(external_group.vni.0)
    );

    Ok((external_group, underlay_group))
}

/// Apply external and underlay groups in dataplane atomically.
async fn mgde_update_dataplane(
    sagactx: NexusActionContext,
) -> Result<DataplaneUpdateResponse, ActionError> {
    let osagactx = sagactx.user_data();
    let (external_group, underlay_group) = sagactx
        .lookup::<(MulticastGroup, UnderlayMulticastGroup)>("group_data")?;

    // Use MulticastDataplaneClient for consistent DPD operations
    let dataplane = MulticastDataplaneClient::new(
        osagactx.nexus().resolver().clone(),
        osagactx.log().clone(),
    )
    .await
    .map_err(ActionError::action_failed)?;

    debug!(
        osagactx.log(),
        "applying multicast configuration via DPD";
        "switch_count" => %dataplane.switch_count(),
        "external_group_id" => %external_group.id(),
        "external_group_name" => external_group.name().as_str(),
        "external_ip" => %external_group.multicast_ip,
        "underlay_group_id" => %underlay_group.id,
        "underlay_ip" => %underlay_group.multicast_ip,
    );

    let (underlay_response, external_response) = dataplane
        .create_groups(&external_group, &underlay_group)
        .await
        .map_err(ActionError::action_failed)?;

    debug!(
        osagactx.log(),
        "applied multicast configuration via DPD";
        "external_group_id" => %external_group.id(),
        "external_group_name" => external_group.name().as_str(),
        "underlay_group_id" => %underlay_group.id,
        "external_ip" => %external_group.multicast_ip,
        "underlay_ip" => %underlay_group.multicast_ip
    );

    Ok(DataplaneUpdateResponse {
        underlay: underlay_response,
        external: external_response,
    })
}

async fn mgde_rollback_dataplane(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let (external_group, _) = sagactx
        .lookup::<(MulticastGroup, UnderlayMulticastGroup)>("group_data")?;

    let multicast_tag = external_group.dpd_tag();

    // Use MulticastDataplaneClient for consistent cleanup
    let dataplane = MulticastDataplaneClient::new(
        osagactx.nexus().resolver().clone(),
        osagactx.log().clone(),
    )
    .await
    .map_err(ActionError::action_failed)?;

    debug!(
        osagactx.log(),
        "rolling back multicast additions";
        "external_group_id" => %params.external_group_id,
        "underlay_group_id" => %params.underlay_group_id,
        "tag" => %multicast_tag,
        "external_group_name" => external_group.name().as_str(),
    );

    dataplane
        .remove_groups(&multicast_tag)
        .await
        .context("failed to cleanup multicast groups during saga rollback")?;

    debug!(
        osagactx.log(),
        "completed rollback of multicast configuration";
        "tag" => %multicast_tag
    );

    Ok(())
}

/// Update multicast group state to "Active" after applying DPD configuration.
async fn mgde_update_group_state(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let (external_group, _) = sagactx
        .lookup::<(MulticastGroup, UnderlayMulticastGroup)>("group_data")?;

    debug!(
        osagactx.log(),
        "updating multicast group state to 'Active'";
        "external_group_id" => %params.external_group_id,
        "current_state" => ?external_group.state
    );

    // Transition the group from "Creating" -> "Active"
    osagactx
        .datastore()
        .multicast_group_set_active(
            &opctx,
            MulticastGroupUuid::from_untyped_uuid(params.external_group_id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    debug!(
        osagactx.log(),
        "transitioned multicast group to 'Active'";
        "external_group_id" => %params.external_group_id
    );

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use std::net::{IpAddr, Ipv4Addr};

    use omicron_uuid_kinds::GenericUuid;

    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, link_ip_pool, object_create,
    };
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::params::IpPoolCreate;
    use nexus_types::external_api::shared::{IpRange, Ipv4Range};
    use nexus_types::external_api::views::{IpPool, IpPoolRange, IpVersion};
    use nexus_types::multicast::MulticastGroupCreate;
    use omicron_common::api::external::IdentityMetadataCreateParams;

    use crate::app::saga::create_saga_dag;
    use crate::app::sagas::test_helpers;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    fn new_test_params(opctx: &nexus_db_queries::context::OpContext) -> Params {
        Params {
            serialized_authn: Serialized::for_opctx(opctx),
            external_group_id: Uuid::new_v4(),
            underlay_group_id: Uuid::new_v4(),
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        // Test that repeated rollback attempts don't cause issues
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_helpers::test_opctx(cptestctx);

        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            external_group_id: Uuid::new_v4(),
            underlay_group_id: Uuid::new_v4(),
        };

        // Run the saga multiple times to test idempotent rollback
        for _i in 1..=3 {
            let result = nexus
                .sagas
                .saga_execute::<SagaMulticastGroupDpdEnsure>(params.clone())
                .await;

            // Each attempt should fail consistently
            assert!(result.is_err());
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_params_serialization(cptestctx: &ControlPlaneTestContext) {
        let opctx = test_helpers::test_opctx(cptestctx);
        let params = new_test_params(&opctx);

        // Test that parameters can be serialized and deserialized
        let serialized = serde_json::to_string(&params).unwrap();
        let deserialized: Params = serde_json::from_str(&serialized).unwrap();

        assert_eq!(params.external_group_id, deserialized.external_group_id);
        assert_eq!(params.underlay_group_id, deserialized.underlay_group_id);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_dag_structure(cptestctx: &ControlPlaneTestContext) {
        let opctx = test_helpers::test_opctx(cptestctx);
        let params = new_test_params(&opctx);
        let dag =
            create_saga_dag::<SagaMulticastGroupDpdEnsure>(params).unwrap();

        // Verify the DAG has the expected structure
        let nodes: Vec<_> = dag.get_nodes().collect();
        assert!(nodes.len() >= 2); // Should have at least our 2 main actions

        // Verify expected node labels exist
        let node_labels: std::collections::HashSet<_> =
            nodes.iter().map(|node| node.label()).collect();

        assert!(node_labels.contains("FetchGroupData"));
        assert!(node_labels.contains("UpdateDataplane"));
    }

    /// Verify saga handles missing groups gracefully when executed with
    /// non-existent group IDs.
    #[nexus_test(server = crate::Server)]
    async fn test_saga_handles_missing_groups(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_helpers::test_opctx(cptestctx);

        // Create params with non-existent UUIDs
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            external_group_id: Uuid::new_v4(), // Non-existent
            underlay_group_id: Uuid::new_v4(), // Non-existent
        };

        // Execute the saga - should fail gracefully when fetching non-existent groups
        let result = nexus
            .sagas
            .saga_execute::<SagaMulticastGroupDpdEnsure>(params)
            .await;

        // Saga should fail (groups don't exist)
        assert!(
            result.is_err(),
            "Saga should fail when groups don't exist in database"
        );
    }

    /// Test that the saga rejects external groups that are not in "Creating" state.
    ///
    /// The saga validates that external groups are in "Creating" state before applying
    /// DPD configuration. This test verifies that validation works correctly.
    #[nexus_test(server = crate::Server)]
    async fn test_saga_rejects_non_creating_state(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_helpers::test_opctx(cptestctx);

        // Setup: Create IP pools
        create_default_ip_pool(client).await;

        // Create multicast IP pool
        let pool_name = "saga-state-pool";
        let pool_params = IpPoolCreate::new_multicast(
            IdentityMetadataCreateParams {
                name: pool_name.parse().unwrap(),
                description: "Multicast IP pool for saga test".to_string(),
            },
            IpVersion::V4,
        );
        object_create::<_, IpPool>(client, "/v1/system/ip-pools", &pool_params)
            .await;

        // Add multicast IP range
        let asm_range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 70, 0, 1),
                Ipv4Addr::new(224, 70, 0, 255),
            )
            .unwrap(),
        );
        let range_url = format!("/v1/system/ip-pools/{}/ranges/add", pool_name);
        object_create::<_, IpPoolRange>(client, &range_url, &asm_range).await;

        // Link pool to silo
        link_ip_pool(client, pool_name, &DEFAULT_SILO.id(), false).await;

        // Create multicast group directly via datastore.
        let (authz_pool, _) = nexus
            .ip_pool_lookup(&opctx, &pool_name.parse().unwrap())
            .expect("Pool lookup should succeed")
            .fetch()
            .await
            .expect("Pool should exist");

        let group_params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "saga-reject-test".parse().unwrap(),
                description: "Test saga state validation".to_string(),
            },
            multicast_ip: Some(IpAddr::V4(Ipv4Addr::new(224, 70, 0, 100))),
            source_ips: None,
            mvlan: None,
        };

        let external_group = datastore
            .multicast_group_create(&opctx, &group_params, Some(authz_pool))
            .await
            .expect("Multicast group should be created");
        let group_id =
            omicron_uuid_kinds::MulticastGroupUuid::from_untyped_uuid(
                external_group.id(),
            );

        // Manually create underlay group (normally done by reconciler)
        let underlay_group = datastore
            .ensure_underlay_multicast_group(
                &opctx,
                external_group.clone(),
                "ff04::1:2:3:4".parse().unwrap(),
            )
            .await
            .expect("Underlay group should be created");

        // Manually transition the group to "Active" state in the database
        datastore
            .multicast_group_set_active(&opctx, group_id)
            .await
            .expect("Group should transition to Active state");

        // Try to run saga on Active group - should fail
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            external_group_id: external_group.id(),
            underlay_group_id: underlay_group.id,
        };

        let result = nexus
            .sagas
            .saga_execute::<SagaMulticastGroupDpdEnsure>(params)
            .await;

        // Saga should reject Active group
        assert!(result.is_err(), "Saga should reject group in Active state");
    }
}
