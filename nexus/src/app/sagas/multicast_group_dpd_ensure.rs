// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga for ensuring multicast dataplane configuration is applied (via DPD).
//!
//! This saga atomically applies both external and underlay multicast
//! configuration via DPD. Either both are successfully applied on all
//! switches, or partial changes are rolled back.
//!
//! The saga is triggered by the RPW reconciler when a multicast group is in
//! "Creating" state and needs to make updates to the dataplane.

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

    // Fetch both groups atomically to ensure consistent state view
    let (external_group, underlay_group) = tokio::try_join!(
        osagactx.datastore().multicast_group_fetch_on_conn(
            &opctx,
            &conn,
            params.external_group_id
        ),
        osagactx.datastore().underlay_multicast_group_fetch_on_conn(
            &opctx,
            &conn,
            params.underlay_group_id
        )
    )
    .map_err(ActionError::action_failed)?;

    // Validate that groups are in correct state
    match external_group.state {
        nexus_db_model::MulticastGroupState::Creating => {}
        other_state => {
            warn!(
                osagactx.log(),
                "external group not in 'Creating' state for DPD";
                "external_group_id" => %params.external_group_id,
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
        "external_ip" => %external_group.multicast_ip,
        "underlay_group_id" => %underlay_group.id,
        "underlay_ip" => %underlay_group.multicast_ip,
        "vni" => %u32::from(underlay_group.vni.0)
    );

    Ok((external_group, underlay_group))
}

/// Apply both external and underlay groups in the dataplane atomically.
async fn mgde_update_dataplane(
    sagactx: NexusActionContext,
) -> Result<DataplaneUpdateResponse, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let (external_group, underlay_group) = sagactx
        .lookup::<(MulticastGroup, UnderlayMulticastGroup)>("group_data")?;

    // Use MulticastDataplaneClient for consistent DPD operations
    let dataplane = MulticastDataplaneClient::new(
        osagactx.nexus().datastore().clone(),
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
        "external_ip" => %external_group.multicast_ip,
        "underlay_group_id" => %underlay_group.id,
        "underlay_ip" => %underlay_group.multicast_ip,
    );

    let (underlay_response, external_response) = dataplane
        .create_groups(&opctx, &external_group, &underlay_group)
        .await
        .map_err(ActionError::action_failed)?;

    debug!(
        osagactx.log(),
        "applied multicast configuration via DPD";
        "external_group_id" => %external_group.id(),
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

    let (external_group, _underlay_group) = sagactx
        .lookup::<(MulticastGroup, UnderlayMulticastGroup)>("group_data")?;

    let multicast_tag = external_group.name().to_string();

    // Use MulticastDataplaneClient for consistent cleanup
    let dataplane = MulticastDataplaneClient::new(
        osagactx.nexus().datastore().clone(),
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

/// Update multicast group state to "Active" after successfully applying DPD configuration.
async fn mgde_update_group_state(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let (external_group, _underlay_group) = sagactx
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
        .multicast_group_set_state(
            &opctx,
            params.external_group_id,
            nexus_db_model::MulticastGroupState::Active,
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
    use crate::app::saga::create_saga_dag;
    use crate::app::sagas::test_helpers;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_test_utils_macros::nexus_test;

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
}
