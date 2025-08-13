// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga for updating multicast group identity information in the dataplane
//! (via DPD).
//!
//! This saga handles atomic updates of both external and underlay multicast
//! groups when identity information (name) or source IPs change.
//!
//! The saga is triggered when multicast_group_update() is called and ensures
//! that either both groups are successfully updated on all switches, or any
//! partial changes are rolled back.

use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};
use slog::{debug, info};
use steno::{ActionError, DagBuilder, Node};
use uuid::Uuid;

use dpd_client::types::{
    MulticastGroupExternalResponse, MulticastGroupUnderlayResponse,
};
use nexus_db_model::{MulticastGroup, UnderlayMulticastGroup};
use nexus_db_queries::authn;
use nexus_types::identity::Resource;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};

use super::{ActionRegistry, NexusActionContext, NexusSaga, SagaInitError};
use crate::app::multicast::dataplane::{
    GroupUpdateParams, MulticastDataplaneClient,
};
use crate::app::sagas::declare_saga_actions;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct Params {
    /// Authentication context
    pub serialized_authn: authn::saga::Serialized,
    /// External multicast group to update
    pub external_group_id: Uuid,
    /// Underlay multicast group to update
    pub underlay_group_id: Uuid,
    /// Old group name (for rollback)
    pub old_name: String,
    /// New group name (for DPD tag updates)
    pub new_name: String,
    /// Old sources (for rollback)
    pub old_sources: Vec<IpNetwork>,
    /// New sources (for update)
    pub new_sources: Vec<IpNetwork>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DataplaneUpdateResponse {
    underlay: MulticastGroupUnderlayResponse,
    external: MulticastGroupExternalResponse,
}

declare_saga_actions! {
    multicast_group_dpd_update;

    FETCH_GROUP_DATA -> "group_data" {
        + mgu_fetch_group_data
    }
    UPDATE_DATAPLANE -> "update_responses" {
        + mgu_update_dataplane
        - mgu_rollback_dataplane
    }
}

#[derive(Debug)]
pub struct SagaMulticastGroupDpdUpdate;
impl NexusSaga for SagaMulticastGroupDpdUpdate {
    const NAME: &'static str = "multicast-group-dpd-update";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        multicast_group_dpd_update_register_actions(registry);
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

        Ok(builder.build()?)
    }
}

/// Fetch multicast group data from database.
async fn mgu_fetch_group_data(
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
        "fetching multicast group data for identity update";
        "external_group_id" => %params.external_group_id,
        "underlay_group_id" => %params.underlay_group_id,
        "old_name" => %params.old_name,
        "new_name" => %params.new_name,
        "old_sources" => ?params.old_sources,
        "new_sources" => ?params.new_sources
    );

    // Fetch external multicast group
    let external_group = osagactx
        .datastore()
        .multicast_group_fetch(
            &opctx,
            MulticastGroupUuid::from_untyped_uuid(params.external_group_id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    // Fetch underlay multicast group
    let underlay_group = osagactx
        .datastore()
        .underlay_multicast_group_fetch(&opctx, params.underlay_group_id)
        .await
        .map_err(ActionError::action_failed)?;

    debug!(
        osagactx.log(),
        "successfully fetched multicast group data for update";
        "external_group_id" => %external_group.id(),
        "external_ip" => %external_group.multicast_ip,
        "underlay_group_id" => %underlay_group.id,
        "underlay_ip" => %underlay_group.multicast_ip
    );

    Ok((external_group, underlay_group))
}

/// Update both external and underlay groups in the dataplane atomically.
async fn mgu_update_dataplane(
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
        "updating multicast group identity via DPD across switches";
        "switch_count" => %dataplane.switch_count(),
        "external_ip" => %external_group.multicast_ip,
        "underlay_ip" => %underlay_group.multicast_ip,
        "params" => ?params,
    );

    let (underlay_response, external_response) = dataplane
        .update_groups(
            &opctx,
            GroupUpdateParams {
                external_group: &external_group,
                underlay_group: &underlay_group,
                new_name: &params.new_name,
                new_sources: &params.new_sources,
            },
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(
        osagactx.log(),
        "successfully updated multicast groups via DPD across switches";
        "external_group_id" => %external_group.id(),
        "underlay_group_id" => %underlay_group.id,
        "old_name" => %params.old_name,
        "new_name" => %params.new_name
    );

    Ok(DataplaneUpdateResponse {
        underlay: underlay_response,
        external: external_response,
    })
}

async fn mgu_rollback_dataplane(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let (external_group, underlay_group) = sagactx
        .lookup::<(MulticastGroup, UnderlayMulticastGroup)>("group_data")?;

    // Use MulticastDataplaneClient for consistent cleanup
    let dataplane = MulticastDataplaneClient::new(
        osagactx.nexus().datastore().clone(),
        osagactx.nexus().resolver().clone(),
        osagactx.log().clone(),
    )
    .await
    .map_err(ActionError::action_failed)?;

    info!(
        osagactx.log(),
        "rolling back multicast group updates";
        "external_group_id" => %params.external_group_id,
        "underlay_group_id" => %params.underlay_group_id,
        "reverting_to_old_name" => %params.old_name,
    );

    dataplane
        .update_groups(
            &opctx,
            GroupUpdateParams {
                external_group: &external_group,
                underlay_group: &underlay_group,
                new_name: &params.old_name,
                new_sources: &params.old_sources,
            },
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(
        osagactx.log(),
        "successfully completed atomic rollback of multicast group updates";
        "switches_reverted" => %dataplane.switch_count(),
        "reverted_to_tag" => %params.old_name
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
            old_name: "old-group-name".to_string(),
            new_name: "new-group-name".to_string(),
            old_sources: vec![],
            new_sources: vec![],
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_dag_structure(cptestctx: &ControlPlaneTestContext) {
        let opctx = test_helpers::test_opctx(cptestctx);
        let params = new_test_params(&opctx);
        let dag =
            create_saga_dag::<SagaMulticastGroupDpdUpdate>(params).unwrap();

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
