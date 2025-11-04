// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga for updating multicast group state in dataplane via DPD.
//!
//! Handles atomic updates of external and underlay multicast groups in DPD.
//! Reads current state from database and applies to all switches.
//!
//! Idempotent saga can be called multiple times safely. If group state hasn't
//! changed, DPD-update is effectively a no-op.

use anyhow::Context;
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
        "fetching multicast group data for DPD-update";
        "external_group_id" => %params.external_group_id,
        "underlay_group_id" => %params.underlay_group_id
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
        "successfully fetched multicast group data for DPD-update";
        "external_group_id" => %external_group.id(),
        "external_group_name" => external_group.name().as_str(),
        "external_ip" => %external_group.multicast_ip,
        "underlay_group_id" => %underlay_group.id,
        "underlay_ip" => %underlay_group.multicast_ip,
        "sources" => ?external_group.source_ips
    );

    Ok((external_group, underlay_group))
}

/// Update external and underlay groups in dataplane atomically.
async fn mgu_update_dataplane(
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
        "updating multicast group in DPD across switches (idempotent)";
        "switch_count" => %dataplane.switch_count(),
        "external_group_id" => %external_group.id(),
        "external_group_name" => external_group.name().as_str(),
        "external_ip" => %external_group.multicast_ip,
        "underlay_ip" => %underlay_group.multicast_ip,
        "sources" => ?external_group.source_ips,
    );

    let (underlay_response, external_response) = dataplane
        .update_groups(GroupUpdateParams {
            external_group: &external_group,
            underlay_group: &underlay_group,
            new_name: external_group.name().as_str(),
            new_sources: &external_group.source_ips,
        })
        .await
        .map_err(ActionError::action_failed)?;

    info!(
        osagactx.log(),
        "successfully updated multicast groups in DPD across switches";
        "external_group_id" => %external_group.id(),
        "underlay_group_id" => %underlay_group.id,
        "group_name" => external_group.name().as_str()
    );

    Ok(DataplaneUpdateResponse {
        underlay: underlay_response,
        external: external_response,
    })
}

/// Roll back multicast group updates by removing groups from DPD.
async fn mgu_rollback_dataplane(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let (external_group, _) = sagactx
        .lookup::<(MulticastGroup, UnderlayMulticastGroup)>("group_data")?;

    let multicast_tag = external_group.name().to_string();

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
