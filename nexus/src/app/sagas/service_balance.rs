// This Source Ccode Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{NexusActionContext, NexusSaga, SagaInitError, ACTION_GENERATE_ID};
use crate::app::sagas::NexusAction;
use crate::authn;
use crate::context::OpContext;
use crate::db::identity::Resource;
use crate::db::model::ServiceKind;
use chrono::Utc;
use lazy_static::lazy_static;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::RACK_PREFIX;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionError;
use steno::ActionFunc;
use steno::Node;
use steno::{DagBuilder, SagaName};
use uuid::Uuid;

// service balance saga: input parameters

/// Describes the target location where the services should
/// eventually be running.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ServiceDestination {
    Scrimlet,
    Rack,
}

/// Parameters used to balance many services.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub destination: ServiceDestination,
    pub kind: ServiceKind,
    pub rack_id: Uuid,
    pub redundancy: u32,
}

/// Parameters used to instantiate a single service.
///
/// This is used within a sub-saga.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServiceParams {
    which: u32,
    kind: ServiceKind,
    rack_id: Uuid,
}

lazy_static! {
    static ref MARK_RACK_BALANCING: NexusAction = ActionFunc::new_action(
        "service-balance.mark-rack-balancing",
        mark_rack_balancing,
        mark_rack_balancing_undo,
    );
    static ref PICK_DESTINATION_SLEDS: NexusAction = new_action_noop_undo(
        "service-balance.pick-destination-sleds",
        pick_destination_sleds,
    );
    static ref CREATE_SERVICE_RECORD: NexusAction = ActionFunc::new_action(
        "service-balance.create-service-record",
        create_service_record,
        create_service_record_undo,
    );
    static ref CREATE_INTERNAL_IP: NexusAction = ActionFunc::new_action(
        "service-balance.create-internal-ip",
        create_internal_ip,
        create_internal_ip_undo,
    );
    static ref CREATE_EXTERNAL_IP: NexusAction = ActionFunc::new_action(
        "service-balance.create-external-ip",
        create_external_ip,
        destroy_external_ip,
    );
    static ref UNMARK_RACK_BALANCING: NexusAction = new_action_noop_undo(
        "service-balance.unmark-rack-balancing",
        unmark_rack_balancing,
    );
}

// Helper function for appending subsagas to our parent saga.
fn subsaga_append<S: Serialize>(
    node_basename: &'static str,
    subsaga_builder: steno::DagBuilder,
    parent_builder: &mut steno::DagBuilder,
    params: S,
    which: u32,
) -> Result<(), SagaInitError> {
    // The "parameter" node is a constant node that goes into the outer saga.
    let params_node_name = format!("{}_params{}", node_basename, which);
    parent_builder.append(Node::constant(
        &params_node_name,
        serde_json::to_value(&params).map_err(|e| {
            SagaInitError::SerializeError(params_node_name.clone(), e)
        })?,
    ));

    let output_name = format!("{}{}", node_basename, which);
    parent_builder.append(Node::subsaga(
        output_name.as_str(),
        subsaga_builder.build()?,
        params_node_name,
    ));
    Ok(())
}

#[derive(Debug)]
pub struct SagaServiceBalance;
impl NexusSaga for SagaServiceBalance {
    const NAME: &'static str = "service-balance";
    type Params = Params;

    fn register_actions(registry: &mut super::ActionRegistry) {
        registry.register(Arc::clone(&*MARK_RACK_BALANCING));
        registry.register(Arc::clone(&*PICK_DESTINATION_SLEDS));
        registry.register(Arc::clone(&*CREATE_SERVICE_RECORD));
        registry.register(Arc::clone(&*CREATE_INTERNAL_IP));
        registry.register(Arc::clone(&*CREATE_EXTERNAL_IP));
        registry.register(Arc::clone(&*UNMARK_RACK_BALANCING));
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        let instance_id = Uuid::new_v4();

        builder.append(Node::action(
            "mark_balancing",
            "MarkBalancing",
            MARK_RACK_BALANCING.as_ref(),
        ));

        builder.append(Node::action(
            "destination_sleds",
            "PickDestinationSleds",
            PICK_DESTINATION_SLEDS.as_ref(),
        ));

        // After selecting destination sleds for our desired number of services,
        // we need to actually provision the services themselves.
        //
        // We do so by creating subsagas for each potential to-be-allocated
        // service.
        for i in 0..params.redundancy {
            let repeat_params = ServiceParams {
                which: i,
                kind: params.kind,
                rack_id: params.rack_id,
            };
            let subsaga_name = SagaName::new(&format!("create-service{i}"));
            let mut subsaga_builder = DagBuilder::new(subsaga_name);
            subsaga_builder.append(Node::action(
                "internal_ip{i}",
                format!("CreateServiceIp{i}").as_str(),
                CREATE_INTERNAL_IP.as_ref(),
            ));
            subsaga_append(
                "network_interface",
                subsaga_builder,
                &mut builder,
                repeat_params,
                i,
            )?;
        }

        builder.append(Node::action(
            "unmark_balancing",
            "UnmarkBalancing",
            UNMARK_RACK_BALANCING.as_ref(),
        ));

        Ok(builder.build()?)
    }
}

async fn mark_rack_balancing(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO
    Ok(())
}

async fn mark_rack_balancing_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // TODO
    Ok(())
}

async fn unmark_rack_balancing(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO
    Ok(())
}

async fn create_service_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO
    Ok(())
}

async fn create_service_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // TODO
    Ok(())
}

async fn create_internal_ip(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO
    Ok(())
}

async fn create_internal_ip_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // TODO
    Ok(())
}

async fn create_external_ip(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO
    Ok(())
}

async fn destroy_external_ip(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // TODO
    Ok(())
}

async fn pick_destination_sleds(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    // TODO
    Ok(())
}
