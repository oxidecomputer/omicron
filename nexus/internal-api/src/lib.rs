// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, BTreeSet};

use dropshot::{
    FreeformBody, HttpError, HttpResponseCreated, HttpResponseDeleted,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path, Query, RequestContext,
    ResultsPage, TypedBody,
};
use nexus_types::{
    deployment::{
        Blueprint, BlueprintMetadata, BlueprintTarget, BlueprintTargetSet,
    },
    external_api::{
        params::{PhysicalDiskPath, SledSelector, UninitializedSledId},
        shared::{ProbeInfo, UninitializedSled},
        views::SledPolicy,
    },
    internal_api::{
        params::{
            OximeterInfo, RackInitializationRequest, SledAgentInfo,
            SwitchPutRequest, SwitchPutResponse,
        },
        views::{BackgroundTask, Ipv4NatEntryView, Saga},
    },
};
use omicron_common::{
    api::{
        external::http_pagination::PaginatedById,
        internal::nexus::{
            DiskRuntimeState, DownstairsClientStopRequest,
            DownstairsClientStopped, ProducerEndpoint,
            ProducerRegistrationResponse, RepairFinishInfo, RepairProgress,
            RepairStartInfo, SledInstanceState,
        },
    },
    update::ArtifactId,
};
use omicron_uuid_kinds::{
    DownstairsKind, SledUuid, TypedUuid, UpstairsKind, UpstairsRepairKind,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[dropshot::api_description]
pub trait NexusInternalApi {
    type Context;

    /// Return information about the given sled agent
    #[endpoint {
        method = GET,
        path = "/sled-agents/{sled_id}",
    }]
    async fn sled_agent_get(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SledAgentPathParam>,
    ) -> Result<HttpResponseOk<SledAgentInfo>, HttpError>;

    /// Report that the sled agent for the specified sled has come online.
    #[endpoint {
        method = POST,
        path = "/sled-agents/{sled_id}",
    }]
    async fn sled_agent_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SledAgentPathParam>,
        sled_info: TypedBody<SledAgentInfo>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Request a new set of firewall rules for a sled.
    ///
    /// This causes Nexus to read the latest set of rules for the sled,
    /// and call a Sled endpoint which applies the rules to all OPTE ports
    /// that happen to exist.
    #[endpoint {
        method = POST,
        path = "/sled-agents/{sled_id}/firewall-rules-update",
    }]
    async fn sled_firewall_rules_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SledAgentPathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Report that the Rack Setup Service initialization is complete
    ///
    /// See RFD 278 for more details.
    #[endpoint {
        method = PUT,
        path = "/racks/{rack_id}/initialization-complete",
    }]
    async fn rack_initialization_complete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<RackPathParam>,
        info: TypedBody<RackInitializationRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/switch/{switch_id}",
    }]
    async fn switch_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SwitchPathParam>,
        body: TypedBody<SwitchPutRequest>,
    ) -> Result<HttpResponseOk<SwitchPutResponse>, HttpError>;

    /// Report updated state for an instance.
    #[endpoint {
        method = PUT,
        path = "/instances/{instance_id}",
    }]
    async fn cpapi_instances_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<InstancePathParam>,
        new_runtime_state: TypedBody<SledInstanceState>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Report updated state for a disk.
    #[endpoint {
        method = PUT,
        path = "/disks/{disk_id}",
    }]
    async fn cpapi_disks_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DiskPathParam>,
        new_runtime_state: TypedBody<DiskRuntimeState>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Request removal of a read_only_parent from a volume.
    ///
    /// A volume can be created with the source data for that volume being another
    /// volume that attached as a "read_only_parent". In the background there
    /// exists a scrubber that will copy the data from the read_only_parent
    /// into the volume. When that scrubber has completed copying the data, this
    /// endpoint can be called to update the database that the read_only_parent
    /// is no longer needed for a volume and future attachments of this volume
    /// should not include that read_only_parent.
    #[endpoint {
        method = POST,
        path = "/volume/{volume_id}/remove-read-only-parent",
    }]
    async fn cpapi_volume_remove_read_only_parent(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VolumePathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Request removal of a read_only_parent from a disk.
    ///
    /// This is a thin wrapper around the volume_remove_read_only_parent saga.
    /// All we are doing here is, given a disk UUID, figure out what the
    /// volume_id is for that disk, then use that to call the
    /// disk_remove_read_only_parent saga on it.
    #[endpoint {
        method = POST,
        path = "/disk/{disk_id}/remove-read-only-parent",
    }]
    async fn cpapi_disk_remove_read_only_parent(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DiskPathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Accept a registration from a new metric producer
    #[endpoint {
        method = POST,
        path = "/metrics/producers",
    }]
    async fn cpapi_producers_post(
        request_context: RequestContext<Self::Context>,
        producer_info: TypedBody<ProducerEndpoint>,
    ) -> Result<HttpResponseCreated<ProducerRegistrationResponse>, HttpError>;

    /// List all metric producers assigned to an oximeter collector.
    #[endpoint {
        method = GET,
        path = "/metrics/collectors/{collector_id}/producers",
    }]
    async fn cpapi_assigned_producers_list(
        request_context: RequestContext<Self::Context>,
        path_params: Path<CollectorIdPathParams>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<ProducerEndpoint>>, HttpError>;

    /// Accept a notification of a new oximeter collection server.
    #[endpoint {
        method = POST,
        path = "/metrics/collectors",
    }]
    async fn cpapi_collectors_post(
        request_context: RequestContext<Self::Context>,
        oximeter_info: TypedBody<OximeterInfo>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Endpoint used by Sled Agents to download cached artifacts.
    #[endpoint {
    method = GET,
    path = "/artifacts/{kind}/{name}/{version}",
    }]
    async fn cpapi_artifact_download(
        request_context: RequestContext<Self::Context>,
        path_params: Path<ArtifactId>,
    ) -> Result<HttpResponseOk<FreeformBody>, HttpError>;

    /// An Upstairs will notify this endpoint when a repair starts
    #[endpoint {
        method = POST,
        path = "/crucible/0/upstairs/{upstairs_id}/repair-start",
    }]
    async fn cpapi_upstairs_repair_start(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsPathParam>,
        repair_start_info: TypedBody<RepairStartInfo>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// An Upstairs will notify this endpoint when a repair finishes.
    #[endpoint {
        method = POST,
        path = "/crucible/0/upstairs/{upstairs_id}/repair-finish",
    }]
    async fn cpapi_upstairs_repair_finish(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsPathParam>,
        repair_finish_info: TypedBody<RepairFinishInfo>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// An Upstairs will update this endpoint with the progress of a repair
    #[endpoint {
        method = POST,
        path = "/crucible/0/upstairs/{upstairs_id}/repair/{repair_id}/progress",
    }]
    async fn cpapi_upstairs_repair_progress(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsRepairPathParam>,
        repair_progress: TypedBody<RepairProgress>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// An Upstairs will update this endpoint if a Downstairs client task is
    /// requested to stop
    #[endpoint {
        method = POST,
        path = "/crucible/0/upstairs/{upstairs_id}/downstairs/{downstairs_id}/stop-request",
    }]
    async fn cpapi_downstairs_client_stop_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsDownstairsPathParam>,
        downstairs_client_stop_request: TypedBody<DownstairsClientStopRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// An Upstairs will update this endpoint if a Downstairs client task stops for
    /// any reason (not just after being requested to)
    #[endpoint {
        method = POST,
        path = "/crucible/0/upstairs/{upstairs_id}/downstairs/{downstairs_id}/stopped",
    }]
    async fn cpapi_downstairs_client_stopped(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsDownstairsPathParam>,
        downstairs_client_stopped: TypedBody<DownstairsClientStopped>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    // Sagas

    /// List sagas
    #[endpoint {
        method = GET,
        path = "/sagas",
    }]
    async fn saga_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<Saga>>, HttpError>;

    /// Fetch a saga
    #[endpoint {
        method = GET,
        path = "/sagas/{saga_id}",
    }]
    async fn saga_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SagaPathParam>,
    ) -> Result<HttpResponseOk<Saga>, HttpError>;

    // Background Tasks

    /// List background tasks
    ///
    /// This is a list of discrete background activities that Nexus carries out.
    /// This is exposed for support and debugging.
    #[endpoint {
        method = GET,
        path = "/bgtasks",
    }]
    async fn bgtask_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BTreeMap<String, BackgroundTask>>, HttpError>;

    /// Fetch status of one background task
    ///
    /// This is exposed for support and debugging.
    #[endpoint {
        method = GET,
        path = "/bgtasks/view/{bgtask_name}",
    }]
    async fn bgtask_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<BackgroundTaskPathParam>,
    ) -> Result<HttpResponseOk<BackgroundTask>, HttpError>;

    /// Activates one or more background tasks, causing them to be run immediately
    /// if idle, or scheduled to run again as soon as possible if already running.
    #[endpoint {
        method = POST,
        path = "/bgtasks/activate",
    }]
    async fn bgtask_activate(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<BackgroundTasksActivateRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    // NAT RPW internal APIs

    /// Fetch NAT ChangeSet
    ///
    /// Caller provides their generation as `from_gen`, along with a query
    /// parameter for the page size (`limit`). Endpoint will return changes
    /// that have occured since the caller's generation number up to the latest
    /// change or until the `limit` is reached. If there are no changes, an
    /// empty vec is returned.
    #[endpoint {
        method = GET,
        path = "/nat/ipv4/changeset/{from_gen}"
    }]
    async fn ipv4_nat_changeset(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<RpwNatPathParam>,
        query_params: Query<RpwNatQueryParam>,
    ) -> Result<HttpResponseOk<Vec<Ipv4NatEntryView>>, HttpError>;

    // APIs for managing blueprints
    //
    // These are not (yet) intended for use by any other programs.  Eventually, we
    // will want this functionality part of the public API.  But we don't want to
    // commit to any of this yet.  These properly belong in an RFD 399-style
    // "Service and Support API".  Absent that, we stick them here.

    /// Lists blueprints
    #[endpoint {
        method = GET,
        path = "/deployment/blueprints/all",
    }]
    async fn blueprint_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<BlueprintMetadata>>, HttpError>;

    /// Fetches one blueprint
    #[endpoint {
        method = GET,
        path = "/deployment/blueprints/all/{blueprint_id}",
    }]
    async fn blueprint_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<nexus_types::external_api::params::BlueprintPath>,
    ) -> Result<HttpResponseOk<Blueprint>, HttpError>;

    /// Deletes one blueprint
    #[endpoint {
        method = DELETE,
        path = "/deployment/blueprints/all/{blueprint_id}",
    }]
    async fn blueprint_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<nexus_types::external_api::params::BlueprintPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // Managing the current target blueprint

    /// Fetches the current target blueprint, if any
    #[endpoint {
        method = GET,
        path = "/deployment/blueprints/target",
    }]
    async fn blueprint_target_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError>;

    /// Make the specified blueprint the new target
    #[endpoint {
        method = POST,
        path = "/deployment/blueprints/target",
    }]
    async fn blueprint_target_set(
        rqctx: RequestContext<Self::Context>,
        target: TypedBody<BlueprintTargetSet>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError>;

    /// Set the `enabled` field of the current target blueprint
    #[endpoint {
        method = PUT,
        path = "/deployment/blueprints/target/enabled",
    }]
    async fn blueprint_target_set_enabled(
        rqctx: RequestContext<Self::Context>,
        target: TypedBody<BlueprintTargetSet>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError>;

    // Generating blueprints

    /// Generates a new blueprint for the current system, re-evaluating anything
    /// that's changed since the last one was generated
    #[endpoint {
        method = POST,
        path = "/deployment/blueprints/regenerate",
    }]
    async fn blueprint_regenerate(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Blueprint>, HttpError>;

    /// Imports a client-provided blueprint
    ///
    /// This is intended for development and support, not end users or operators.
    #[endpoint {
        method = POST,
        path = "/deployment/blueprints/import",
    }]
    async fn blueprint_import(
        rqctx: RequestContext<Self::Context>,
        blueprint: TypedBody<Blueprint>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// List uninitialized sleds
    #[endpoint {
        method = GET,
        path = "/sleds/uninitialized",
    }]
    async fn sled_list_uninitialized(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ResultsPage<UninitializedSled>>, HttpError>;

    /// Add sled to initialized rack
    //
    // TODO: In the future this should really be a PUT request, once we resolve
    // https://github.com/oxidecomputer/omicron/issues/4494. It should also
    // explicitly be tied to a rack via a `rack_id` path param. For now we assume
    // we are only operating on single rack systems.
    #[endpoint {
        method = POST,
        path = "/sleds/add",
    }]
    async fn sled_add(
        rqctx: RequestContext<Self::Context>,
        sled: TypedBody<UninitializedSledId>,
    ) -> Result<HttpResponseCreated<SledId>, HttpError>;

    /// Mark a sled as expunged
    ///
    /// This is an irreversible process! It should only be called after
    /// sufficient warning to the operator.
    ///
    /// This is idempotent, and it returns the old policy of the sled.
    #[endpoint {
        method = POST,
        path = "/sleds/expunge",
    }]
    async fn sled_expunge(
        rqctx: RequestContext<Self::Context>,
        sled: TypedBody<SledSelector>,
    ) -> Result<HttpResponseOk<SledPolicy>, HttpError>;

    /// Mark a physical disk as expunged
    ///
    /// This is an irreversible process! It should only be called after
    /// sufficient warning to the operator.
    ///
    /// This is idempotent.
    #[endpoint {
        method = POST,
        path = "/physical-disk/expunge",
    }]
    async fn physical_disk_expunge(
        rqctx: RequestContext<Self::Context>,
        disk: TypedBody<PhysicalDiskPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get all the probes associated with a given sled.
    #[endpoint {
        method = GET,
        path = "/probes/{sled}"
    }]
    async fn probes_get(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ProbePathParam>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<Vec<ProbeInfo>>, HttpError>;
}

/// Path parameters for Sled Agent requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct SledAgentPathParam {
    pub sled_id: Uuid,
}

/// Path parameters for Disk requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct DiskPathParam {
    pub disk_id: Uuid,
}

/// Path parameters for Volume requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct VolumePathParam {
    pub volume_id: Uuid,
}

/// Path parameters for Rack requests.
#[derive(Deserialize, JsonSchema)]
pub struct RackPathParam {
    pub rack_id: Uuid,
}

/// Path parameters for Switch requests.
#[derive(Deserialize, JsonSchema)]
pub struct SwitchPathParam {
    pub switch_id: Uuid,
}

/// Path parameters for Instance requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct InstancePathParam {
    pub instance_id: Uuid,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CollectorIdPathParams {
    /// The ID of the oximeter collector.
    pub collector_id: Uuid,
}

/// Path parameters for Upstairs requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct UpstairsPathParam {
    pub upstairs_id: TypedUuid<UpstairsKind>,
}

/// Path parameters for Upstairs requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct UpstairsRepairPathParam {
    pub upstairs_id: TypedUuid<UpstairsKind>,
    pub repair_id: TypedUuid<UpstairsRepairKind>,
}

/// Path parameters for Downstairs requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct UpstairsDownstairsPathParam {
    pub upstairs_id: TypedUuid<UpstairsKind>,
    pub downstairs_id: TypedUuid<DownstairsKind>,
}

/// Path parameters for Saga requests
#[derive(Deserialize, JsonSchema)]
pub struct SagaPathParam {
    #[serde(rename = "saga_id")]
    pub saga_id: Uuid,
}

/// Path parameters for Background Task requests
#[derive(Deserialize, JsonSchema)]
pub struct BackgroundTaskPathParam {
    pub bgtask_name: String,
}

/// Query parameters for Background Task activation requests.
#[derive(Deserialize, JsonSchema)]
pub struct BackgroundTasksActivateRequest {
    pub bgtask_names: BTreeSet<String>,
}

/// Path parameters for NAT ChangeSet
#[derive(Deserialize, JsonSchema)]
pub struct RpwNatPathParam {
    /// which change number to start generating
    /// the change set from
    pub from_gen: i64,
}

/// Query parameters for NAT ChangeSet
#[derive(Deserialize, JsonSchema)]
pub struct RpwNatQueryParam {
    pub limit: u32,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct SledId {
    pub id: SledUuid,
}

/// Path parameters for probes
#[derive(Deserialize, JsonSchema)]
pub struct ProbePathParam {
    pub sled: Uuid,
}
