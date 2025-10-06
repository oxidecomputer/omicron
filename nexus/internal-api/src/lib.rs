// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeSet;

use dropshot::{
    HttpError, HttpResponseCreated, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, Query, RequestContext, ResultsPage,
    TypedBody,
};
use nexus_types::{
    external_api::{
        shared::ProbeInfo,
        views::{Ping, PingStatus},
    },
    internal_api::{
        params::{
            OximeterInfo, SledAgentInfo, SwitchPutRequest, SwitchPutResponse,
        },
        views::NatEntryView,
    },
};
use omicron_common::api::{
    external::http_pagination::PaginatedById,
    internal::nexus::{
        DiskRuntimeState, DownstairsClientStopRequest, DownstairsClientStopped,
        ProducerEndpoint, ProducerRegistrationResponse, RepairFinishInfo,
        RepairProgress, RepairStartInfo, SledVmmState,
    },
};
use omicron_uuid_kinds::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[dropshot::api_description]
pub trait NexusInternalApi {
    type Context;

    /// Ping API
    ///
    /// Always responds with Ok if it responds at all.
    #[endpoint {
        method = GET,
        path = "/v1/ping",
    }]
    async fn ping(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Ping>, HttpError> {
        Ok(HttpResponseOk(Ping { status: PingStatus::Ok }))
    }

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

    #[endpoint {
        method = PUT,
        path = "/switch/{switch_id}",
    }]
    async fn switch_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SwitchPathParam>,
        body: TypedBody<SwitchPutRequest>,
    ) -> Result<HttpResponseOk<SwitchPutResponse>, HttpError>;

    /// Report updated state for a VMM.
    #[endpoint {
        method = PUT,
        path = "/vmms/{propolis_id}",
    }]
    async fn cpapi_instances_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        new_runtime_state: TypedBody<SledVmmState>,
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

    /// **Do not use in new code!**
    ///
    /// Callers to this API should either be capable of using the nexus-lockstep
    /// API or should be rewritten to use a doorbell API to activate a specific
    /// task. Task names are internal to Nexus.
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
    ) -> Result<HttpResponseOk<Vec<NatEntryView>>, HttpError>;

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
    pub sled_id: SledUuid,
}

/// Path parameters for Disk requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct DiskPathParam {
    pub disk_id: Uuid,
}

/// Path parameters for Volume requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct VolumePathParam {
    pub volume_id: VolumeUuid,
}

/// Path parameters for Switch requests.
#[derive(Deserialize, JsonSchema)]
pub struct SwitchPathParam {
    pub switch_id: Uuid,
}

/// Path parameters for VMM requests (internal API)
#[derive(Deserialize, JsonSchema)]
pub struct VmmPathParam {
    pub propolis_id: PropolisUuid,
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
    #[schemars(with = "Uuid")]
    pub sled: SledUuid,
}
