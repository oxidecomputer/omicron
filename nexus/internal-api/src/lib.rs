// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, BTreeSet};

use dropshot::{
    Body, Header, HttpError, HttpResponseCreated, HttpResponseDeleted,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path, Query, RequestContext,
    ResultsPage, TypedBody,
};
use http::Response;
use nexus_types::{
    deployment::{
        Blueprint, BlueprintMetadata, BlueprintTarget, BlueprintTargetSet,
        ClickhousePolicy, OximeterReadPolicy, ReconfiguratorConfigParam,
        ReconfiguratorConfigView,
    },
    external_api::{
        headers::RangeRequest,
        params::{self, PhysicalDiskPath, SledSelector, UninitializedSledId},
        shared::{self, ProbeInfo, UninitializedSled},
        views::{Ping, PingStatus, SledPolicy},
    },
    internal_api::{
        params::{
            InstanceMigrateRequest, OximeterInfo, RackInitializationRequest,
            SledAgentInfo, SwitchPutRequest, SwitchPutResponse,
        },
        views::{
            BackgroundTask, DemoSaga, MgsUpdateDriverStatus, NatEntryView,
            QuiesceStatus, Saga, UpdateStatus,
        },
    },
};
use omicron_common::api::{
    external::{
        Instance,
        http_pagination::{PaginatedById, PaginatedByTimeAndId},
    },
    internal::nexus::{
        DiskRuntimeState, DownstairsClientStopRequest, DownstairsClientStopped,
        ProducerEndpoint, ProducerRegistrationResponse, RepairFinishInfo,
        RepairProgress, RepairStartInfo, SledVmmState,
    },
};
use omicron_uuid_kinds::{
    DemoSagaUuid, DownstairsKind, PropolisUuid, SledUuid, TypedUuid,
    UpstairsKind, UpstairsRepairKind, VolumeUuid,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const RACK_INITIALIZATION_REQUEST_MAX_BYTES: usize = 10 * 1024 * 1024;

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

    /// Report that the Rack Setup Service initialization is complete
    ///
    /// See RFD 278 for more details.
    #[endpoint {
        method = PUT,
        path = "/racks/{rack_id}/initialization-complete",
        request_body_max_bytes = RACK_INITIALIZATION_REQUEST_MAX_BYTES,
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

    #[endpoint {
        method = POST,
        path = "/instances/{instance_id}/migrate",
    }]
    async fn instance_migrate(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<InstancePathParam>,
        migrate_params: TypedBody<InstanceMigrateRequest>,
    ) -> Result<HttpResponseOk<Instance>, HttpError>;

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

    // Debug interfaces for sagas

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

    /// Kick off an instance of the "demo" saga
    ///
    /// This saga is used for demo and testing.  The saga just waits until you
    /// complete using the `saga_demo_complete` API.
    #[endpoint {
        method = POST,
        path = "/demo-saga",
    }]
    async fn saga_demo_create(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<DemoSaga>, HttpError>;

    /// Complete a waiting demo saga
    ///
    /// Note that the id used here is not the same as the id of the saga.  It's
    /// the one returned by the `saga_demo_create` API.
    #[endpoint {
        method = POST,
        path = "/demo-saga/{demo_saga_id}/complete",
    }]
    async fn saga_demo_complete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DemoSagaPathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    // Debug interfaces for background Tasks

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

    // Debug interfaces for ongoing MGS updates

    /// Fetch information about ongoing MGS updates
    #[endpoint {
        method = GET,
        path = "/mgs-updates",
    }]
    async fn mgs_updates(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<MgsUpdateDriverStatus>, HttpError>;

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

    /// Get the current reconfigurator configuration
    #[endpoint {
        method = GET,
        path = "/deployment/reconfigurator-config"
    }]
    async fn reconfigurator_config_show_current(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ReconfiguratorConfigView>, HttpError>;

    /// Get the reconfigurator config at `version` if it exists
    #[endpoint {
        method = GET,
        path = "/deployment/reconfigurator-config/{version}"
    }]
    async fn reconfigurator_config_show(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VersionPathParam>,
    ) -> Result<HttpResponseOk<ReconfiguratorConfigView>, HttpError>;

    /// Update the reconfigurator config at the latest versions
    #[endpoint {
        method = POST,
        path = "/deployment/reconfigurator-config"
    }]
    async fn reconfigurator_config_set(
        rqctx: RequestContext<Self::Context>,
        switches: TypedBody<ReconfiguratorConfigParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Show deployed versions of artifacts
    #[endpoint {
        method = GET,
        path = "/deployment/update-status"
    }]
    async fn update_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<UpdateStatus>, HttpError>;

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

    // Support bundles (experimental)

    /// List all support bundles
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles",
    }]
    async fn support_bundle_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByTimeAndId>,
    ) -> Result<HttpResponseOk<ResultsPage<shared::SupportBundleInfo>>, HttpError>;

    /// View a support bundle
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles/{bundle_id}",
    }]
    async fn support_bundle_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<HttpResponseOk<shared::SupportBundleInfo>, HttpError>;

    /// Download the index of a support bundle
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/index",
    }]
    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Download the contents of a support bundle
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/download",
    }]
    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Download a file within a support bundle
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/download/{file}",
    }]
    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<params::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Download the metadata of a support bundle
    #[endpoint {
        method = HEAD,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/download",
    }]
    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Download the metadata of a file within the support bundle
    #[endpoint {
        method = HEAD,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/download/{file}",
    }]
    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<params::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Create a new support bundle
    #[endpoint {
        method = POST,
        path = "/experimental/v1/system/support-bundles",
    }]
    async fn support_bundle_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<params::SupportBundleCreate>,
    ) -> Result<HttpResponseCreated<shared::SupportBundleInfo>, HttpError>;

    /// Delete an existing support bundle
    ///
    /// May also be used to cancel a support bundle which is currently being
    /// collected, or to remove metadata for a support bundle that has failed.
    #[endpoint {
        method = DELETE,
        path = "/experimental/v1/system/support-bundles/{bundle_id}",
    }]
    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Update a support bundle
    #[endpoint {
        method = PUT,
        path = "/experimental/v1/system/support-bundles/{bundle_id}",
    }]
    async fn support_bundle_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SupportBundlePath>,
        body: TypedBody<params::SupportBundleUpdate>,
    ) -> Result<HttpResponseOk<shared::SupportBundleInfo>, HttpError>;

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

    /// Get the current clickhouse policy
    #[endpoint {
     method = GET,
     path = "/clickhouse/policy"
     }]
    async fn clickhouse_policy_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhousePolicy>, HttpError>;

    /// Set the new clickhouse policy
    #[endpoint {
        method = POST,
        path = "/clickhouse/policy"
    }]
    async fn clickhouse_policy_set(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<ClickhousePolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get the current oximeter read policy
    #[endpoint {
            method = GET,
            path = "/oximeter/read-policy"
            }]
    async fn oximeter_read_policy_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<OximeterReadPolicy>, HttpError>;

    /// Set the new oximeter read policy
    #[endpoint {
               method = POST,
               path = "/oximeter/read-policy"
           }]
    async fn oximeter_read_policy_set(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<OximeterReadPolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Begin quiescing this Nexus instance
    ///
    /// This causes no new sagas to be started and eventually causes no database
    /// connections to become available.  This is a one-way trip.  There's no
    /// unquiescing Nexus.
    #[endpoint {
        method = POST,
        path = "/quiesce"
    }]
    async fn quiesce_start(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Check whether Nexus is running normally, quiescing, or fully quiesced.
    #[endpoint {
        method = GET,
        path = "/quiesce"
    }]
    async fn quiesce_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<QuiesceStatus>, HttpError>;
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

/// Path parameters for Saga requests
#[derive(Deserialize, JsonSchema)]
pub struct SagaPathParam {
    #[serde(rename = "saga_id")]
    pub saga_id: Uuid,
}

/// Path parameters for DemoSaga requests
#[derive(Deserialize, JsonSchema)]
pub struct DemoSagaPathParam {
    pub demo_saga_id: DemoSagaUuid,
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

#[derive(Deserialize, JsonSchema)]
pub struct VersionPathParam {
    pub version: u32,
}
