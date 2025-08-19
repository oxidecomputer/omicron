// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP API for the gateway service.

use dropshot::{
    HttpError, HttpResponseOk, HttpResponseUpdatedNoContent, Path, Query,
    RequestContext, TypedBody, UntypedBody, WebsocketEndpointResult,
    WebsocketUpgrade,
};
use gateway_types::{
    caboose::SpComponentCaboose,
    component::{
        PowerState, SpComponentFirmwareSlot, SpComponentList, SpIdentifier,
        SpState,
    },
    component_details::SpComponentDetails,
    host::{ComponentFirmwareHashStatus, HostStartupOptions},
    ignition::{IgnitionCommand, SpIgnitionInfo},
    rot::{RotCfpa, RotCfpaSlot, RotCmpa, RotState},
    sensor::SpSensorReading,
    task_dump::TaskDump,
    update::{
        HostPhase2Progress, HostPhase2RecoveryImageId, InstallinatorImageId,
        SpComponentResetError, SpUpdateStatus,
    },
};
use openapi_manager_types::{
    SupportedVersion, SupportedVersions, api_versions,
};
use schemars::JsonSchema;
use serde::Deserialize;
use uuid::Uuid;

api_versions!([
    // WHEN CHANGING THE API (part 1 of 2):
    //
    // +- Pick a new semver and define it in the list below.  The list MUST
    // |  remain sorted, which generally means that your version should go at
    // |  the very top.
    // |
    // |  Duplicate this line, uncomment the *second* copy, update that copy for
    // |  your new API version, and leave the first copy commented out as an
    // |  example for the next person.
    // v
    // (next_int, IDENT),
    (1, INITIAL),
]);

// WHEN CHANGING THE API (part 2 of 2):
//
// The call to `api_versions!` above defines constants of type
// `semver::Version` that you can use in your Dropshot API definition to specify
// the version when a particular endpoint was added or removed.  For example, if
// you used:
//
//     (2, ADD_FOOBAR)
//
// Then you could use `VERSION_ADD_FOOBAR` as the version in which endpoints
// were added or removed.

/// This endpoint is used to upload SP and ROT Hubris archives as well as phase 1 host OS
/// images. The phase 1 image is 32 MiB, driven by the QSPI flash on hardware.
const SP_COMPONENT_UPDATE_MAX_BYTES: usize = 64 * 1024 * 1024;
/// The host phase 2 recovery image is currently (Dec 2024) ~130 MiB.
const HOST_PHASE2_MAX_BYTES: usize = 512 * 1024 * 1024;

#[dropshot::api_description]
pub trait GatewayApi {
    type Context;

    /// Get info on an SP
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}",
    }]
    async fn sp_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<SpState>, HttpError>;

    /// Get host startup options for a sled
    ///
    /// This endpoint will currently fail for any `SpType` other than
    /// `SpType::Sled`.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/startup-options",
    }]
    async fn sp_startup_options_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<HostStartupOptions>, HttpError>;

    /// Set host startup options for a sled
    ///
    /// This endpoint will currently fail for any `SpType` other than
    /// `SpType::Sled`.
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/startup-options",
    }]
    async fn sp_startup_options_set(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
        body: TypedBody<HostStartupOptions>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Read the current value of a sensor by ID
    ///
    /// Sensor IDs come from the host topo tree.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/sensor/{sensor_id}/value",
    }]
    async fn sp_sensor_read_value(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpSensorId>,
    ) -> Result<HttpResponseOk<SpSensorReading>, HttpError>;

    /// List components of an SP
    ///
    /// A component is a distinct entity under an SP's direct control. This
    /// lists all those components for a given SP.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component",
    }]
    async fn sp_component_list(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<SpComponentList>, HttpError>;

    /// Get info for an SP component
    ///
    /// This can be useful, for example, to poll the state of a component if
    /// another interface has changed the power state of a component or updated
    /// a component.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}",
    }]
    async fn sp_component_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseOk<Vec<SpComponentDetails>>, HttpError>;

    /// Get the caboose of an SP component
    ///
    /// Not all components have a caboose.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}/caboose",
    }]
    async fn sp_component_caboose_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        query_params: Query<ComponentCabooseSlot>,
    ) -> Result<HttpResponseOk<SpComponentCaboose>, HttpError>;

    /// Clear status of a component
    ///
    /// For components that maintain event counters (e.g., the sidecar
    /// `monorail`), this will reset the event counters to zero.
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/component/{component}/clear-status",
    }]
    async fn sp_component_clear_status(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get the currently-active slot for an SP component
    ///
    /// Note that the meaning of "current" in "currently-active" may vary
    /// depending on the component: for example, it may mean "the
    /// actively-running slot" or "the slot that will become active the next
    /// time the component is booted".
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}/active-slot",
    }]
    async fn sp_component_active_slot_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseOk<SpComponentFirmwareSlot>, HttpError>;

    /// Set the currently-active slot for an SP component
    ///
    /// Note that the meaning of "current" in "currently-active" may vary
    /// depending on the component: for example, it may mean "the
    /// actively-running slot" or "the slot that will become active the next
    /// time the component is booted".
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/component/{component}/active-slot",
    }]
    async fn sp_component_active_slot_set(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        query_params: Query<SetComponentActiveSlotParams>,
        body: TypedBody<SpComponentFirmwareSlot>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Upgrade into a websocket connection attached to the given SP
    /// component's serial console.
    //
    // # Notes
    //
    // This is a websocket endpoint; normally we'd expect to use
    // `dropshot::channel` with `protocol = WEBSOCKETS` instead of
    // `dropshot::endpoint`, but `dropshot::channel` doesn't allow us to return
    // an error _before_ upgrading the connection, and we want to bail out ASAP
    // if the SP doesn't allow us to attach.
    //
    // Therefore, we use `dropshot::endpoint` with the special argument type
    // `WebsocketUpgrade`: this inserts the correct marker for progenitor to
    // know this is a websocket endpoint, and it allows us to call
    // `WebsocketUpgrade::handle()` (the method `dropshot::channel` would call
    // for us to upgrade the connection) by hand after our error checking.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}/serial-console/attach",
    }]
    async fn sp_component_serial_console_attach(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        websocket: WebsocketUpgrade,
    ) -> WebsocketEndpointResult;

    /// Detach the websocket connection attached to the given SP component's
    /// serial console, if such a connection exists.
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/component/{component}/serial-console/detach",
    }]
    async fn sp_component_serial_console_detach(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Reset an SP component (possibly the SP itself).
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/component/{component}/reset",
    }]
    async fn sp_component_reset(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseUpdatedNoContent, SpComponentResetError>;

    /// Update an SP component
    ///
    /// Update a component of an SP according to its specific update mechanism.
    /// This interface is generic for all component types, but resolves to a
    /// mechanism specific to the given component type. This may fail for a
    /// variety of reasons including the update bundle being invalid or
    /// improperly specified or due to an error originating from the SP itself.
    ///
    /// Note that not all components may be updated; components without known
    /// update mechanisms will return an error without any inspection of the
    /// update bundle.
    ///
    /// Updating the SP itself is done via the component name `sp`.
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/component/{component}/update",
        request_body_max_bytes = SP_COMPONENT_UPDATE_MAX_BYTES,
    }]
    async fn sp_component_update(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        query_params: Query<ComponentUpdateIdSlot>,
        body: UntypedBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get the status of an update being applied to an SP component
    ///
    /// Getting the status of an update to the SP itself is done via the
    /// component name `sp`.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}/update-status",
    }]
    async fn sp_component_update_status(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseOk<SpUpdateStatus>, HttpError>;

    /// Start computing the hash of a given slot of a component.
    ///
    /// This endpoint is only valid for the `host-boot-flash` component.
    ///
    /// Computing the hash takes several seconds; callers should poll for results
    /// using `sp_component_hash_firmware_get()`. In general they should call
    /// `sp_component_hash_firmware_get()` first anyway, as the hashes are
    /// cached in the SP and may already be ready.
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/component/{component}/hash/{firmware_slot}",
    }]
    async fn sp_component_hash_firmware_start(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponentFirmwareSlot>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get a computed hash of a given slot of a component.
    ///
    /// This endpoint is only valid for the `host-boot-flash` component.
    ///
    /// Computing the hash takes several seconds; this endpoint returns the
    /// current status. If the status is `HashNotStarted`, callers should start
    /// hashing using `sp_component_hash_firmware_start()`. If the status is
    /// `HashInProgress`, callers should wait a bit then call this endpoint
    /// again.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}/hash/{firmware_slot}",
    }]
    async fn sp_component_hash_firmware_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponentFirmwareSlot>,
    ) -> Result<HttpResponseOk<ComponentFirmwareHashStatus>, HttpError>;

    /// Abort any in-progress update an SP component
    ///
    /// Aborting an update to the SP itself is done via the component name
    /// `sp`.
    ///
    /// On a successful return, the update corresponding to the given UUID will
    /// no longer be in progress (either aborted or applied).
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/component/{component}/update-abort",
    }]
    async fn sp_component_update_abort(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        body: TypedBody<UpdateAbortBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Read the CMPA from a root of trust.
    ///
    /// This endpoint is only valid for the `rot` component.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}/cmpa",
    }]
    async fn sp_rot_cmpa_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseOk<RotCmpa>, HttpError>;

    /// Read the requested CFPA slot from a root of trust.
    ///
    /// This endpoint is only valid for the `rot` component.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}/cfpa",
    }]
    async fn sp_rot_cfpa_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        params: TypedBody<GetCfpaParams>,
    ) -> Result<HttpResponseOk<RotCfpa>, HttpError>;

    /// Read the RoT boot state from a root of trust
    ///
    /// This endpoint is only valid for the `rot` component.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/component/{component}/rot-boot-info",
    }]
    async fn sp_rot_boot_info(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        params: TypedBody<GetRotBootInfoParams>,
    ) -> Result<HttpResponseOk<RotState>, HttpError>;

    /// Get the number of task dumps present on an SP
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/task-dump",
    }]
    async fn sp_task_dump_count(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<u32>, HttpError>;

    /// Read a single task dump from an SP
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/task-dump/{task_dump_index}",
    }]
    async fn sp_task_dump_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpTaskDumpIndex>,
    ) -> Result<HttpResponseOk<TaskDump>, HttpError>;

    /// List SPs via Ignition
    ///
    /// Retreive information for all SPs via the Ignition controller. This is
    /// lower latency and has fewer possible failure modes than querying the SP
    /// over the management network.
    #[endpoint {
        method = GET,
        path = "/ignition",
    }]
    async fn ignition_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SpIgnitionInfo>>, HttpError>;

    /// Get SP info via Ignition
    ///
    /// Retreive information for an SP via the Ignition controller. This is
    /// lower latency and has fewer possible failure modes than querying the SP
    /// over the management network.
    #[endpoint {
        method = GET,
        path = "/ignition/{type}/{slot}",
    }]
    async fn ignition_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<SpIgnitionInfo>, HttpError>;

    /// Send an ignition command targeting a specific SP.
    ///
    /// This endpoint can be used to transition a target between A2 and A3 (via
    /// power-on / power-off) or reset it.
    ///
    /// The management network traffic caused by requests to this endpoint is
    /// between this MGS instance and its local ignition controller, _not_ the
    /// SP targeted by the command.
    #[endpoint {
        method = POST,
        path = "/ignition/{type}/{slot}/{command}",
    }]
    async fn ignition_command(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpIgnitionCommand>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get the current power state of a sled via its SP.
    ///
    /// Note that if the sled is in A3, the SP is powered off and will not be able
    /// to respond; use the ignition control endpoints for those cases.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/power-state",
    }]
    async fn sp_power_state_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<PowerState>, HttpError>;

    /// Set the current power state of a sled via its SP.
    ///
    /// Note that if the sled is in A3, the SP is powered off and will not be
    /// able to respond; use the ignition control endpoints for those cases.
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/power-state",
    }]
    async fn sp_power_state_set(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
        body: TypedBody<PowerState>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Set the installinator image ID the sled should use for recovery.
    ///
    /// This value can be read by the host via IPCC; see the `ipcc` crate.
    #[endpoint {
        method = PUT,
        path = "/sp/{type}/{slot}/ipcc/installinator-image-id",
    }]
    async fn sp_installinator_image_id_set(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
        body: TypedBody<InstallinatorImageId>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Clear any previously-set installinator image ID on the target sled.
    #[endpoint {
        method = DELETE,
        path = "/sp/{type}/{slot}/ipcc/installinator-image-id",
    }]
    async fn sp_installinator_image_id_delete(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get the most recent host phase2 request we've seen from the target SP.
    ///
    /// This method can be used as an indirect progress report for how far along a
    /// host is when it is booting via the MGS -> SP -> UART recovery path. This
    /// path is used to install the trampoline image containing installinator to
    /// recover a sled.
    #[endpoint {
        method = GET,
        path = "/sp/{type}/{slot}/host-phase2-progress",
    }]
    async fn sp_host_phase2_progress_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<HostPhase2Progress>, HttpError>;

    /// Clear the most recent host phase2 request we've seen from the target SP.
    #[endpoint {
        method = DELETE,
        path = "/sp/{type}/{slot}/host-phase2-progress",
    }]
    async fn sp_host_phase2_progress_delete(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Upload a host phase2 image that can be served to recovering hosts via the
    /// host/SP control uart.
    ///
    /// MGS caches this image in memory and is limited to a small, fixed number of
    /// images (potentially 1). Uploading a new image may evict the
    /// least-recently-requested image if our cache is already full.
    #[endpoint {
        method = POST,
        path = "/recovery/host-phase2",
        request_body_max_bytes = HOST_PHASE2_MAX_BYTES,
    }]
    async fn recovery_host_phase2_upload(
        rqctx: RequestContext<Self::Context>,
        body: UntypedBody,
    ) -> Result<HttpResponseOk<HostPhase2RecoveryImageId>, HttpError>;

    /// Get the identifier for the switch this MGS instance is connected to.
    ///
    /// Note that most MGS endpoints behave identically regardless of which
    /// scrimlet the MGS instance is running on; this one, however, is
    /// intentionally different. This endpoint is _probably_ only useful for
    /// clients communicating with MGS over localhost (i.e., other services in
    /// the switch zone) who need to know which sidecar they are connected to.
    #[endpoint {
        method = GET,
        path = "/local/switch-id",
    }]
    async fn sp_local_switch_id(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SpIdentifier>, HttpError>;

    /// Get the complete list of SP identifiers this MGS instance is configured to
    /// find and communicate with.
    ///
    /// Note that unlike most MGS endpoints, this endpoint does not send any
    /// communication on the management network.
    #[endpoint {
        method = GET,
        path = "/local/all-sp-ids",
    }]
    async fn sp_all_ids(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SpIdentifier>>, HttpError>;

    /// Request ereports from the target service processor.
    ///
    /// Query parameters provide the ENAs of the initial ereport in the returned
    /// tranche, the SP's restart ID that the control plane believes is current,
    /// and the (optional) last committed ENA.
    #[endpoint {
        method = POST,
        path = "/sp/{type}/{slot}/ereports"
    }]
    async fn sp_ereports_ingest(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
        query: Query<ereport_types::EreportQuery>,
    ) -> Result<HttpResponseOk<ereport_types::Ereports>, HttpError>;
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSp {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpSensorId {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// ID for the sensor on the SP.
    pub sensor_id: u32,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpComponent {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// ID for the component of the SP; this is the internal identifier used by
    /// the SP itself to identify its components.
    pub component: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpComponentFirmwareSlot {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// ID for the component of the SP; this is the internal identifier used by
    /// the SP itself to identify its components.
    pub component: String,
    /// Firmware slot of the component.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpTaskDumpIndex {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// The index of the task dump to be read.
    pub task_dump_index: u32,
}

#[derive(Deserialize, JsonSchema)]
pub struct ComponentCabooseSlot {
    /// The firmware slot to for which we want to request caboose information.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct SetComponentActiveSlotParams {
    /// Persist this choice of active slot.
    pub persist: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct ComponentUpdateIdSlot {
    /// An identifier for this update.
    ///
    /// This ID applies to this single instance of the API call; it is not an
    /// ID of `image` itself. Multiple API calls with the same `image` should
    /// use different IDs.
    pub id: Uuid,
    /// The update slot to apply this image to. Supply 0 if the component only
    /// has one update slot.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct UpdateAbortBody {
    /// The ID of the update to abort.
    ///
    /// If the SP is currently receiving an update with this ID, it will be
    /// aborted.
    ///
    /// If the SP is currently receiving an update with a different ID, the
    /// abort request will fail.
    ///
    /// If the SP is not currently receiving any update, the request to abort
    /// should succeed but will not have actually done anything.
    pub id: Uuid,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema,
)]
pub struct GetCfpaParams {
    pub slot: RotCfpaSlot,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema,
)]
pub struct GetRotBootInfoParams {
    pub version: u8,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpIgnitionCommand {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// Ignition command to perform on the targeted SP.
    pub command: IgnitionCommand,
}
