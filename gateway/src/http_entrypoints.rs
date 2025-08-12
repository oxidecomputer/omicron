// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! HTTP entrypoint functions for the gateway service

use crate::ServerContext;
use crate::error::SpCommsError;
use crate::http_err_with_message;
use base64::Engine;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::TypedBody;
use dropshot::UntypedBody;
use dropshot::WebsocketEndpointResult;
use dropshot::WebsocketUpgrade;
use futures::TryFutureExt;
use gateway_api::*;
use gateway_messages::HfError;
use gateway_messages::RotBootInfo;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_sp_comms::HostPhase2Provider;
use gateway_sp_comms::VersionedSpState;
use gateway_sp_comms::error::CommunicationError;
use gateway_types::caboose::SpComponentCaboose;
use gateway_types::component::PowerState;
use gateway_types::component::SpComponentFirmwareSlot;
use gateway_types::component::SpComponentInfo;
use gateway_types::component::SpComponentList;
use gateway_types::component::SpIdentifier;
use gateway_types::component::SpState;
use gateway_types::component_details::SpComponentDetails;
use gateway_types::host::ComponentFirmwareHashStatus;
use gateway_types::host::HostStartupOptions;
use gateway_types::ignition::SpIgnitionInfo;
use gateway_types::rot::RotCfpa;
use gateway_types::rot::RotCfpaSlot;
use gateway_types::rot::RotCmpa;
use gateway_types::rot::RotState;
use gateway_types::sensor::SpSensorReading;
use gateway_types::task_dump::TaskDump;
use gateway_types::update::HostPhase2Progress;
use gateway_types::update::HostPhase2RecoveryImageId;
use gateway_types::update::InstallinatorImageId;
use gateway_types::update::SpUpdateStatus;
use omicron_uuid_kinds::GenericUuid;
use std::io::Cursor;
use std::num::NonZeroU8;
use std::str;
use std::sync::Arc;
use tufaceous_artifact::ArtifactHash;

// TODO
// The gateway service will get asynchronous notifications both from directly
// SPs over the management network and indirectly from Ignition via the Sidecar
// SP.
// TODO The Ignition controller will send an interrupt to its local SP. Will
// that SP then notify both gateway services or just its local gateway service?
// Both Ignition controller should both do the same thing at about the same
// time so is there a real benefit to them both sending messages to both
// gateways? This would cause a single message to effectively be replicated 4x
// (Nexus would need to dedup these).

type GatewayApiDescription = ApiDescription<Arc<ServerContext>>;

/// Returns a description of the gateway API
pub fn api() -> GatewayApiDescription {
    gateway_api_mod::api_description::<GatewayImpl>()
        .expect("entrypoints registered successfully")
}
enum GatewayImpl {}

impl GatewayApi for GatewayImpl {
    type Context = Arc<ServerContext>;

    /// Get info on an SP
    async fn sp_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<SpState>, HttpError> {
        let apictx = rqctx.context();
        let sp_id = path.into_inner().sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;

            let state = sp.state().await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            let rot_state = sp
                .rot_state(gateway_messages::RotBootInfo::HIGHEST_KNOWN_VERSION)
                .await;

            let final_state = sp_state_from_comms(state, rot_state);

            Ok(HttpResponseOk(final_state))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_startup_options_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<HostStartupOptions>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let mgmt_switch = &apictx.mgmt_switch;
            let sp_id = path.into_inner().sp.into();
            let sp = mgmt_switch.sp(sp_id)?;

            let options = sp.get_startup_options().await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseOk(options.into()))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_startup_options_set(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
        body: TypedBody<HostStartupOptions>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let mgmt_switch = &apictx.mgmt_switch;
        let sp_id = path.into_inner().sp.into();
        let handler = async {
            let sp = mgmt_switch.sp(sp_id)?;

            sp.set_startup_options(body.into_inner().into()).await.map_err(
                |err| SpCommsError::SpCommunicationFailed { sp: sp_id, err },
            )?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_sensor_read_value(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpSensorId>,
    ) -> Result<HttpResponseOk<SpSensorReading>, HttpError> {
        let apictx = rqctx.context();
        let PathSpSensorId { sp, sensor_id } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let value =
                sp.read_sensor_value(sensor_id).await.map_err(|err| {
                    SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                })?;

            Ok(HttpResponseOk(value.into()))
        };

        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_list(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<SpComponentList>, HttpError> {
        let apictx = rqctx.context();
        let sp_id = path.into_inner().sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let inventory = sp.inventory().await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseOk(sp_component_list_from_comms(inventory)))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseOk<Vec<SpComponentDetails>>, HttpError> {
        let apictx = rqctx.context();
        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;

            let details =
                sp.component_details(component).await.map_err(|err| {
                    SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                })?;

            Ok(HttpResponseOk(
                details.entries.into_iter().map(Into::into).collect(),
            ))
        };

        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    // Implementation notes:
    //
    // 1. As of the time of this comment, the cannonical keys written to the hubris
    //    caboose are defined in https://github.com/oxidecomputer/hubtools; see
    //    `write_default_caboose()`.
    // 2. We currently assume that the caboose always includes the same set of
    //    fields regardless of the component (e.g., the SP and RoT caboose have the
    //    same fields). If that becomes untrue, we may need to split this endpoint
    //    up to allow differently-typed responses.
    async fn sp_component_caboose_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        query_params: Query<ComponentCabooseSlot>,
    ) -> Result<HttpResponseOk<SpComponentCaboose>, HttpError> {
        const CABOOSE_KEY_GIT_COMMIT: [u8; 4] = *b"GITC";
        const CABOOSE_KEY_BOARD: [u8; 4] = *b"BORD";
        const CABOOSE_KEY_NAME: [u8; 4] = *b"NAME";
        const CABOOSE_KEY_VERSION: [u8; 4] = *b"VERS";
        const CABOOSE_KEY_SIGN: [u8; 4] = *b"SIGN";
        const CABOOSE_KEY_EPOC: [u8; 4] = *b"EPOC";

        let apictx = rqctx.context();
        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();

        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let ComponentCabooseSlot { firmware_slot } =
                query_params.into_inner();
            let component = component_from_str(&component)?;

            let from_utf8 = |key: &[u8], bytes| {
                // This helper closure is only called with the ascii-printable [u8; 4]
                // key constants we define above, so we can unwrap this conversion.
                let key = str::from_utf8(key).unwrap();
                String::from_utf8(bytes).map_err(|_| {
                    http_err_with_message(
                        dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                        "InvalidCaboose",
                        format!("non-utf8 data returned for caboose key {key}"),
                    )
                })
            };

            let git_commit =
                sp.read_component_caboose(
                    component,
                    firmware_slot,
                    CABOOSE_KEY_GIT_COMMIT,
                )
                .await
                .map_err(|err| {
                    SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                })?;
            let board =
                sp.read_component_caboose(
                    component,
                    firmware_slot,
                    CABOOSE_KEY_BOARD,
                )
                .await
                .map_err(|err| {
                    SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                })?;
            let name =
                sp.read_component_caboose(
                    component,
                    firmware_slot,
                    CABOOSE_KEY_NAME,
                )
                .await
                .map_err(|err| {
                    SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                })?;
            let version =
                sp.read_component_caboose(
                    component,
                    firmware_slot,
                    CABOOSE_KEY_VERSION,
                )
                .await
                .map_err(|err| {
                    SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                })?;

            let git_commit = from_utf8(&CABOOSE_KEY_GIT_COMMIT, git_commit)?;
            let board = from_utf8(&CABOOSE_KEY_BOARD, board)?;
            let name = from_utf8(&CABOOSE_KEY_NAME, name)?;
            let version = from_utf8(&CABOOSE_KEY_VERSION, version)?;

            // Not all images include the SIGN or EPOC in the caboose, if it's not present
            // don't treat it as an error

            let sign = match sp
                .read_component_caboose(
                    component,
                    firmware_slot,
                    CABOOSE_KEY_SIGN,
                )
                .await
                .ok()
            {
                None => None,
                Some(v) => Some(from_utf8(&CABOOSE_KEY_SIGN, v)?),
            };

            let epoch = match sp
                .read_component_caboose(
                    component,
                    firmware_slot,
                    CABOOSE_KEY_EPOC,
                )
                .await
                .ok()
            {
                None => None,
                Some(v) => Some(from_utf8(&CABOOSE_KEY_EPOC, v)?),
            };

            let caboose = SpComponentCaboose {
                git_commit,
                board,
                name,
                version,
                sign,
                epoch,
            };

            Ok(HttpResponseOk(caboose))
        };

        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_clear_status(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;

            sp.component_clear_status(component).await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseUpdatedNoContent {})
        };

        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_active_slot_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseOk<SpComponentFirmwareSlot>, HttpError> {
        let apictx = rqctx.context();
        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;

            let slot =
                sp.component_active_slot(component).await.map_err(|err| {
                    SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                })?;

            Ok(HttpResponseOk(SpComponentFirmwareSlot { slot }))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_active_slot_set(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        query_params: Query<SetComponentActiveSlotParams>,
        body: TypedBody<SpComponentFirmwareSlot>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;
            let slot = body.into_inner().slot;
            let persist = query_params.into_inner().persist;

            sp.set_component_active_slot(component, slot, persist)
                .await
                .map_err(|err| SpCommsError::SpCommunicationFailed {
                    sp: sp_id,
                    err,
                })?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_serial_console_attach(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        websocket: WebsocketUpgrade,
    ) -> WebsocketEndpointResult {
        // TODO(eliza): I'm not sure whether there's a way to make
        // `oximeter_instruments`'s HTTP latency tracker work with websockets
        // requests? It would be nice to get the latency and any error returned
        // prior to actually returning the websocket stream...
        let apictx = rqctx.context();
        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let component = component_from_str(&component)?;

        // Ensure we can attach to this SP's serial console.
        let console = apictx
            .mgmt_switch
            .sp(sp_id)?
            .serial_console_attach(component)
            .await
            .map_err(|err| SpCommsError::SpCommunicationFailed {
                sp: sp_id,
                err,
            })?;

        let log = apictx.log.new(slog::o!("sp" => format!("{sp:?}")));

        // We've successfully attached to the SP's serial console: upgrade the
        // websocket and run our side of that connection.
        websocket.handle(move |conn| {
            crate::serial_console::run(sp_id, console, conn, log)
        })
    }

    async fn sp_component_serial_console_detach(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();

        // TODO-cleanup: "component" support for the serial console is half baked;
        // we don't use it at all to detach.
        let PathSpComponent { sp, component: _ } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            sp.serial_console_detach().await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_reset(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;

            // Our config may specifically disallow resetting the SP of our
            // local sled. This is because resetting our local SP after an
            // update doesn't work: `reset_component_trigger` below will issue a
            // "reset with watchdog", wait for the SP to come back, then send a
            // "disarm the watchdog" message. But if we've reset our own local
            // sled, we won't be alive to disarm the watchdog, which will result
            // in the SP (erroneously) rolling back the update. (In production
            // we always disable this; it's a config option to allow for
            // dev/test environments that don't need this.)
            if component == SpComponent::SP_ITSELF
                && !apictx.mgmt_switch.allowed_to_reset_sp(sp_id)?
            {
                return Err(HttpError::for_bad_request(
                    None,
                    "MGS will not reset its own sled's SP".to_string(),
                ));
            }

            sp.reset_component_prepare(component)
                // We always want to run with the watchdog when resetting as
                // disabling the watchdog should be considered a debug only
                // feature
                .and_then(|()| sp.reset_component_trigger(component, false))
                .await
                .map_err(|err| SpCommsError::SpCommunicationFailed {
                    sp: sp_id,
                    err,
                })?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_update(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        query_params: Query<ComponentUpdateIdSlot>,
        body: UntypedBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();

        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;
            let ComponentUpdateIdSlot { id, firmware_slot } =
                query_params.into_inner();

            // TODO-performance: this makes a full copy of the uploaded data
            let image = body.as_bytes().to_vec();

            sp.start_update(component, id, firmware_slot, image)
                .await
                .map_err(|err| SpCommsError::UpdateFailed { sp: sp_id, err })?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_update_status(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseOk<SpUpdateStatus>, HttpError> {
        let apictx = rqctx.context();

        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;

            let status = sp.update_status(component).await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseOk(status.into()))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_hash_firmware_start(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponentFirmwareSlot>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();

        let PathSpComponentFirmwareSlot { sp, component, firmware_slot } =
            path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;

            if component != SpComponent::HOST_CPU_BOOT_FLASH {
                return Err(HttpError::for_bad_request(
                    Some("RequestUnsupportedForComponent".to_string()),
                    "Only the host boot flash can be hashed".into(),
                ));
            }

            // The SP (reasonably!) returns a `HashInProgress` error if we try
            // to start hashing while hashing is being calculated, but we're
            // presenting an idempotent "start hashing if it isn't started"
            // endpoint instead. Swallow that error.
            match sp.start_host_flash_hash(firmware_slot).await {
                Ok(())
                | Err(CommunicationError::SpError(SpError::Hf(
                    HfError::HashInProgress,
                ))) => Ok(HttpResponseUpdatedNoContent()),
                Err(err) => {
                    Err(SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                        .into())
                }
            }
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_hash_firmware_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponentFirmwareSlot>,
    ) -> Result<HttpResponseOk<ComponentFirmwareHashStatus>, HttpError> {
        let apictx = rqctx.context();

        let PathSpComponentFirmwareSlot { sp, component, firmware_slot } =
            path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;

            if component != SpComponent::HOST_CPU_BOOT_FLASH {
                return Err(HttpError::for_bad_request(
                    Some("RequestUnsupportedForComponent".to_string()),
                    "Only the host boot flash can be hashed".into(),
                ));
            }

            let status = match sp.get_host_flash_hash(firmware_slot).await {
                // success
                Ok(sha256) => ComponentFirmwareHashStatus::Hashed { sha256 },

                // expected failure: hash needs to be calculated (or
                // recalculated; either way the client operation is the same)
                Err(CommunicationError::SpError(SpError::Hf(
                    HfError::HashUncalculated | HfError::RecalculateHash,
                ))) => ComponentFirmwareHashStatus::HashNotCalculated,

                // expected failure: hashing is currently in progress; client
                // needs to wait and try again later
                Err(CommunicationError::SpError(SpError::Hf(
                    HfError::HashInProgress,
                ))) => ComponentFirmwareHashStatus::HashInProgress,

                // other errors are failures
                Err(err) => {
                    return Err(HttpError::from(
                        SpCommsError::SpCommunicationFailed { sp: sp_id, err },
                    ));
                }
            };

            Ok(HttpResponseOk(status))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_component_update_abort(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        body: TypedBody<UpdateAbortBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();

        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let component = component_from_str(&component)?;

            let UpdateAbortBody { id } = body.into_inner();
            sp.update_abort(component, id).await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_rot_cmpa_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
    ) -> Result<HttpResponseOk<RotCmpa>, HttpError> {
        let apictx = rqctx.context();

        let PathSpComponent { sp, component } = path.into_inner();
        let sp_id = sp.into();
        let handler = async {
            // Ensure the caller knows they're asking for the RoT
            if component_from_str(&component)? != SpComponent::ROT {
                return Err(HttpError::for_bad_request(
                    Some("RequestUnsupportedForComponent".to_string()),
                    "Only the RoT has a CFPA".into(),
                ));
            }

            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let data = sp.read_rot_cmpa().await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            let base64_data =
                base64::engine::general_purpose::STANDARD.encode(data);

            Ok(HttpResponseOk(RotCmpa { base64_data }))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_rot_cfpa_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        params: TypedBody<GetCfpaParams>,
    ) -> Result<HttpResponseOk<RotCfpa>, HttpError> {
        let apictx = rqctx.context();

        let PathSpComponent { sp, component } = path.into_inner();
        let GetCfpaParams { slot } = params.into_inner();
        let sp_id = sp.into();

        let handler = async {
            // Ensure the caller knows they're asking for the RoT
            if component_from_str(&component)? != SpComponent::ROT {
                return Err(HttpError::for_bad_request(
                    Some("RequestUnsupportedForComponent".to_string()),
                    "Only the RoT has a CFPA".into(),
                ));
            }

            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let data = match slot {
                RotCfpaSlot::Active => sp.read_rot_active_cfpa().await,
                RotCfpaSlot::Inactive => sp.read_rot_inactive_cfpa().await,
                RotCfpaSlot::Scratch => sp.read_rot_scratch_cfpa().await,
            }
            .map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            let base64_data =
                base64::engine::general_purpose::STANDARD.encode(data);

            Ok(HttpResponseOk(RotCfpa { base64_data, slot }))
        };

        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_rot_boot_info(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpComponent>,
        params: TypedBody<GetRotBootInfoParams>,
    ) -> Result<HttpResponseOk<RotState>, HttpError> {
        let apictx = rqctx.context();

        let PathSpComponent { sp, component } = path.into_inner();
        let GetRotBootInfoParams { version } = params.into_inner();
        let sp_id = sp.into();

        let handler = async {
            // Ensure the caller knows they're asking for the RoT
            if component_from_str(&component)? != SpComponent::ROT {
                return Err(HttpError::for_bad_request(
                    Some("RequestUnsupportedForComponent".to_string()),
                    "rot_boot_info only makes sent for a RoT".into(),
                ));
            }

            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let state = sp.rot_state(version).await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseOk(state.into()))
        };

        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_task_dump_count(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<u32>, HttpError> {
        let apictx = rqctx.context();
        let sp_id = path.into_inner().sp.into();

        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let ct = sp.task_dump_count().await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseOk(ct))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_task_dump_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpTaskDumpIndex>,
    ) -> Result<HttpResponseOk<TaskDump>, HttpError> {
        let apictx = rqctx.context();
        let path = path.into_inner();
        let task_index = path.task_dump_index;
        let sp_id = path.sp.into();

        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let raw_dump =
                sp.task_dump_read(task_index).await.map_err(|err| {
                    SpCommsError::SpCommunicationFailed { sp: sp_id, err }
                })?;

            let mut cursor = Cursor::new(Vec::new());
            raw_dump.write_zip(&mut cursor).map_err(|err| {
                HttpError::for_internal_error(err.to_string())
            })?;

            let base64_zip = base64::engine::general_purpose::STANDARD
                .encode(cursor.into_inner());

            Ok(HttpResponseOk(TaskDump {
                task_index: raw_dump.task_index,
                timestamp: raw_dump.timestamp,
                archive_id: hex::encode(raw_dump.archive_id),
                bord: raw_dump.bord,
                gitc: raw_dump.gitc,
                vers: raw_dump.vers,
                base64_zip,
            }))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn ignition_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SpIgnitionInfo>>, HttpError> {
        let apictx = rqctx.context();
        let mgmt_switch = &apictx.mgmt_switch;
        let handler = async {
            let out = mgmt_switch
                .bulk_ignition_state()
                .await?
                .map(|(id, state)| SpIgnitionInfo {
                    id: id.into(),
                    details: state.into(),
                })
                .collect();

            Ok(HttpResponseOk(out))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn ignition_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<SpIgnitionInfo>, HttpError> {
        let apictx = rqctx.context();
        let mgmt_switch = &apictx.mgmt_switch;

        let sp_id = path.into_inner().sp.into();
        let handler = async {
            let ignition_target = mgmt_switch.ignition_target(sp_id)?;

            let state = mgmt_switch
                .ignition_controller()
                .ignition_state(ignition_target)
                .await
                .map_err(|err| SpCommsError::SpCommunicationFailed {
                    sp: sp_id,
                    err,
                })?;

            let info =
                SpIgnitionInfo { id: sp_id.into(), details: state.into() };
            Ok(HttpResponseOk(info))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn ignition_command(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpIgnitionCommand>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let mgmt_switch = &apictx.mgmt_switch;
        let PathSpIgnitionCommand { sp, command } = path.into_inner();
        let sp_id = sp.into();

        let handler = async {
            let ignition_target = mgmt_switch.ignition_target(sp_id)?;

            mgmt_switch
                .ignition_controller()
                .ignition_command(ignition_target, command.into())
                .await
                .map_err(|err| SpCommsError::SpCommunicationFailed {
                    sp: sp_id,
                    err,
                })?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_power_state_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<PowerState>, HttpError> {
        let apictx = rqctx.context();
        let sp_id = path.into_inner().sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;

            let power_state = sp.power_state().await.map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseOk(power_state.into()))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_power_state_set(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
        body: TypedBody<PowerState>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let sp_id = path.into_inner().sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;
            let power_state = body.into_inner();

            let transition = sp
                .set_power_state(power_state.into())
                .await
                .map_err(|err| SpCommsError::SpCommunicationFailed {
                    sp: sp_id,
                    err,
                })?;

            // Log whether the power state actually changed, or if the SP was
            // already in the desired state.
            slog::debug!(
                &rqctx.log,
                "sp_power_state_set";
                "type" => ?sp_id.typ,
                "slot" => sp_id.slot,
                "power_state" => ?power_state,
                "transition" => ?transition,
            );

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_installinator_image_id_set(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
        body: TypedBody<InstallinatorImageId>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        use ipcc::Key;

        let apictx = rqctx.context();
        let sp_id = path.into_inner().sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;

            let image_id = ipcc::InstallinatorImageId::from(body.into_inner());

            sp.set_ipcc_key_lookup_value(
                Key::InstallinatorImageId as u8,
                image_id.serialize(),
            )
            .await
            .map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_installinator_image_id_delete(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        use ipcc::Key;

        let apictx = rqctx.context();
        let sp_id = path.into_inner().sp.into();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(sp_id)?;

            // We clear the image ID by setting it to a 0-length vec.
            sp.set_ipcc_key_lookup_value(
                Key::InstallinatorImageId as u8,
                Vec::new(),
            )
            .await
            .map_err(|err| {
                SpCommsError::SpCommunicationFailed { sp: sp_id, err }
            })?;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_host_phase2_progress_get(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseOk<HostPhase2Progress>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;

            let Some(progress) = sp.most_recent_host_phase2_request().await
            else {
                return Ok(HttpResponseOk(HostPhase2Progress::None));
            };

            // Our `host_phase2_provider` is using an in-memory cache, so the only way
            // we can fail to get the total size is if we no longer have the image that
            // this SP most recently requested. We'll treat that as "no progress
            // information", since it almost certainly means our progress info on this
            // SP is very stale.
            let Ok(total_size) =
                apictx.host_phase2_provider.total_size(progress.hash).await
            else {
                return Ok(HttpResponseOk(HostPhase2Progress::None));
            };

            let image_id = HostPhase2RecoveryImageId {
                sha256_hash: ArtifactHash(progress.hash),
            };

            // `progress` tells us the offset the SP requested and the amount of data we
            // sent starting at that offset; report the end of that chunk to our caller.
            let offset = progress.offset.saturating_add(progress.data_sent);

            Ok(HttpResponseOk(HostPhase2Progress::Available {
                image_id,
                offset,
                total_size,
                age: progress.received.elapsed(),
            }))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_host_phase2_progress_delete(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;

            sp.clear_most_recent_host_phase2_request().await;

            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn recovery_host_phase2_upload(
        rqctx: RequestContext<Self::Context>,
        body: UntypedBody,
    ) -> Result<HttpResponseOk<HostPhase2RecoveryImageId>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            // TODO: this makes a full copy of the host image, potentially unnecessarily
            // if it's malformed.
            let image = body.as_bytes().to_vec();

            let sha256_hash =
                apictx.host_phase2_provider.insert(image).await.map_err(
                    |err| {
                        // Any cache-insertion failure indicates a malformed image; map them
                        // to bad requests.
                        HttpError::for_bad_request(
                            Some("BadHostPhase2Image".to_string()),
                            err.to_string(),
                        )
                    },
                )?;
            let sha256_hash = ArtifactHash(sha256_hash);

            Ok(HttpResponseOk(HostPhase2RecoveryImageId { sha256_hash }))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_local_switch_id(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SpIdentifier>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let id = apictx.mgmt_switch.local_switch()?;

            Ok(HttpResponseOk(id.into()))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_all_ids(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SpIdentifier>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let all_ids = apictx
                .mgmt_switch
                .all_sps()?
                .map(|(id, _)| id.into())
                .collect();

            Ok(HttpResponseOk(all_ids))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }

    async fn sp_ereports_ingest(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSp>,
        query: Query<ereport_types::EreportQuery>,
    ) -> Result<HttpResponseOk<ereport_types::Ereports>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            use crate::EreportError;
            use gateway_sp_comms::ereport;

            let ereport_types::EreportQuery {
                restart_id,
                start_at,
                committed,
                limit,
            } = query.into_inner();

            let sp_id = path.into_inner().sp.into();
            let sp = apictx.mgmt_switch.sp(sp_id)?;

            let req_restart_id = restart_id.into_untyped_uuid();
            let start_ena = start_at
                .map(|ereport_types::Ena(e)| ereport::Ena::new(e))
                .unwrap_or(ereport::Ena::new(0));

            // If the limit is greater than 255, just clamp to that for now.
            // TODO(eliza): eventually, we may want to request multiple tranches
            // from the SP until we either receive an empty one or satisfy the
            // limit requested by Nexus.
            let limit = NonZeroU8::try_from(limit).unwrap_or(NonZeroU8::MAX);
            let committed_ena =
                committed.map(|ereport_types::Ena(e)| ereport::Ena::new(e));

            let ereport::EreportTranche { restart_id, ereports } = sp
                .ereports(req_restart_id, start_ena, limit, committed_ena)
                .await
                .map_err(|error| match error {
                    gateway_sp_comms::error::EreportError::Communication(
                        err,
                    ) => EreportError::SpCommunicationFailed { sp: sp_id, err },
                    err => EreportError::Ereport { sp: sp_id, err },
                })?;
            let restart_id =
                ereport_types::EreporterRestartUuid::from_untyped_uuid(
                    restart_id,
                );
            let ereports = ereports
                .into_iter()
                .map(|ereport::Ereport { ena: ereport::Ena(ena), data }| {
                    ereport_types::Ereport {
                        ena: ereport_types::Ena(ena.into()),
                        data,
                    }
                })
                .collect();
            let reports = dropshot::ResultsPage::new(
                ereports,
                &dropshot::EmptyScanParams {},
                |ereport_types::Ereport { ena, .. }, _| *ena,
            )?;
            Ok(HttpResponseOk(ereport_types::Ereports { restart_id, reports }))
        };
        apictx.latencies.instrument_dropshot_handler(&rqctx, handler).await
    }
}

// wrap `SpComponent::try_from(&str)` into a usable form for dropshot endpoints
fn component_from_str(s: &str) -> Result<SpComponent, HttpError> {
    SpComponent::try_from(s).map_err(|_| {
        HttpError::for_bad_request(
            Some("InvalidSpComponent".to_string()),
            "invalid SP component name".to_string(),
        )
    })
}

// The _from_comms functions are here rather than `From` impls in gateway-types
// so that gateway-types avoids a dependency on gateway-sp-comms.

fn sp_state_from_comms(
    sp_state: VersionedSpState,
    rot_state: Result<RotBootInfo, CommunicationError>,
) -> SpState {
    // We need to keep this backwards compatible. If we get an error from reading `rot_state`
    // it could be because the RoT/SP isn't updated or because we have failed for some
    // other reason. If we're on V1/V2 SP info and we fail, just fall back to using the
    // RoT info in that struct since any error will also be communicated there.
    match (sp_state, rot_state) {
        (VersionedSpState::V1(s), Err(_)) => SpState::from(s),
        (VersionedSpState::V1(s), Ok(r)) => {
            SpState::from((s, RotState::from(r)))
        }
        (VersionedSpState::V2(s), Err(_)) => SpState::from(s),
        (VersionedSpState::V2(s), Ok(r)) => {
            SpState::from((s, RotState::from(r)))
        }
        (VersionedSpState::V3(s), Ok(r)) => {
            SpState::from((s, RotState::from(r)))
        }
        (VersionedSpState::V3(s), Err(err)) => SpState::from((
            s,
            RotState::CommunicationFailed { message: err.to_string() },
        )),
    }
}

fn sp_component_list_from_comms(
    inv: gateway_sp_comms::SpInventory,
) -> SpComponentList {
    SpComponentList {
        components: inv
            .devices
            .into_iter()
            .map(sp_component_info_from_comms)
            .collect(),
    }
}

fn sp_component_info_from_comms(
    dev: gateway_sp_comms::SpDevice,
) -> SpComponentInfo {
    SpComponentInfo {
        component: dev.component.as_str().unwrap_or("???").to_string(),
        device: dev.device,
        serial_number: None, // TODO populate when SP provides it
        description: dev.description,
        capabilities: dev.capabilities.bits(),
        presence: dev.presence.into(),
    }
}
