// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers that implement sled-agent side of host OS updates, used to test host
//! phase 1 updates.

use anyhow::Context as _;
use dropshot::ConfigDropshot;
use dropshot::HttpServer;
use dropshot::ServerBuilder;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types::inventory::Baseboard;
use sled_agent_types::inventory::SledRole;
use slog::Logger;
use sp_sim::GimletPowerState;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use tokio::sync::watch;
use tufaceous_artifact::ArtifactHash;

/// Current state of a fake sled-agent.
#[derive(Debug)]
pub struct HostPhase2State {
    /// The address on which this fake sled-agent is running the sled-agent API
    /// dropshot server.
    pub sled_agent_address: SocketAddrV6,

    /// The current phase 2 artifact in slot A.
    pub slot_a_artifact: ArtifactHash,

    /// The current phase 2 artifact in slot B.
    pub slot_b_artifact: ArtifactHash,

    // Internal channel; we use this to decide whether to pretend our fake
    // sled-agent is "powered on". We can't really fake "powered off" very well,
    // but we'll at least return an HTTP 500 instead of a 200.
    sp_sim_power_state: watch::Receiver<GimletPowerState>,
}

impl HostPhase2State {
    pub fn active_slot_artifact(&self) -> ArtifactHash {
        let (active, _inactive) = self.active_inactive_artifacts();
        active
    }

    pub fn inactive_slot_artifact(&self) -> ArtifactHash {
        let (_active, inactive) = self.active_inactive_artifacts();
        inactive
    }

    // Returns (active, inactive); helper to avoid needing to unpack the
    // active/inactive slot in both of this method's callers.
    fn active_inactive_artifacts(&self) -> (ArtifactHash, ArtifactHash) {
        let a = self.slot_a_artifact;
        let b = self.slot_b_artifact;
        match self.boot_disk().expect("sp-sim should be powered on") {
            M2Slot::A => (a, b),
            M2Slot::B => (b, a),
        }
    }

    pub fn boot_disk(&self) -> Option<M2Slot> {
        match *self.sp_sim_power_state.borrow() {
            GimletPowerState::A2 => None,
            GimletPowerState::A0(slot) => Some(slot),
        }
    }
}

/// A single fake sled-agent.
pub struct HostPhase2TestContext {
    /// The inner state, including the phase 2 artifact hashes and a
    /// `watch::Receiver` connected to an sp-sim instance's power state.
    ///
    /// We don't currently expose a way to change its phase 2 artifact hashes
    /// (nor do we need one for our existing tests). We keep this in a watch
    /// channel anyway to simplify management of the `sled_agent_address` (see
    /// the guts of `new()` below), and to leave the door open for future
    /// changes where we do want to be able to mutate the phase 2 hashes in
    /// tests.
    state: watch::Sender<HostPhase2State>,

    /// Handle to the dropshot server for our fake sled-agent.
    sled_agent_server: HttpServer<HostPhase2SledAgentContext>,
}

impl HostPhase2TestContext {
    /// Construct a new fake sled-agent that responds to the `/inventory`
    /// endpoint with sufficient fidelity for testing host OS phase 1 updates.
    ///
    /// The responses this fake sled-agent make will depend on the value in
    /// `sp_sim_power_state` when the request is received. In particular, if the
    /// simulated SP is powered off, we will return an HTTP 500. (A real
    /// sled-agent wouldn't respond at all, but that's a little tricky for us to
    /// do; returning a 500 is good enough for our tests.)
    pub fn new(
        log: &Logger,
        sp_sim_power_state: watch::Receiver<GimletPowerState>,
    ) -> anyhow::Result<Self> {
        let (state, state_rx) = watch::channel(HostPhase2State {
            // We'll fill this in correctly once we start the dropshot server
            // below. We have to construct this first to give it the receiving
            // half of this watch channel in its server context.
            sled_agent_address: "[::]:0".parse().unwrap(),
            slot_a_artifact: ArtifactHash([0; 32]),
            slot_b_artifact: ArtifactHash([1; 32]),
            sp_sim_power_state,
        });

        let sled_agent_server = {
            let log = log.new(slog::o!("component" => "HostPhase2SledAgent"));
            let api = sled_agent_api::sled_agent_api_mod::api_description::<
                HostPhase2SledAgentImpl,
            >()
            .unwrap();
            ServerBuilder::new(
                api,
                HostPhase2SledAgentContext::new(state_rx),
                log,
            )
            .config(ConfigDropshot {
                bind_address: "[::1]:0".parse().unwrap(),
                ..Default::default()
            })
            .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
                dropshot::ClientSpecifiesVersionInHeader::new(
                    omicron_common::api::VERSION_HEADER,
                    sled_agent_api::latest_version(),
                ),
            )))
            .start()
            .context("failed to create dropshot server")?
        };
        let sled_agent_address = match sled_agent_server.local_addr() {
            SocketAddr::V6(addr) => addr,
            SocketAddr::V4(_) => unreachable!(),
        };
        state.send_modify(|st| {
            st.sled_agent_address = sled_agent_address;
        });

        Ok(Self { state, sled_agent_server })
    }

    pub fn state_rx(&self) -> watch::Receiver<HostPhase2State> {
        self.state.subscribe()
    }

    pub async fn teardown(self) {
        let _ = self.sled_agent_server.close().await;
    }
}

struct HostPhase2SledAgentContext {
    state: watch::Receiver<HostPhase2State>,
    id: SledUuid,
    role: SledRole,
    baseboard: Baseboard,
}

impl HostPhase2SledAgentContext {
    fn new(state: watch::Receiver<HostPhase2State>) -> Self {
        Self {
            state,
            // None of these fields matter to the update tests we perform, but
            // we fill in a few of them here to avoid (e.g.) generating a new
            // random sled ID every time our `/inventory` endpoint is collected.
            id: SledUuid::new_v4(),
            role: SledRole::Gimlet,
            baseboard: Baseboard::Unknown,
        }
    }
}

struct HostPhase2SledAgentImpl;

mod api_impl {
    use super::HostPhase2SledAgentContext;
    use super::HostPhase2SledAgentImpl;
    use camino::Utf8PathBuf;
    use chrono::Utc;
    use dropshot::Body;
    use dropshot::FreeformBody;
    use dropshot::Header;
    use dropshot::HttpError;
    use dropshot::HttpResponseAccepted;
    use dropshot::HttpResponseCreated;
    use dropshot::HttpResponseDeleted;
    use dropshot::HttpResponseHeaders;
    use dropshot::HttpResponseOk;
    use dropshot::HttpResponseUpdatedNoContent;
    use dropshot::Path;
    use dropshot::Query;
    use dropshot::RequestContext;
    use dropshot::StreamingBody;
    use dropshot::TypedBody;
    use iddqd::IdOrdMap;
    use omicron_common::api::external::Generation;
    use omicron_common::api::internal::nexus::DiskRuntimeState;
    use omicron_common::api::internal::nexus::SledVmmState;
    use omicron_common::api::internal::shared::ExternalIpGatewayMap;
    use omicron_common::api::internal::shared::SledIdentifiers;
    use omicron_common::api::internal::shared::VirtualNetworkInterfaceHost;
    use omicron_common::api::internal::shared::{
        ResolvedVpcRouteSet, ResolvedVpcRouteState, SwitchPorts,
    };
    // Fixed identifiers from the migrations crate for API types
    use sled_agent_types_migrations::{v1, v3, v7, v9, v10};
    use sled_diagnostics::SledDiagnosticsQueryOutput;
    use std::collections::BTreeMap;
    use std::time::Duration;

    // We only implement endpoints required for testing host OS updates. All
    // others are left as `unimplemented!()`.
    impl sled_agent_api::SledAgentApi for HostPhase2SledAgentImpl {
        type Context = HostPhase2SledAgentContext;

        async fn inventory(
            rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<v10::inventory::Inventory>, HttpError>
        {
            let ctx = rqctx.context();

            let (
                sled_agent_address,
                boot_disk,
                slot_a_artifact,
                slot_b_artifact,
            ) = {
                let state = ctx.state.borrow();
                (
                    state.sled_agent_address,
                    state.boot_disk(),
                    state.slot_a_artifact,
                    state.slot_b_artifact,
                )
            };

            // If we have no boot disk, we're supposed to be powered off. We
            // can't (easily) just fail to respond like a real powered-off sled,
            // but we can at least return an error.
            let Some(boot_disk) = boot_disk else {
                return Err(HttpError::for_unavail(
                    None,
                    "sled is supposed to be powered off".to_string(),
                ));
            };

            // Construct the `boot_partitions` inventory field (the one our
            // tests really care about) from our current state.
            let make_details = |artifact_hash| {
                Ok(v1::inventory::BootPartitionDetails {
                    artifact_hash,
                    artifact_size: 1000,
                    header: v1::inventory::BootImageHeader {
                        flags: 0,
                        data_size: 1000,
                        image_size: 1000,
                        target_size: 1000,
                        sha256: [0x1d; 32],
                        image_name: "fake header for tests".to_string(),
                    },
                })
            };
            let boot_partitions = v1::inventory::BootPartitionContents {
                boot_disk: Ok(boot_disk),
                slot_a: make_details(slot_a_artifact),
                slot_b: make_details(slot_b_artifact),
            };

            // The rest of the inventory fields are irrelevant; fill them in
            // with something quasi-reasonable (or empty, if we can).
            let config = v10::inventory::OmicronSledConfig {
                generation: Generation::new(),
                disks: IdOrdMap::new(),
                datasets: IdOrdMap::new(),
                zones: IdOrdMap::new(),
                remove_mupdate_override: None,
                host_phase_2: v1::inventory::HostPhase2DesiredSlots {
                    slot_a: v1::inventory::HostPhase2DesiredContents::CurrentContents,
                    slot_b: v1::inventory::HostPhase2DesiredContents::CurrentContents,
                },
            };

            Ok(HttpResponseOk(v10::inventory::Inventory {
                sled_id: ctx.id,
                sled_agent_address,
                sled_role: ctx.role,
                baseboard: ctx.baseboard.clone(),
                usable_hardware_threads: 64,
                usable_physical_ram: (1 << 30).into(),
                reservoir_size: (1 << 29).into(),
                cpu_family: v1::inventory::SledCpuFamily::AmdMilan,
                disks: Vec::new(),
                zpools: Vec::new(),
                datasets: Vec::new(),
                ledgered_sled_config: Some(config.clone()),
                reconciler_status: v10::inventory::ConfigReconcilerInventoryStatus::Idle {
                    completed_at: Utc::now(),
                    ran_for: Duration::from_secs(5),
                },
                last_reconciliation: Some(v10::inventory::ConfigReconcilerInventory {
                    last_reconciled_config: config,
                    external_disks: BTreeMap::new(),
                    datasets: BTreeMap::new(),
                    orphaned_datasets: IdOrdMap::new(),
                    zones: BTreeMap::new(),
                    remove_mupdate_override: None,
                    boot_partitions,
                }),
                zone_image_resolver: v1::inventory::ZoneImageResolverInventory {
                    zone_manifest: v1::inventory::ZoneManifestInventory {
                        boot_disk_path: Utf8PathBuf::new(),
                        boot_inventory: Err(
                            "not implemented by HostPhase2SledAgentImpl"
                                .to_string(),
                        ),
                        non_boot_status: IdOrdMap::new(),
                    },
                    mupdate_override: v1::inventory::MupdateOverrideInventory {
                        boot_disk_path: Utf8PathBuf::new(),
                        boot_override: Err(
                            "not implemented by HostPhase2SledAgentImpl"
                                .to_string(),
                        ),
                        non_boot_status: IdOrdMap::new(),
                    },
                },
            }))
        }

        async fn zone_bundle_list_all(
            _rqctx: RequestContext<Self::Context>,
            _query: Query<v1::params::ZoneBundleFilter>,
        ) -> Result<
            HttpResponseOk<Vec<v1::zone_bundle::ZoneBundleMetadata>>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn zone_bundle_list(
            _rqctx: RequestContext<Self::Context>,
            _params: Path<v1::params::ZonePathParam>,
        ) -> Result<
            HttpResponseOk<Vec<v1::zone_bundle::ZoneBundleMetadata>>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn zone_bundle_get(
            _rqctx: RequestContext<Self::Context>,
            _params: Path<v1::zone_bundle::ZoneBundleId>,
        ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>
        {
            unimplemented!()
        }

        async fn zone_bundle_delete(
            _rqctx: RequestContext<Self::Context>,
            _params: Path<v1::zone_bundle::ZoneBundleId>,
        ) -> Result<HttpResponseDeleted, HttpError> {
            unimplemented!()
        }

        async fn zone_bundle_utilization(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<
            HttpResponseOk<
                BTreeMap<Utf8PathBuf, v1::zone_bundle::BundleUtilization>,
            >,
            HttpError,
        > {
            unimplemented!()
        }

        async fn zone_bundle_cleanup_context(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<v1::zone_bundle::CleanupContext>, HttpError>
        {
            unimplemented!()
        }

        async fn zone_bundle_cleanup_context_update(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<v1::params::CleanupContextUpdate>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn zone_bundle_cleanup(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<
            HttpResponseOk<
                BTreeMap<Utf8PathBuf, v1::zone_bundle::CleanupCount>,
            >,
            HttpError,
        > {
            unimplemented!()
        }

        async fn zones_list(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_list(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::SupportBundleListPathParam>,
        ) -> Result<
            HttpResponseOk<Vec<v1::support_bundle::SupportBundleMetadata>>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn support_bundle_start_creation(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::SupportBundlePathParam>,
        ) -> Result<
            HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn support_bundle_transfer(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::SupportBundlePathParam>,
            _query_params: Query<v1::params::SupportBundleTransferQueryParams>,
            _body: StreamingBody,
        ) -> Result<
            HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn support_bundle_finalize(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::SupportBundlePathParam>,
            _query_params: Query<v1::params::SupportBundleFinalizeQueryParams>,
        ) -> Result<
            HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn support_bundle_download(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<v1::params::RangeRequestHeaders>,
            _path_params: Path<v1::params::SupportBundlePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_download_file(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<v1::params::RangeRequestHeaders>,
            _path_params: Path<v1::params::SupportBundleFilePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_index(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<v1::params::RangeRequestHeaders>,
            _path_params: Path<v1::params::SupportBundlePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_head(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<v1::params::RangeRequestHeaders>,
            _path_params: Path<v1::params::SupportBundlePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_head_file(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<v1::params::RangeRequestHeaders>,
            _path_params: Path<v1::params::SupportBundleFilePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_head_index(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<v1::params::RangeRequestHeaders>,
            _path_params: Path<v1::params::SupportBundlePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_delete(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::SupportBundlePathParam>,
        ) -> Result<HttpResponseDeleted, HttpError> {
            unimplemented!()
        }

        async fn omicron_config_put(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<v10::inventory::OmicronSledConfig>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn sled_role_get(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<v1::inventory::SledRole>, HttpError>
        {
            unimplemented!()
        }

        async fn vmm_register(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VmmPathParam>,
            _body: TypedBody<v10::instance::InstanceEnsureBody>,
        ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
            unimplemented!()
        }

        async fn vmm_unregister(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VmmPathParam>,
        ) -> Result<
            HttpResponseOk<v1::instance::VmmUnregisterResponse>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn vmm_put_state(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VmmPathParam>,
            _body: TypedBody<v1::instance::VmmPutStateBody>,
        ) -> Result<HttpResponseOk<v1::instance::VmmPutStateResponse>, HttpError>
        {
            unimplemented!()
        }

        async fn vmm_get_state(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VmmPathParam>,
        ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
            unimplemented!()
        }

        async fn vmm_put_external_ip(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VmmPathParam>,
            _body: TypedBody<v1::instance::InstanceExternalIpBody>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn vmm_delete_external_ip(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VmmPathParam>,
            _body: TypedBody<v1::instance::InstanceExternalIpBody>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn vmm_join_multicast_group(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VmmPathParam>,
            body: TypedBody<v7::instance::InstanceMulticastBody>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            let body_args = body.into_inner();
            match body_args {
                v7::instance::InstanceMulticastBody::Join(_) => {
                    // MGS test utility - just return success for test compatibility
                    Ok(HttpResponseUpdatedNoContent())
                }
                v7::instance::InstanceMulticastBody::Leave(_) => {
                    // This endpoint is for joining - reject leave operations
                    Err(HttpError::for_bad_request(
                        None,
                        "Join endpoint cannot process Leave operations"
                            .to_string(),
                    ))
                }
            }
        }

        async fn vmm_leave_multicast_group(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VmmPathParam>,
            body: TypedBody<v7::instance::InstanceMulticastBody>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            let body_args = body.into_inner();
            match body_args {
                v7::instance::InstanceMulticastBody::Leave(_) => {
                    // MGS test utility - just return success for test compatibility
                    Ok(HttpResponseUpdatedNoContent())
                }
                v7::instance::InstanceMulticastBody::Join(_) => {
                    // This endpoint is for leaving - reject join operations
                    Err(HttpError::for_bad_request(
                        None,
                        "Leave endpoint cannot process Join operations"
                            .to_string(),
                    ))
                }
            }
        }

        async fn disk_put(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::DiskPathParam>,
            _body: TypedBody<v1::disk::DiskEnsureBody>,
        ) -> Result<HttpResponseOk<DiskRuntimeState>, HttpError> {
            unimplemented!()
        }

        async fn artifact_config_get(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<v1::artifact::ArtifactConfig>, HttpError>
        {
            unimplemented!()
        }

        async fn artifact_config_put(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<v1::artifact::ArtifactConfig>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn artifact_list(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<v1::views::ArtifactListResponse>, HttpError>
        {
            unimplemented!()
        }

        async fn artifact_copy_from_depot(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::ArtifactPathParam>,
            _query_params: Query<v1::params::ArtifactQueryParam>,
            _body: TypedBody<v1::params::ArtifactCopyFromDepotBody>,
        ) -> Result<
            HttpResponseAccepted<v1::views::ArtifactCopyFromDepotResponse>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn artifact_put(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::ArtifactPathParam>,
            _query_params: Query<v1::params::ArtifactQueryParam>,
            _body: StreamingBody,
        ) -> Result<HttpResponseOk<v1::views::ArtifactPutResponse>, HttpError>
        {
            unimplemented!()
        }

        async fn vmm_issue_disk_snapshot_request(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<
                v1::params::VmmIssueDiskSnapshotRequestPathParam,
            >,
            _body: TypedBody<v1::params::VmmIssueDiskSnapshotRequestBody>,
        ) -> Result<
            HttpResponseOk<v1::views::VmmIssueDiskSnapshotRequestResponse>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn vpc_firewall_rules_put(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<v1::params::VpcPathParam>,
            _body: TypedBody<v10::instance::VpcFirewallRulesEnsureBody>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn set_v2p(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<VirtualNetworkInterfaceHost>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn del_v2p(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<VirtualNetworkInterfaceHost>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn list_v2p(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<VirtualNetworkInterfaceHost>>, HttpError>
        {
            unimplemented!()
        }

        async fn uplink_ensure(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<SwitchPorts>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn read_network_bootstore_config_cache(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<
            HttpResponseOk<v1::early_networking::EarlyNetworkConfig>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn write_network_bootstore_config(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<v1::early_networking::EarlyNetworkConfig>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn sled_add(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<v1::sled::AddSledRequest>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn sled_identifiers(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<SledIdentifiers>, HttpError> {
            unimplemented!()
        }

        async fn bootstore_status(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<v1::bootstore::BootstoreStatus>, HttpError>
        {
            unimplemented!()
        }

        async fn list_vpc_routes(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<ResolvedVpcRouteState>>, HttpError>
        {
            unimplemented!()
        }

        async fn set_vpc_routes(
            _request_context: RequestContext<Self::Context>,
            _body: TypedBody<Vec<ResolvedVpcRouteSet>>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn set_eip_gateways(
            _request_context: RequestContext<Self::Context>,
            _body: TypedBody<ExternalIpGatewayMap>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn support_zoneadm_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError>
        {
            unimplemented!()
        }

        async fn support_ipadm_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
        {
            unimplemented!()
        }

        async fn support_dladm_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
        {
            unimplemented!()
        }

        async fn support_nvmeadm_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError>
        {
            unimplemented!()
        }

        async fn support_pargs_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
        {
            unimplemented!()
        }

        async fn support_pstack_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
        {
            unimplemented!()
        }

        async fn support_pfiles_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
        {
            unimplemented!()
        }

        async fn support_zfs_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError>
        {
            unimplemented!()
        }

        async fn support_zpool_info(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError>
        {
            unimplemented!()
        }

        async fn support_health_check(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
        {
            unimplemented!()
        }

        async fn support_logs(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
            unimplemented!()
        }

        async fn support_logs_download(
            _request_context: RequestContext<Self::Context>,
            _path_params: Path<v1::params::SledDiagnosticsLogsDownloadPathParm>,
            _query_params: dropshot::Query<
                v1::params::SledDiagnosticsLogsDownloadQueryParam,
            >,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn chicken_switch_destroy_orphaned_datasets_get(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<
            HttpResponseOk<v1::shared::ChickenSwitchDestroyOrphanedDatasets>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn chicken_switch_destroy_orphaned_datasets_put(
            _request_context: RequestContext<Self::Context>,
            _body: TypedBody<v1::shared::ChickenSwitchDestroyOrphanedDatasets>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn debug_operator_switch_zone_policy_get(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<
            HttpResponseOk<v3::shared::OperatorSwitchZonePolicy>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn debug_operator_switch_zone_policy_put(
            _request_context: RequestContext<Self::Context>,
            _body: TypedBody<v3::shared::OperatorSwitchZonePolicy>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn probes_put(
            _request_context: RequestContext<Self::Context>,
            _body: TypedBody<v10::probes::ProbeSet>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn local_storage_dataset_ensure(
            _request_context: RequestContext<Self::Context>,
            _path_params: Path<v9::params::LocalStoragePathParam>,
            _body: TypedBody<v9::params::LocalStorageDatasetEnsureRequest>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn local_storage_dataset_delete(
            _request_context: RequestContext<Self::Context>,
            _path_params: Path<v9::params::LocalStoragePathParam>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }
    }
}
