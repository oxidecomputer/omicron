// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers that implement sled-agent side of host OS updates, used to test host
//! phase 1 updates.

use anyhow::Context as _;
use dropshot::ConfigDropshot;
use dropshot::HttpServer;
use dropshot::ServerBuilder;
use nexus_sled_agent_shared::inventory::Baseboard;
use nexus_sled_agent_shared::inventory::SledRole;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::SledUuid;
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
                    sled_agent_api::VERSION_ADD_NEXUS_DEBUG_PORT_TO_INVENTORY,
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
    use id_map::IdMap;
    use iddqd::IdOrdMap;
    use nexus_sled_agent_shared::inventory::BootImageHeader;
    use nexus_sled_agent_shared::inventory::BootPartitionContents;
    use nexus_sled_agent_shared::inventory::BootPartitionDetails;
    use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
    use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
    use nexus_sled_agent_shared::inventory::HostPhase2DesiredContents;
    use nexus_sled_agent_shared::inventory::HostPhase2DesiredSlots;
    use nexus_sled_agent_shared::inventory::Inventory;
    use nexus_sled_agent_shared::inventory::MupdateOverrideInventory;
    use nexus_sled_agent_shared::inventory::OmicronSledConfig;
    use nexus_sled_agent_shared::inventory::SledCpuFamily;
    use nexus_sled_agent_shared::inventory::SledRole;
    use nexus_sled_agent_shared::inventory::ZoneImageResolverInventory;
    use nexus_sled_agent_shared::inventory::ZoneManifestInventory;
    use omicron_common::api::external::Generation;
    use omicron_common::api::internal::nexus::DiskRuntimeState;
    use omicron_common::api::internal::nexus::SledVmmState;
    use omicron_common::api::internal::shared::ExternalIpGatewayMap;
    use omicron_common::api::internal::shared::SledIdentifiers;
    use omicron_common::api::internal::shared::VirtualNetworkInterfaceHost;
    use omicron_common::api::internal::shared::{
        ResolvedVpcRouteSet, ResolvedVpcRouteState, SwitchPorts,
    };
    use sled_agent_api::*;
    use sled_agent_types::bootstore::BootstoreStatus;
    use sled_agent_types::disk::DiskEnsureBody;
    use sled_agent_types::early_networking::EarlyNetworkConfig;
    use sled_agent_types::firewall_rules::VpcFirewallRulesEnsureBody;
    use sled_agent_types::instance::InstanceEnsureBody;
    use sled_agent_types::instance::InstanceExternalIpBody;
    use sled_agent_types::instance::VmmPutStateBody;
    use sled_agent_types::instance::VmmPutStateResponse;
    use sled_agent_types::instance::VmmUnregisterResponse;
    use sled_agent_types::sled::AddSledRequest;
    use sled_agent_types::zone_bundle::BundleUtilization;
    use sled_agent_types::zone_bundle::CleanupContext;
    use sled_agent_types::zone_bundle::CleanupCount;
    use sled_agent_types::zone_bundle::ZoneBundleId;
    use sled_agent_types::zone_bundle::ZoneBundleMetadata;
    use sled_diagnostics::SledDiagnosticsQueryOutput;
    use std::collections::BTreeMap;
    use std::time::Duration;

    // We only implement endpoints required for testing host OS updates. All
    // others are left as `unimplemented!()`.
    impl sled_agent_api::SledAgentApi for HostPhase2SledAgentImpl {
        type Context = HostPhase2SledAgentContext;

        async fn inventory(
            rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<Inventory>, HttpError> {
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
                Ok(BootPartitionDetails {
                    artifact_hash,
                    artifact_size: 1000,
                    header: BootImageHeader {
                        flags: 0,
                        data_size: 1000,
                        image_size: 1000,
                        target_size: 1000,
                        sha256: [0x1d; 32],
                        image_name: "fake header for tests".to_string(),
                    },
                })
            };
            let boot_partitions = BootPartitionContents {
                boot_disk: Ok(boot_disk),
                slot_a: make_details(slot_a_artifact),
                slot_b: make_details(slot_b_artifact),
            };

            // The rest of the inventory fields are irrelevant; fill them in
            // with something quasi-reasonable (or empty, if we can).
            let config = OmicronSledConfig {
                generation: Generation::new(),
                disks: IdMap::new(),
                datasets: IdMap::new(),
                zones: IdMap::new(),
                remove_mupdate_override: None,
                host_phase_2: HostPhase2DesiredSlots {
                    slot_a: HostPhase2DesiredContents::CurrentContents,
                    slot_b: HostPhase2DesiredContents::CurrentContents,
                },
            };

            Ok(HttpResponseOk(Inventory {
                sled_id: ctx.id,
                sled_agent_address,
                sled_role: ctx.role,
                baseboard: ctx.baseboard.clone(),
                usable_hardware_threads: 64,
                usable_physical_ram: (1 << 30).into(),
                reservoir_size: (1 << 29).into(),
                cpu_family: SledCpuFamily::AmdMilan,
                disks: Vec::new(),
                zpools: Vec::new(),
                datasets: Vec::new(),
                ledgered_sled_config: Some(config.clone()),
                reconciler_status: ConfigReconcilerInventoryStatus::Idle {
                    completed_at: Utc::now(),
                    ran_for: Duration::from_secs(5),
                },
                last_reconciliation: Some(ConfigReconcilerInventory {
                    last_reconciled_config: config,
                    external_disks: BTreeMap::new(),
                    datasets: BTreeMap::new(),
                    orphaned_datasets: IdOrdMap::new(),
                    zones: BTreeMap::new(),
                    remove_mupdate_override: None,
                    boot_partitions,
                }),
                zone_image_resolver: ZoneImageResolverInventory {
                    zone_manifest: ZoneManifestInventory {
                        boot_disk_path: Utf8PathBuf::new(),
                        boot_inventory: Err(
                            "not implemented by HostPhase2SledAgentImpl"
                                .to_string(),
                        ),
                        non_boot_status: IdOrdMap::new(),
                    },
                    mupdate_override: MupdateOverrideInventory {
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
            _query: Query<ZoneBundleFilter>,
        ) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError>
        {
            unimplemented!()
        }

        async fn zone_bundle_list(
            _rqctx: RequestContext<Self::Context>,
            _params: Path<ZonePathParam>,
        ) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError>
        {
            unimplemented!()
        }

        async fn zone_bundle_get(
            _rqctx: RequestContext<Self::Context>,
            _params: Path<ZoneBundleId>,
        ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>
        {
            unimplemented!()
        }

        async fn zone_bundle_delete(
            _rqctx: RequestContext<Self::Context>,
            _params: Path<ZoneBundleId>,
        ) -> Result<HttpResponseDeleted, HttpError> {
            unimplemented!()
        }

        async fn zone_bundle_utilization(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<
            HttpResponseOk<BTreeMap<Utf8PathBuf, BundleUtilization>>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn zone_bundle_cleanup_context(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<CleanupContext>, HttpError> {
            unimplemented!()
        }

        async fn zone_bundle_cleanup_context_update(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<CleanupContextUpdate>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn zone_bundle_cleanup(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<
            HttpResponseOk<BTreeMap<Utf8PathBuf, CleanupCount>>,
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
            _path_params: Path<SupportBundleListPathParam>,
        ) -> Result<HttpResponseOk<Vec<SupportBundleMetadata>>, HttpError>
        {
            unimplemented!()
        }

        async fn support_bundle_start_creation(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<SupportBundlePathParam>,
        ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError>
        {
            unimplemented!()
        }

        async fn support_bundle_transfer(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<SupportBundlePathParam>,
            _query_params: Query<SupportBundleTransferQueryParams>,
            _body: StreamingBody,
        ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError>
        {
            unimplemented!()
        }

        async fn support_bundle_finalize(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<SupportBundlePathParam>,
            _query_params: Query<SupportBundleFinalizeQueryParams>,
        ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError>
        {
            unimplemented!()
        }

        async fn support_bundle_download(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<RangeRequestHeaders>,
            _path_params: Path<SupportBundlePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_download_file(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<RangeRequestHeaders>,
            _path_params: Path<SupportBundleFilePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_index(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<RangeRequestHeaders>,
            _path_params: Path<SupportBundlePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_head(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<RangeRequestHeaders>,
            _path_params: Path<SupportBundlePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_head_file(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<RangeRequestHeaders>,
            _path_params: Path<SupportBundleFilePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_head_index(
            _rqctx: RequestContext<Self::Context>,
            _headers: Header<RangeRequestHeaders>,
            _path_params: Path<SupportBundlePathParam>,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn support_bundle_delete(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<SupportBundlePathParam>,
        ) -> Result<HttpResponseDeleted, HttpError> {
            unimplemented!()
        }

        async fn omicron_config_put(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<OmicronSledConfig>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn sled_role_get(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<SledRole>, HttpError> {
            unimplemented!()
        }

        async fn vmm_register(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<VmmPathParam>,
            _body: TypedBody<InstanceEnsureBody>,
        ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
            unimplemented!()
        }

        async fn vmm_unregister(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<VmmPathParam>,
        ) -> Result<HttpResponseOk<VmmUnregisterResponse>, HttpError> {
            unimplemented!()
        }

        async fn vmm_put_state(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<VmmPathParam>,
            _body: TypedBody<VmmPutStateBody>,
        ) -> Result<HttpResponseOk<VmmPutStateResponse>, HttpError> {
            unimplemented!()
        }

        async fn vmm_get_state(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<VmmPathParam>,
        ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
            unimplemented!()
        }

        async fn vmm_put_external_ip(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<VmmPathParam>,
            _body: TypedBody<InstanceExternalIpBody>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn vmm_delete_external_ip(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<VmmPathParam>,
            _body: TypedBody<InstanceExternalIpBody>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn disk_put(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<DiskPathParam>,
            _body: TypedBody<DiskEnsureBody>,
        ) -> Result<HttpResponseOk<DiskRuntimeState>, HttpError> {
            unimplemented!()
        }

        async fn artifact_config_get(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<ArtifactConfig>, HttpError> {
            unimplemented!()
        }

        async fn artifact_config_put(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<ArtifactConfig>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn artifact_list(
            _rqctx: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<ArtifactListResponse>, HttpError> {
            unimplemented!()
        }

        async fn artifact_copy_from_depot(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<ArtifactPathParam>,
            _query_params: Query<ArtifactQueryParam>,
            _body: TypedBody<ArtifactCopyFromDepotBody>,
        ) -> Result<
            HttpResponseAccepted<ArtifactCopyFromDepotResponse>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn artifact_put(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<ArtifactPathParam>,
            _query_params: Query<ArtifactQueryParam>,
            _body: StreamingBody,
        ) -> Result<HttpResponseOk<ArtifactPutResponse>, HttpError> {
            unimplemented!()
        }

        async fn vmm_issue_disk_snapshot_request(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<VmmIssueDiskSnapshotRequestPathParam>,
            _body: TypedBody<VmmIssueDiskSnapshotRequestBody>,
        ) -> Result<
            HttpResponseOk<VmmIssueDiskSnapshotRequestResponse>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn vpc_firewall_rules_put(
            _rqctx: RequestContext<Self::Context>,
            _path_params: Path<VpcPathParam>,
            _body: TypedBody<VpcFirewallRulesEnsureBody>,
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
        ) -> Result<HttpResponseOk<EarlyNetworkConfig>, HttpError> {
            unimplemented!()
        }

        async fn write_network_bootstore_config(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<EarlyNetworkConfig>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn sled_add(
            _rqctx: RequestContext<Self::Context>,
            _body: TypedBody<AddSledRequest>,
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
        ) -> Result<HttpResponseOk<BootstoreStatus>, HttpError> {
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
            _path_params: Path<SledDiagnosticsLogsDownloadPathParm>,
            _query_params: dropshot::Query<
                SledDiagnosticsLogsDownloadQueryParam,
            >,
        ) -> Result<http::Response<Body>, HttpError> {
            unimplemented!()
        }

        async fn chicken_switch_destroy_orphaned_datasets_get(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<
            HttpResponseOk<ChickenSwitchDestroyOrphanedDatasets>,
            HttpError,
        > {
            unimplemented!()
        }

        async fn chicken_switch_destroy_orphaned_datasets_put(
            _request_context: RequestContext<Self::Context>,
            _body: TypedBody<ChickenSwitchDestroyOrphanedDatasets>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }

        async fn debug_operator_switch_zone_policy_get(
            _request_context: RequestContext<Self::Context>,
        ) -> Result<HttpResponseOk<OperatorSwitchZonePolicy>, HttpError>
        {
            unimplemented!()
        }

        async fn debug_operator_switch_zone_policy_put(
            _request_context: RequestContext<Self::Context>,
            _body: TypedBody<OperatorSwitchZonePolicy>,
        ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
            unimplemented!()
        }
    }
}
