// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Object managing "probe" zones, used to test networking configuration without
//! running a full VM.

use crate::metrics::MetricsRequestQueue;
use anyhow::{Result, anyhow};
use dropshot::HttpError;
use iddqd::IdHashItem;
use iddqd::IdHashMap;
use iddqd::id_upcast;
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::{DhcpCfg, PortCreateParams, PortManager};
use illumos_utils::running_zone::{RunningZone, ZoneBuilderFactory};
use illumos_utils::smf_helper::SmfHelper;
use illumos_utils::zpool::ZpoolOrRamdisk;
use omicron_common::api::external::{
    VpcFirewallRuleAction, VpcFirewallRuleDirection, VpcFirewallRulePriority,
    VpcFirewallRuleStatus,
};
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid, ProbeUuid};
use rand::SeedableRng;
use rand::prelude::IteratorRandom;
use sled_agent_config_reconciler::{
    AvailableDatasetsReceiver, CurrentlyManagedZpools,
    CurrentlyManagedZpoolsReceiver,
};
use sled_agent_resolvable_files::ramdisk_file_source;
use sled_agent_types::instance::ExternalIpConfig;
use sled_agent_types::instance::ExternalIpv4Config;
use sled_agent_types::instance::ExternalIpv6Config;
use sled_agent_types::instance::InstanceMulticastMembership;
use sled_agent_types::instance::ResolvedVpcFirewallRule;
use sled_agent_types::inventory::NetworkInterface;
use sled_agent_types::probes::ExternalIp;
use sled_agent_types::probes::ProbeCreate;
use slog::{Logger, error, warn};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;
use zone::Zone;

/// Prefix used for probe zone names
const PROBE_ZONE_PREFIX: &str = "oxz_probe";

/// The scope to use when allocating VNICs
const VNIC_ALLOCATOR_SCOPE: &str = "probe";

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    #[error("Probe with ID {0} already exists")]
    ProbeAlreadyExists(Uuid),
}

impl From<Error> for HttpError {
    fn from(value: Error) -> Self {
        let msg = value.to_string();
        let code = Some(msg.clone());
        match value {
            Error::ProbeAlreadyExists(_) => {
                HttpError::for_bad_request(code, msg)
            }
        }
    }
}

/// The in-zone service that holds a probe's host-local multicast memberships
/// open so the probe answers reachability checks for its enrolled groups.
///
/// A guest receives multicast on an Oxide rack only when two independent
/// requirements are met. The first is dataplane delivery to the OPTE port: the
/// control plane declares this via OPTE `subscribe()` when an instance joins
/// through the external API (see `port_manager` `multicast_groups_ensure`), not
/// by snooping guest IGMP/MLD. A guest still emits a membership report on
/// join, but nothing in the dataplane uses it to drive forwarding. The second
/// is host-local membership: the join installs the interface's multicast
/// reception filter, both the NIC's multicast-MAC acceptance and the L3 group
/// filter, and, without it, the receiving kernel discards the group's traffic
/// rather than accepting and replying to it.
///
/// The control plane only ever programs the port subscription. Nothing in the
/// instance path reaches into the guest. For a normal instance, the host-local
/// membership is the guest's own concern, satisfied by whatever receiver the
/// customer runs inside the VM joining the group with an ordinary socket.
/// Nothing Oxide-specific is required in the guest.
///
/// A probe zone has no customer receiver, so this service stands in for it. It
/// joins each enrolled group and holds the joining socket open for the life
/// of that zone. Membership is socket-scoped, so closing the socket
/// drops the reception filter at once, and a one-shot join would leave nothing
/// holding it. The reachability that the end-to-end tests measure is ICMP
/// echo, which is answered by the probe's own kernel once the filter is
/// installed. The held socket exists to keep that filter present, independent
/// of whether anything reads the socket.
///
/// The current implementation is the `thundermuffin` network instrument. The
/// `service_name`/`smf_name` below are its SMF identifiers in the probe zone
/// image.
struct ProbeMulticastJoiner;

impl illumos_utils::smf_helper::Service for ProbeMulticastJoiner {
    fn service_name(&self) -> &str {
        "thundermuffin"
    }

    fn smf_name(&self) -> String {
        "svc:/oxide/thundermuffin".to_string()
    }
}

/// Manages a set of "probe" zones, used to validate networking configuration.
///
/// This type spawns and manages a set of zones on a sled. In the Oxide product,
/// Nexus periodically sends the manager the zones it expects, and this ensures
/// that they're running.
pub(crate) struct ProbeManager {
    _join_handle: JoinHandle<()>,
    // The set of probes we have been told to run.
    expected_probes_tx: watch::Sender<IdHashMap<ProbeState>>,
}

/// Worker object that actually reconciles the desired and actual probe zones.
pub(crate) struct ProbeManagerInner {
    log: Logger,
    vnic_allocator: VnicAllocator<Etherstub>,
    port_manager: PortManager,
    metrics_queue: MetricsRequestQueue,
    // The set of probe zones we are actually running on this sled.
    running_probes: HashMap<ProbeUuid, RunningProbe>,
    available_datasets_rx: AvailableDatasetsReceiver,
    zones_api: Arc<dyn illumos_utils::zone::Api>,
}

/// A booted probe zone together with its desired in-zone multicast joiner
/// config and whether that config has been realized yet.
struct RunningProbe {
    zone: RunningZone,
    /// The probe's network interface, fixed at provisioning. Retained so the
    /// reconciler can report a non-lossy `current` state. A `Zone` carries no
    /// information about its network devices, so reconstructing this from a
    /// running zone is not possible without querying dladm/opteadm.
    interface: NetworkInterface,
    /// The probe's external IP addresses, fixed at provisioning. Retained for
    /// the same reason as `interface`.
    external_ips: Vec<ExternalIp>,
    /// The desired in-zone multicast joiner config: the single source of truth
    /// for the kernel joins. It carries both the overlay interface (fixed at
    /// provisioning) and the target group set. A membership change on an
    /// already-running probe updates this and clears `joiner_realized`. The
    /// zone's OPTE port subscription is reconciled separately against the port
    /// manager's own authoritative set.
    joiner: JoinerConfig,
    /// Whether the `joiner` has been applied in-zone. Cleared when the desired
    /// config changes or when a boot-time apply failed, so
    /// `retry_unrealized_multicast` re-applies it on a later reconcile without
    /// tearing the zone down.
    joiner_realized: bool,
    /// Whether the OPTE port multicast subscription has been ensured for the
    /// stored desired group set. Cleared when a re-ensure in
    /// `reconcile_membership` fails so `retry_unrealized_multicast`
    /// re-converges it on the periodic timer, mirroring `joiner_realized` for
    /// the in-zone joiner. Without it, a transient OPTE failure would sit
    /// stale until the expected-probe set next changed.
    subscription_realized: bool,
    /// Set when zone teardown failed and the probe awaits retirement.
    ///
    /// A defunct probe is withheld from `current_state` enrichment so the
    /// reconciler sees it as diverged and drives it back through
    /// `remove_probe`, which retries the halt and releases the OPTE ports
    /// only once the zone is gone. Releasing them while the zone is live
    /// deletes the `xde` devices as busy, leaking them and wedging later
    /// recreates with `MacExists`.
    defunct: bool,
}

/// The in-zone multicast joiner configuration for a probe, captured at
/// provisioning so a failed realization can be retried verbatim.
#[derive(Clone)]
struct JoinerConfig {
    /// Overlay address used to pin the joins (`IP_MULTICAST_IF` / SSM).
    multicast_iface: Option<Ipv4Addr>,
    /// Groups the probe's kernel should locally join.
    multicast_groups: Vec<InstanceMulticastMembership>,
}

/// The period of how often to retry realizing the in-zone multicast joiner for
/// probes whose join has not yet been applied. The reconciler otherwise only
/// runs when the expected-probe set changes, which it does not while a probe is
/// stuck, so this timer drives the re-converge.
const JOINER_RETRY_PERIOD: Duration = Duration::from_secs(30);

/// Translate a probe's membership set into the OPTE port subscription config.
fn multicast_group_cfgs(
    groups: &[InstanceMulticastMembership],
) -> Vec<illumos_utils::opte::MulticastGroupCfg> {
    groups
        .iter()
        .map(|m| illumos_utils::opte::MulticastGroupCfg {
            group_ip: m.group_ip,
            sources: m.sources.clone(),
        })
        .collect()
}

impl ProbeManager {
    pub(crate) fn new(
        etherstub: Etherstub,
        port_manager: PortManager,
        metrics_queue: MetricsRequestQueue,
        available_datasets_rx: AvailableDatasetsReceiver,
        log: Logger,
        currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
    ) -> Self {
        let (expected_probes_tx, expected_probes_rx) =
            watch::channel(IdHashMap::new());
        let inner = ProbeManagerInner {
            vnic_allocator: VnicAllocator::new(
                VNIC_ALLOCATOR_SCOPE,
                etherstub,
                Arc::new(illumos_utils::dladm::Dladm::real_api()),
            ),
            running_probes: HashMap::new(),
            log,
            port_manager,
            metrics_queue,
            available_datasets_rx,
            zones_api: Arc::new(illumos_utils::zone::Zones::real_api()),
        };
        Self {
            expected_probes_tx,
            _join_handle: inner
                .reconciler(expected_probes_rx, currently_managed_zpools_rx),
        }
    }

    /// Completely replace the set of managed probes.
    pub(crate) fn set_probes(&self, probes: IdHashMap<ProbeCreate>) {
        let probes =
            probes.into_iter().map(|probe| ProbeState::from(probe)).collect();
        let _old = self.expected_probes_tx.send_replace(probes);
    }
}

/// State information about a probe. This is a common representation that
/// captures elements from both the nexus and running-zone representation of a
/// probe.
#[derive(Debug, Clone)]
struct ProbeState {
    /// Id as determined by nexus
    id: ProbeUuid,
    /// Runtime state on this sled
    status: zone::State,
    /// The external IP addresses the probe has been assigned.
    external_ips: Vec<ExternalIp>,
    /// The probes networking interface.
    ///
    /// This is only `None` when we reconstruct the existing state from the
    /// current set of zones on the sled. The `Zone` type we build this from
    /// doesn't have information about the network devices in that case. Note
    /// that we _could_ fetch it by asking `dladm` and `opteadm` for all the
    /// relevant details, but we haven't needed that so far.
    ///
    /// If we've built this object from a request through the sled-agent API,
    /// then we always have this.
    interface: Option<NetworkInterface>,
    /// Multicast groups the probe's OPTE port should subscribe to,
    /// fixed at probe zone provisioning. Empty when reconstructing
    /// state from an existing zone.
    multicast_groups: Vec<InstanceMulticastMembership>,
}

impl IdHashItem for ProbeState {
    type Key<'a> = ProbeUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

impl From<ProbeCreate> for ProbeState {
    fn from(params: ProbeCreate) -> Self {
        Self {
            id: params.id,
            status: zone::State::Running,
            external_ips: params.external_ips,
            interface: Some(params.interface),
            multicast_groups: params.multicast_groups,
        }
    }
}

/// Decide whether a probe present in both the target and current sets must be
/// torn down and recreated, as opposed to reconciled in place.
///
/// The probe ID is the map key, so it matches by construction and the decision
/// rests on comparing field values. Two conditions force a recreate:
///
/// 1. A provisioning-fixed field (the network interface or external IP set)
///    differs and cannot be changed on a running probe.
/// 2. The probe zone is not in the `Running` state and must be restarted to
///    drive it back to convergence.
///
/// The target side is always constructed as `Running`, so the state check
/// compares the zone-derived current status.
fn probe_requires_recreate(target: &ProbeState, current: &ProbeState) -> bool {
    current.status != zone::State::Running
        || target.interface != current.interface
        || target.external_ips != current.external_ips
}

/// Normalized membership view for change detection: each group address paired
/// with its source set.
fn membership_set(
    memberships: &[InstanceMulticastMembership],
) -> HashSet<(IpAddr, BTreeSet<IpAddr>)> {
    memberships
        .iter()
        .map(|m| (m.group_ip, m.sources.iter().copied().collect()))
        .collect()
}

/// Translate from running zone state into a `ProbeState`
impl TryFrom<Zone> for ProbeState {
    type Error = String;
    fn try_from(value: Zone) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            id: value
                .name()
                .strip_prefix(&format!("{PROBE_ZONE_PREFIX}_"))
                .ok_or(String::from("not a probe prefix"))?
                .parse()
                .map_err(|e| format!("invalid uuid: {e}"))?,
            status: value.state(),
            external_ips: Vec::new(),
            interface: None,
            multicast_groups: Vec::new(),
        })
    }
}

impl ProbeManagerInner {
    /// Run the reconciler loop.
    fn reconciler(
        mut self,
        mut expected_probes_rx: watch::Receiver<IdHashMap<ProbeState>>,
        mut currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut joiner_retry = tokio::time::interval(JOINER_RETRY_PERIOD);
            joiner_retry.set_missed_tick_behavior(
                tokio::time::MissedTickBehavior::Delay,
            );
            loop {
                tokio::select! {
                    // Re-converge any probe whose OPTE subscription or in-zone
                    // joiner is still unrealized. The first tick fires
                    // immediately (this is a no-op when nothing is pending).
                    _ = joiner_retry.tick() => {
                        self.retry_unrealized_multicast().await;
                    }

                    // Wait for changes to the set of expected probes.
                    //
                    // This is cancel-safe, according to
                    // https://docs.rs/tokio/latest/tokio/sync/watch/struct.Receiver.html#method.changed.
                    res = expected_probes_rx.changed() => {
                        if res.is_err() {
                            warn!(
                                self.log,
                                "Watch channel with expected probes \
                                is closed and all values have been seen, \
                                exiting now."
                            );
                            return;
                        }
                        let expected_probes =
                            expected_probes_rx.borrow_and_update().clone();
                        self.do_reconcile(expected_probes).await;
                    }

                    // Wait for changes to the set of managed zpools.
                    //
                    // Cancel-safe per docs on `changed()`
                    result = currently_managed_zpools_rx.changed() => {
                        if result.is_ok() {
                            self.use_only_these_disks(
                                &currently_managed_zpools_rx
                                    .current_and_update()
                            ).await;
                        } else {
                            warn!(
                                self.log,
                                "ProbeManager's 'current zpools' \
                                 channel closed; shutting down",
                            );
                            return;
                        }
                    }
                }
            }
        })
    }

    /// Reconcile the target set of probes against the actual current set.
    ///
    /// Each probe is matched by ID (its identity) and then classified by config
    /// content. A probe present only in the target is added, and one present
    /// only in the current set is removed. For a probe present in both,
    /// `probe_requires_recreate` decides whether a fixed-at-provision field
    /// changed (the zone must be torn down and recreated) or the probe stays
    /// up and its in-place-mutable config (multicast membership) is reconciled
    /// without a zone restart.
    async fn do_reconcile(&mut self, target: IdHashMap<ProbeState>) {
        let current = match self.current_state().await {
            Ok(state) => state,
            Err(e) => {
                error!(self.log, "get current probe state: {e}");
                return;
            }
        };

        // A defunct probe whose zone has since vanished is absent from
        // `current`, so the loops below cannot retire it. Therefore, we retire
        // it here before the add loop so a recreate does not collide with its
        // still-held OPTE ports. `remove_probe`'s halt is a no-op for an
        // absent zone.
        let vanished: Vec<ProbeUuid> = self
            .running_probes
            .iter()
            .filter(|(id, p)| p.defunct && current.get(*id).is_none())
            .map(|(id, _)| *id)
            .collect();
        for id in vanished {
            info!(self.log, "removing vanished defunct probe {id}");
            self.remove_probe(id).await;
        }

        for probe in &target {
            let Some(running) = current.get(&probe.id) else {
                info!(self.log, "adding probe {}", probe.id);
                if let Err(e) = self.add_probe(probe).await {
                    error!(self.log, "add probe: {e}");
                }
                continue;
            };

            if !probe_requires_recreate(probe, running) {
                // The probe stays up, so reconcile its multicast membership
                // in place. This runs even when the membership appears
                // unchanged so the OPTE subscription self-heals drift we
                // cannot observe from our own local record.
                self.reconcile_membership(probe).await;
                continue;
            }

            info!(
                self.log,
                "probe diverged from desired state, recreating";
                "probe" => %probe.id,
                "status" => ?running.status,
            );
            if !self.remove_probe(probe.id).await {
                warn!(
                    self.log,
                    "could not remove diverged probe, deferring recreate";
                    "probe" => %probe.id,
                );
                continue;
            }
            if let Err(e) = self.add_probe(probe).await {
                error!(self.log, "recreate probe: {e}");
            }
        }

        for running in &current {
            if target.get(&running.id).is_none() {
                info!(self.log, "removing probe {}", running.id);
                self.remove_probe(running.id).await;
            }
        }

        // Re-attempt any OPTE subscription or in-zone join left pending by a
        // previous reconcile, so a target change drives re-converge immediately
        // rather than waiting for the periodic timer.
        self.retry_unrealized_multicast().await;
    }

    /// Removes any probes using filesystem roots on zpools that are not
    /// contained in the set of "disks".
    async fn use_only_these_disks(&mut self, disks: &CurrentlyManagedZpools) {
        let to_remove = self
            .running_probes
            .iter()
            .filter_map(|(id, probe)| {
                let probe_pool = match probe.zone.root_zpool() {
                    ZpoolOrRamdisk::Zpool(zpool_name) => zpool_name,
                    ZpoolOrRamdisk::Ramdisk => {
                        info!(
                            self.log,
                            "use_only_these_disks: removing probe on ramdisk";
                            "id" => ?id,
                        );
                        return None;
                    }
                };

                if !disks.contains(probe_pool) { Some(*id) } else { None }
            })
            .collect::<Vec<_>>();

        for probe_id in to_remove {
            info!(self.log, "use_only_these_disks: Removing probe"; "probe_id" => ?probe_id);
            self.remove_probe(probe_id).await;
        }
    }

    /// Add a probe to this sled. This sets up resources for the probe zone
    /// such as storage and networking. Then it configures, installs and
    /// boots the probe zone.
    async fn add_probe(&mut self, probe: &ProbeState) -> Result<()> {
        let mut rng = rand::rngs::StdRng::from_os_rng();
        let zone_root_path = self
            .available_datasets_rx
            .all_mounted_zone_root_datasets()
            .into_iter()
            .choose(&mut rng)
            .ok_or_else(|| anyhow!("u2 not found"))?;

        let nic = probe
            .interface
            .as_ref()
            .ok_or(anyhow!("no interface specified for probe"))?;

        // NOTE: The Nexus probe API only supports constructing an Ephemeral
        // address, so ensure that's the case here and us it as such.
        let eip = probe
            .external_ips
            .get(0)
            .ok_or(anyhow!("expected an external ip"))?;
        anyhow::ensure!(
            matches!(eip.kind, sled_agent_types::probes::IpKind::Ephemeral),
            "Probes are expected to have an Ephemeral IP address",
        );
        let external_ips = match eip.ip {
            IpAddr::V4(ipv4) => ExternalIpConfig {
                v4: Some(ExternalIpv4Config {
                    ephemeral_ip: Some(ipv4),
                    ..Default::default()
                }),
                v6: None,
            },
            IpAddr::V6(ipv6) => ExternalIpConfig {
                v6: Some(ExternalIpv6Config {
                    ephemeral_ip: Some(ipv6),
                    ..Default::default()
                }),
                v4: None,
            },
        };

        let port = self.port_manager.create_port(PortCreateParams {
            nic,
            external_ips: &external_ips,
            firewall_rules: &[ResolvedVpcFirewallRule {
                status: VpcFirewallRuleStatus::Enabled,
                direction: VpcFirewallRuleDirection::Inbound,
                targets: vec![nic.clone()],
                filter_hosts: None,
                filter_ports: None,
                filter_protocols: None,
                action: VpcFirewallRuleAction::Allow,
                priority: VpcFirewallRulePriority(100),
            }],
            dhcp_config: DhcpCfg::default(),
            // TODO-completeness: Attached subnets are meant only for instances,
            // but probes are supposed to mimic instances as closely as
            // possible. We should consider if we want to support them here.
            attached_subnets: vec![],
            multicast_groups: &multicast_group_cfgs(&probe.multicast_groups),
            mtu: None,
        })?;

        // Captured before `port` is moved into the zone builder below. The
        // joiner config carries it so `apply_joiner` can pin the probe's
        // multicast joins (IP_MULTICAST_IF / SSM membership) to its overlay
        // port.
        let multicast_iface = port.0.ipv4_addr().copied();

        let installed_zone = ZoneBuilderFactory::new()
            .builder()
            .with_log(self.log.clone())
            .with_underlay_vnic_allocator(&self.vnic_allocator)
            .with_zone_root_path(zone_root_path)
            .with_file_source(&ramdisk_file_source("probe"))
            .with_zone_type("probe")
            .with_unique_name(OmicronZoneUuid::from_untyped_uuid(
                probe.id.into_untyped_uuid(),
            ))
            .with_datasets(&[])
            .with_filesystems(&[])
            .with_data_links(&[])
            .with_devices(&[])
            .with_opte_ports(vec![port])
            .with_links(vec![])
            .with_limit_priv(vec![])
            .install()
            .await?;

        info!(self.log, "installed probe {}", probe.id);

        let mut running_zone = match RunningZone::try_boot(installed_zone).await
        {
            Ok(zone) => zone,
            Err((e, zone)) => {
                if let Some(zone) = zone {
                    error!(
                        self.log,
                        "failed to boot probe zone and cleanup halt failed, \
                         tracking as defunct";
                        "probe" => %probe.id,
                        "error" => %e,
                    );
                    // The zone is still live, so keep its ports rather than
                    // dropping them here. The reconciler retires the defunct
                    // probe through `remove_probe`, which retries the halt
                    // and releases the ports once the zone is gone.
                    self.running_probes.insert(
                        probe.id,
                        RunningProbe {
                            zone,
                            interface: nic.clone(),
                            external_ips: probe.external_ips.clone(),
                            joiner: JoinerConfig {
                                multicast_iface,
                                multicast_groups: probe
                                    .multicast_groups
                                    .clone(),
                            },
                            joiner_realized: false,
                            // `create_port` established the subscription.
                            subscription_realized: true,
                            defunct: true,
                        },
                    );
                }
                return Err(e.into());
            }
        };

        // Assigning the overlay address is the only post-boot step that must
        // roll the zone back on failure. Without it the OPTE port is live but
        // unusable, and `RunningZone`'s `Drop` only halts the zone
        // asynchronously, never releasing the port.
        //
        // Teardown order matters: `stop` halts and removes the zone, tearing
        // down the VNIC layered over the `xde` device, and only then can
        // `release_opte_ports` delete the `xde` cleanly. In the other order,
        // the delete fails as busy while the VNIC is live and the port leaks,
        // holding the probe's MAC so every later reconcile fails `create_xde`
        // with `MacExists`, wedging the probe permanently.
        if let Err(e) = running_zone.ensure_address_for_port("overlay", 0).await
        {
            error!(
                self.log,
                "failed to assign probe overlay address, tearing down zone";
                "probe" => %probe.id,
                "error" => %e,
            );
            if let Err(stop_err) = running_zone.stop().await {
                error!(
                    self.log,
                    "failed to stop probe zone during rollback";
                    "probe" => %probe.id,
                    "error" => %stop_err,
                );
                // `stop` takes the zone ID before halting, so a failed halt
                // cannot be retried through it and the async drop path will
                // not run either. Retry the halt directly.
                if let Err(halt_err) = self
                    .zones_api
                    .halt_and_remove_logged(&self.log, running_zone.name())
                    .await
                {
                    error!(
                        self.log,
                        "failed to halt probe zone during rollback";
                        "probe" => %probe.id,
                        "error" => %halt_err,
                    );
                    // The zone is still live, so track it as defunct rather
                    // than releasing its ports here. The reconciler retires
                    // it through `remove_probe`, which retries the halt and
                    // releases the ports only once the zone is gone.
                    self.running_probes.insert(
                        probe.id,
                        RunningProbe {
                            zone: running_zone,
                            interface: nic.clone(),
                            external_ips: probe.external_ips.clone(),
                            joiner: JoinerConfig {
                                multicast_iface,
                                multicast_groups: probe
                                    .multicast_groups
                                    .clone(),
                            },
                            joiner_realized: false,
                            // `create_port` established the subscription.
                            subscription_realized: true,
                            defunct: true,
                        },
                    );
                    return Err(e.into());
                }
            }
            running_zone.release_opte_ports();
            return Err(e.into());
        }
        info!(self.log, "started probe {}", probe.id);

        // By contrast, a joiner failure (e.g., `manifest-import` still
        // running) does not fail probe creation. The zone is already
        // serving its OPTE port subscription, and tearing it down would only
        // feed a reconcile loop where Nexus re-pushes the probe and the zone
        // flaps. Keep the zone and let `retry_unrealized_multicast` re-apply
        // the join on a later reconcile.
        let joiner = JoinerConfig {
            multicast_iface,
            multicast_groups: probe.multicast_groups.clone(),
        };
        let joiner_realized =
            match self.apply_joiner(&running_zone, probe.id, &joiner).await {
                Ok(()) => true,
                Err(e) => {
                    warn!(
                        self.log,
                        "probe multicast joiner not yet realized, will retry";
                        "probe" => %probe.id,
                        "error" => %e,
                    );
                    false
                }
            };

        // Notify the sled-agent's metrics task to start tracking the VNIC and
        // any OPTE ports in the zone.
        match self.metrics_queue.track_zone_links(&running_zone) {
            Ok(_) => debug!(
                self.log,
                "started tracking zone datalinks";
                "zone_name" => running_zone.name(),
            ),
            Err(errors) => error!(
                self.log,
                "Failed to track one or more datalinks in the zone, \
                some metrics will not be produced";
                "zone_name" => running_zone.name(),
                "errors" => ?errors,
            ),
        }

        self.running_probes.insert(
            probe.id,
            RunningProbe {
                zone: running_zone,
                interface: nic.clone(),
                external_ips: probe.external_ips.clone(),
                joiner,
                joiner_realized,
                // `create_port` above established the subscription, so it
                // starts realized. `reconcile_membership` clears it when a
                // later re-ensure fails.
                subscription_realized: true,
                defunct: false,
            },
        );

        Ok(())
    }

    /// Enroll the probe's kernel as a local member of each multicast group it
    /// was asked to join. See `ProbeMulticastJoiner` for why a probe needs
    /// this while a normal instance does not.
    ///
    /// `RunningZone::boot` only waits for the single-user milestone, but the
    /// joiner's manifest lives under `/var/svc/manifest/site` and is imported
    /// by `svc:/system/manifest-import`, which may still be running at that
    /// point. Until it completes, `svccfg` against the joiner fails because the
    /// service is absent (or the repository server is briefly unavailable), so
    /// we wait for the service to become reachable before programming it.
    ///
    /// The service ships disabled in the zone image. We populate its config and
    /// enable it when there is at least one group to join. An empty target is
    /// the inverse, where the prior membership is cleared and the service
    /// disabled, so a non-empty to empty transition does not leave stale joins
    /// behind an enabled service.
    ///
    /// Retries by `retry_unrealized_multicast` are safe: the
    /// `config/multicast_group` values are cleared before being re-added and
    /// disabling an already-disabled service is a no-op. A non-empty target
    /// restarts the service after refreshing config so an already-running
    /// joiner drops sockets for removed groups and opens them for the new set.
    async fn apply_joiner(
        &self,
        running_zone: &RunningZone,
        probe_id: ProbeUuid,
        cfg: &JoinerConfig,
    ) -> Result<()> {
        let smf = SmfHelper::new(running_zone, &ProbeMulticastJoiner);

        // Wait (bounded) for the joiner service to be importable. `refresh`
        // has no effect on a fresh service yet requires it to exist, so it
        // doubles as a readiness probe. On a retry the service already
        // exists, so this returns on the first attempt. The bound is kept
        // short because this runs inside the reconcile loop. If the service
        // is not ready yet, we give up quickly and let
        // `retry_unrealized_multicast` try again on the periodic timer rather
        // than stalling all reconciles.
        const MAX_ATTEMPTS: usize = 5;
        const RETRY_DELAY: Duration = Duration::from_secs(2);
        let mut attempt = 1;
        loop {
            match smf.refresh() {
                Ok(()) => break,
                Err(e) if attempt < MAX_ATTEMPTS => {
                    debug!(
                        self.log,
                        "waiting for probe multicast joiner service to import";
                        "probe" => %probe_id,
                        "attempt" => attempt,
                        "error" => %e,
                    );
                    attempt += 1;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(e) => {
                    return Err(anyhow!(
                        "probe {probe_id} multicast joiner service did not \
                         become available after {attempt} attempts: {e}",
                    ));
                }
            }
        }

        // The joiner ships its `config` group at the service level, and
        // `svccfg setprop` only auto-creates a group when none exists at any
        // scope, so the first instance-scoped write below would otherwise
        // fail with "No such property group". Create the instance group
        // first (an existing one is tolerated).
        smf.addpropgroup_default_instance("config", "application")?;
        // Clear values from a prior attempt before re-adding so a retry does
        // not accumulate duplicates (the glob matches everything, a missing
        // property is tolerated). Running ahead of the empty-target branch
        // means a non-empty to empty transition also drops the stale joins.
        smf.delpropvalue_default_instance("config/multicast_group", "*")?;

        // An empty target means the probe should hold no joins. The values
        // are already cleared above, so refresh the now-empty config and
        // disable the joiner (disabling a disabled service is a no-op).
        if cfg.multicast_groups.is_empty() {
            smf.refresh()?;
            smf.disable()?;
            info!(
                self.log,
                "disabled multicast joiner for probe {probe_id} \
                 with empty membership",
            );
            return Ok(());
        }

        if let Some(iface) = cfg.multicast_iface {
            smf.setprop_default_instance(
                "config/multicast_iface",
                iface.to_string(),
            )?;
        }
        // TODO: set `config/ipv6_scope` (the overlay port's ifindex) once
        // probes carry IPv6 multicast memberships. `config/multicast_iface`
        // only pins IPv4 joins. IPv6 has no address-based IP_MULTICAST_IF
        // equivalent, so the joiner takes a numeric scope for
        // IPV6_MULTICAST_IF and leaves it kernel-selected when unset.
        for membership in &cfg.multicast_groups {
            // A source-specific (SSM) membership cannot be joined any-source:
            // the kernel requires an INCLUDE-mode (S, G) join, so the sources
            // must travel with the group to the joiner. Encode them inline as
            // `group@src1,src2`. An any-source (ASM) membership stays a bare
            // `group`. The `@` separator is not shell-special, so svcprop emits
            // it verbatim (unlike `|`, which it escapes), and neither `@` nor
            // `,` can appear in a v4 or v6 IP literal, so the split is
            // unambiguous for the method script.
            let value = if membership.sources.is_empty() {
                membership.group_ip.to_string()
            } else {
                let sources = membership
                    .sources
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{}@{}", membership.group_ip, sources)
            };
            smf.addpropvalue_type_default_instance(
                "config/multicast_group",
                value,
                "astring",
            )?;
        }
        smf.refresh()?;
        // A persistently failing joiner gets restart-throttled by svc.startd
        // into maintenance, where enable and restart are no-ops. Clear it so
        // a re-apply with corrected config actually brings the service back
        // up (clearing a healthy instance is a no-op).
        smf.clear()?;
        smf.enable()?;
        smf.restart()?;

        // `enable` and `restart` are asynchronous requests to svc.startd, and
        // `restart` only acts on an instance that is online or degraded.
        // Without observing the state, a failed start method (the instance
        // lands in maintenance after we return) or a restart dropped while
        // the instance is still transitioning would be reported as success,
        // and a realized joiner is never revisited.
        let mut state = smf.state()?;
        for _ in 1..MAX_ATTEMPTS {
            if state == "online" {
                break;
            }
            tokio::time::sleep(RETRY_DELAY).await;
            state = smf.state()?;
        }
        if state != "online" {
            return Err(anyhow!(
                "probe {probe_id} multicast joiner did not come online, \
                 state is '{state}'",
            ));
        }
        info!(
            self.log,
            "enabled and restarted multicast joiner for probe {probe_id}";
            "groups" => cfg.multicast_groups.len(),
        );
        Ok(())
    }

    /// Re-converge any running probe whose OPTE port subscription or in-zone
    /// multicast joiner has not yet been realized. Driven by the periodic timer
    /// in `reconciler` because the expected-probe set does not change while a
    /// probe is stuck, so the reconcile would not otherwise re-run.
    ///
    /// The two are handled independently: the subscription is re-ensured
    /// against the port manager (which diffs internally) and the joiner is
    /// re-applied in-zone. Both are safe to repeat and read the stored
    /// desired group set from `RunningProbe`, so no target snapshot is
    /// needed here.
    ///
    /// A realized joiner is also health-checked. `apply_joiner` observed the
    /// service `online` at least once, but svc.startd can later restart and
    /// throttle a crash-looping joiner into maintenance. The expected-probe
    /// set is unchanged, so nothing else would revisit it. A probe whose
    /// joiner left `online` is demoted back to unrealized here and re-applied
    /// by the same reconciler pass.
    async fn retry_unrealized_multicast(&mut self) {
        // A probe with an empty target keeps its joiner disabled, which is
        // its healthy state, so only non-empty targets are checked.
        let mut demoted = Vec::new();
        for (id, p) in &self.running_probes {
            if p.defunct
                || !p.joiner_realized
                || p.joiner.multicast_groups.is_empty()
            {
                continue;
            }
            let smf = SmfHelper::new(&p.zone, &ProbeMulticastJoiner);
            match smf.state() {
                Ok(state) if state == "online" => {}
                Ok(state) => {
                    warn!(
                        self.log,
                        "probe multicast joiner left 'online', \
                         scheduling re-apply";
                        "probe" => %id,
                        "state" => %state,
                    );
                    demoted.push(*id);
                }
                // The query itself failing (e.g., the zone is mid-teardown)
                // is not evidence the joiner regressed, so the probe keeps
                // its realized state and the next tick tries again.
                Err(e) => warn!(
                    self.log,
                    "failed to query probe multicast joiner state";
                    "probe" => %id,
                    "error" => %e,
                ),
            }
        }
        for id in demoted {
            if let Some(p) = self.running_probes.get_mut(&id) {
                p.joiner_realized = false;
            }
        }

        let pending: Vec<ProbeUuid> = self
            .running_probes
            .iter()
            // Defunct probes are awaiting retirement, not repair.
            .filter(|(_, p)| {
                !p.defunct && (!p.joiner_realized || !p.subscription_realized)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in pending {
            // Re-ensure the OPTE port subscription if it is still pending. The
            // interface and stored desired group set are owned by
            // `RunningProbe`, so copy them out and drop the shared borrow
            // before the ensure.
            let subscription_retry = match self.running_probes.get(&id) {
                Some(running_probe) if !running_probe.subscription_realized => {
                    Some((
                        running_probe.interface.id,
                        running_probe.interface.kind,
                        multicast_group_cfgs(
                            &running_probe.joiner.multicast_groups,
                        ),
                    ))
                }
                _ => None,
            };
            if let Some((nic_id, nic_kind, cfgs)) = subscription_retry {
                match self
                    .port_manager
                    .multicast_groups_ensure(nic_id, nic_kind, &cfgs)
                {
                    Ok(()) => {
                        if let Some(running_probe) =
                            self.running_probes.get_mut(&id)
                        {
                            running_probe.subscription_realized = true;
                        }
                        info!(
                            self.log,
                            "probe OPTE multicast subscription realized on \
                             retry";
                            "probe" => %id,
                        );
                    }
                    Err(e) => warn!(
                        self.log,
                        "probe OPTE multicast subscription retry failed, \
                         will retry again";
                        "probe" => %id,
                        "error" => %e,
                    ),
                }
            }

            // Re-apply the in-zone joiner if it is still pending. Capturing
            // the result in `outcome` ends the shared borrow of
            // `running_probes` before the `get_mut` update below.
            let outcome = match self.running_probes.get(&id) {
                Some(running_probe) if !running_probe.joiner_realized => {
                    let cfg = running_probe.joiner.clone();
                    Some(self.apply_joiner(&running_probe.zone, id, &cfg).await)
                }
                _ => None,
            };
            match outcome {
                Some(Ok(())) => {
                    if let Some(running_probe) =
                        self.running_probes.get_mut(&id)
                    {
                        running_probe.joiner_realized = true;
                    }
                    info!(
                        self.log,
                        "probe multicast joiner realized on retry";
                        "probe" => %id,
                    );
                }
                Some(Err(e)) => warn!(
                    self.log,
                    "probe multicast joiner retry failed, will retry again";
                    "probe" => %id,
                    "error" => %e,
                ),
                None => {}
            }
        }
    }

    /// Reconcile the multicast membership of a probe that is already running.
    ///
    /// Called for a probe present in both the target and current sets that
    /// does not require a recreate, where only the group membership may have
    /// changed. The two pieces of state are reconciled differently:
    ///
    /// The OPTE port subscription is re-ensured unconditionally. The port
    /// manager owns the authoritative set and diffs internally, so a matching
    /// subscription costs nothing and a drifted one is repaired. The outcome
    /// lands in `subscription_realized` so a transient failure is retried by
    /// `retry_unrealized_multicast` rather than left stale until the next
    /// expected-probe change.
    ///
    /// The in-zone kernel joiner is reapplied only when the target set
    /// differs from the stored desired set in `joiner.multicast_groups`. A
    /// difference updates the stored set and clears `joiner_realized`, and
    /// the `retry_unrealized_multicast` call at the end of `do_reconcile`
    /// performs the apply.
    async fn reconcile_membership(&mut self, probe: &ProbeState) {
        // A target probe without an interface cannot have its port reconciled.
        let Some(nic) = probe.interface.as_ref() else {
            return;
        };
        // Skip probes we are not actually running. The caller only invokes this
        // for probes in both sets, so a mismatch here would be a bookkeeping
        // bug rather than an expected case.
        if !self.running_probes.contains_key(&probe.id) {
            return;
        }

        // Re-ensure the OPTE port subscription, recording the outcome so a
        // transient failure is re-converged by the periodic retry. This
        // failure handling is specific to probes: an instance ensures its
        // subscription from `join_multicast_group_inner` and
        // `leave_multicast_group_inner`, whose `Result` returns to the Nexus
        // RPC caller for retry, whereas a probe is driven by a level-triggered
        // reconcile against the expected-probe watch snapshot. A failure
        // dropped here would not be reattempted until that snapshot next
        // changed, so we track it and let `retry_unrealized_multicast`
        // re-ensure on the periodic timer.
        let subscription_realized = match self
            .port_manager
            .multicast_groups_ensure(
                nic.id,
                nic.kind,
                &multicast_group_cfgs(&probe.multicast_groups),
            ) {
            Ok(()) => true,
            Err(e) => {
                error!(
                    self.log,
                    "failed to reconcile probe OPTE multicast subscription, \
                     will retry";
                    "probe" => %probe.id,
                    "error" => %e,
                );
                false
            }
        };

        let Some(running_probe) = self.running_probes.get_mut(&probe.id) else {
            return;
        };
        running_probe.subscription_realized = subscription_realized;

        // Reapply the in-zone joiner only when the set changed.
        // `joiner.multicast_groups` is the stored desired set, so a set
        // difference means the membership target moved and the kernel
        // joins must be re-realized.
        let applied = membership_set(&running_probe.joiner.multicast_groups);
        let target = membership_set(&probe.multicast_groups);
        if applied == target {
            return;
        }
        info!(
            self.log,
            "probe multicast membership changed, will reapply joiner";
            "probe" => %probe.id,
            "groups" => probe.multicast_groups.len(),
        );
        running_probe.joiner.multicast_groups = probe.multicast_groups.clone();
        running_probe.joiner_realized = false;
    }

    /// Remove a probe from this sled. This tears down the zone and its
    /// network resources.
    /// Returns `true` once the zone and its network resources have been
    /// removed. A failed halt leaves a defunct probe tracked so a later
    /// reconciliation can retry without creating a second zone or releasing
    /// ports underneath the live one.
    async fn remove_probe(&mut self, id: ProbeUuid) -> bool {
        match self.running_probes.remove(&id) {
            Some(mut running_probe) => {
                // TODO-correctness: There are no physical links in the zone, is
                // this intended to delete the control VNIC?
                for l in running_probe.zone.links_mut() {
                    if let Err(e) = l.delete().await {
                        error!(self.log, "delete probe link {}: {e}", l.name());
                    }
                }

                // Ask the sled-agent to stop tracking our datalinks, and then
                // delete the OPTE ports.
                match self.metrics_queue.untrack_zone_links(&running_probe.zone)
                {
                    Ok(_) => debug!(
                        self.log,
                        "stopped tracking zone datalinks";
                        "zone_name" => running_probe.zone.name(),
                    ),
                    Err(errors) => error!(
                        self.log,
                        "Failed to stop tracking one or more datalinks in the \
                        zone, some metrics may still be produced";
                        "zone_name" => running_probe.zone.name(),
                        "errors" => ?errors,
                    ),
                }
                // Halt the zone before releasing its OPTE ports. The halt
                // tears down the VNICs layered over the `xde` devices so they
                // can be deleted cleanly. Releasing first deletes them while
                // the zone is still up, the delete fails as busy, and the
                // ports leak, holding the probe's MAC and wedging future
                // `create_xde` calls with `MacExists`.
                //
                // Halt by name rather than through `stop`. `stop` takes the
                // zone ID before halting, so a failed halt cannot be retried
                // through it, and on a probe re-inserted below it would
                // succeed vacuously. Halting by name is a no-op once the zone
                // is gone.
                if let Err(e) = self
                    .zones_api
                    .halt_and_remove_logged(
                        &self.log,
                        running_probe.zone.name(),
                    )
                    .await
                {
                    error!(
                        self.log,
                        "failed to halt probe zone, keeping its ports for a \
                         later pass";
                        "probe" => %id,
                        "error" => %e,
                    );
                    // Keep the probe tracked as defunct so the next reconcile
                    // retries the halt with the ports intact, instead of
                    // releasing them under a live zone.
                    running_probe.defunct = true;
                    self.running_probes.insert(id, running_probe);
                    return false;
                }
                // Consume the zone ID so the drop path does not spawn a
                // redundant halt of the already-removed zone.
                let _ = running_probe.zone.stop().await;
                running_probe.zone.release_opte_ports();
                true
            }
            None => {
                // The probe zone is live on the sled but absent from
                // `running_probes`, typically because a prior sled-agent
                // incarnation created it and the restart emptied the map.
                // `current_state` reports such a zone as bare (no interface
                // or external IPs), which `probe_requires_recreate` reads as
                // a diverged probe, so `do_reconcile` lands here before the
                // recreate.
                //
                // Without removing the zone first, the following `add_probe`
                // would collide with the live zone. Halt and remove it by name
                // so that the recreate starts clean.
                let zone_name = format!("{PROBE_ZONE_PREFIX}_{id}");
                if let Err(e) = self
                    .zones_api
                    .halt_and_remove_logged(&self.log, &zone_name)
                    .await
                {
                    error!(
                        self.log,
                        "failed to remove untracked probe zone";
                        "probe" => %id,
                        "zone_name" => %zone_name,
                        "error" => %e,
                    );
                    return false;
                }
                true
            }
        }
    }

    /// Collect the current probe state from the running zones on this sled.
    ///
    /// `TryFrom<Zone>` recovers only the ID and runtime status from a running
    /// zone. The interface, external IPs, and multicast groups are not present
    /// on the `Zone`. We enrich each zone-derived state with the intended
    /// config retained in `running_probes` so the `current` side of the
    /// reconcile diff is not lossy. A zone we are not tracking (for instance,
    /// one left by a prior sled-agent incarnation) has no retained config and
    /// is reported as the bare zone-derived state.
    async fn current_state(&self) -> Result<IdHashMap<ProbeState>> {
        Ok(self
            .zones_api
            .get()
            .await?
            .into_iter()
            .filter_map(|z| ProbeState::try_from(z).ok())
            .map(|mut state| {
                // A defunct probe is intentionally left bare so
                // `probe_requires_recreate` reads it as diverged and the
                // reconciler retires it through `remove_probe`.
                match self.running_probes.get(&state.id) {
                    Some(running_probe) if !running_probe.defunct => {
                        state.interface = Some(running_probe.interface.clone());
                        state.external_ips = running_probe.external_ips.clone();
                        state.multicast_groups =
                            running_probe.joiner.multicast_groups.clone();
                    }
                    _ => {}
                }
                state
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use omicron_common::api::external::{MacAddr, Vni};
    use omicron_common::api::internal::shared::PrivateIpConfig;
    use sled_agent_types::inventory::NetworkInterfaceKind;

    fn membership(group_ip: &str) -> InstanceMulticastMembership {
        InstanceMulticastMembership {
            group_ip: group_ip.parse().unwrap(),
            sources: Vec::new(),
        }
    }

    fn test_nic(id: Uuid) -> NetworkInterface {
        NetworkInterface {
            id,
            kind: NetworkInterfaceKind::Probe { id: Uuid::new_v4() },
            name: "probe-nic".parse().unwrap(),
            ip_config: PrivateIpConfig::new_ipv4(
                "172.30.0.5".parse().unwrap(),
                "172.30.0.0/24".parse().unwrap(),
            )
            .unwrap(),
            mac: MacAddr::random_guest(),
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        }
    }

    fn probe(interface: Option<NetworkInterface>) -> ProbeState {
        ProbeState {
            id: ProbeUuid::new_v4(),
            status: zone::State::Running,
            external_ips: Vec::new(),
            interface,
            multicast_groups: Vec::new(),
        }
    }

    /// A change to the network interface, fixed at probe-zone provisioning,
    /// forces a recreate, while an identical interface does not.
    #[test]
    fn interface_change_requires_recreate() {
        let nic = test_nic(Uuid::new_v4());

        let current = probe(Some(nic.clone()));
        let unchanged = ProbeState { interface: Some(nic), ..current.clone() };
        assert!(!probe_requires_recreate(&unchanged, &current));

        let changed = ProbeState {
            interface: Some(test_nic(Uuid::new_v4())),
            ..current.clone()
        };
        assert!(probe_requires_recreate(&changed, &current));
    }

    /// A probe whose only change is its multicast group set must not be
    /// recreated. The reconciler keeps the zone and reapplies membership in
    /// place.
    ///
    /// The membership delta is detected by the `membership_set` comparison
    /// that the in-zone joiner path in `reconcile_membership` uses, which
    /// is insensitive to both membership and source ordering.
    #[test]
    fn membership_change_does_not_require_recreate() {
        let nic = test_nic(Uuid::new_v4());
        let current = ProbeState {
            interface: Some(nic.clone()),
            multicast_groups: vec![membership("239.100.0.1")],
            ..probe(None)
        };
        let target = ProbeState {
            interface: Some(nic),
            multicast_groups: vec![
                membership("239.100.0.1"),
                membership("239.100.0.2"),
            ],
            ..probe(None)
        };

        assert!(!probe_requires_recreate(&target, &current));

        assert_ne!(
            membership_set(&current.multicast_groups),
            membership_set(&target.multicast_groups),
        );

        // A reordered source list is the same membership, not a change that
        // should reapply the joiner.
        let sources = |ips: &[&str]| InstanceMulticastMembership {
            group_ip: "232.100.0.1".parse().unwrap(),
            sources: ips.iter().map(|ip| ip.parse().unwrap()).collect(),
        };
        assert_eq!(
            membership_set(&[sources(&["10.0.0.1", "10.0.0.2"])]),
            membership_set(&[sources(&["10.0.0.2", "10.0.0.1"])]),
        );
    }

    /// A change to the external IP set, also fixed at provisioning, forces a
    /// recreate.
    #[test]
    fn external_ip_change_requires_recreate() {
        let eip = |ip: &str| ExternalIp {
            ip: ip.parse().unwrap(),
            kind: sled_agent_types::probes::IpKind::Ephemeral,
            first_port: 0,
            last_port: u16::MAX,
        };
        let nic = test_nic(Uuid::new_v4());
        let current = ProbeState {
            interface: Some(nic.clone()),
            external_ips: vec![eip("198.51.100.1")],
            ..probe(None)
        };
        let target = ProbeState {
            interface: Some(nic),
            external_ips: vec![eip("198.51.100.2")],
            ..probe(None)
        };
        assert!(probe_requires_recreate(&target, &current));
    }

    /// A probe wedged in a non-`Running` state is recreated even when its
    /// config is unchanged, driving it back to convergence.
    #[test]
    fn wedged_probe_requires_recreate() {
        let nic = test_nic(Uuid::new_v4());
        let target = probe(Some(nic.clone()));
        let current = ProbeState {
            status: zone::State::Down,
            interface: Some(nic),
            ..target.clone()
        };
        assert!(probe_requires_recreate(&target, &current));
    }
}
