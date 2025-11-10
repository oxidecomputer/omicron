use crate::metrics::MetricsRequestQueue;
use crate::nexus::NexusClient;
use crate::port_manager::SledAgentPortManager;
use anyhow::{Result, anyhow};
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::{DhcpCfg, PortCreateParams};
use illumos_utils::running_zone::{RunningZone, ZoneBuilderFactory};
use illumos_utils::zpool::ZpoolOrRamdisk;
use nexus_client::types::{ProbeExternalIp, ProbeInfo};
use omicron_common::api::external::{
    VpcFirewallRuleAction, VpcFirewallRuleDirection, VpcFirewallRulePriority,
    VpcFirewallRuleStatus,
};
use omicron_common::api::internal::shared::{
    NetworkInterface, ResolvedVpcFirewallRule,
};
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid};
use rand::SeedableRng;
use rand::prelude::IteratorRandom;
use sled_agent_config_reconciler::{
    AvailableDatasetsReceiver, CurrentlyManagedZpools,
    CurrentlyManagedZpoolsReceiver,
};
use sled_agent_zone_images::ramdisk_file_source;
use slog::{Logger, error, warn};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use uuid::Uuid;
use zone::Zone;

/// Prefix used for probe zone names
const PROBE_ZONE_PREFIX: &str = "oxz_probe";

/// How long to wait between check-ins with nexus
const RECONCILIATION_INTERVAL: Duration = Duration::from_secs(1);

/// The scope to use when allocating VNICs
const VNIC_ALLOCATOR_SCOPE: &str = "probe";

/// The probe manager periodically asks nexus what probes it should be running.
/// It checks the probes it should be running versus the probes it's actually
/// running and reconciles any differences.
pub(crate) struct ProbeManager {
    inner: Arc<ProbeManagerInner>,
}

struct RunningProbes {
    zones: HashMap<Uuid, RunningZone>,
}

pub(crate) struct ProbeManagerInner {
    join_handle: Mutex<Option<JoinHandle<()>>>,
    nexus_client: NexusClient,
    log: Logger,
    sled_id: Uuid,
    vnic_allocator: VnicAllocator<Etherstub>,
    port_manager: SledAgentPortManager,
    metrics_queue: MetricsRequestQueue,
    running_probes: Mutex<RunningProbes>,
    available_datasets_rx: AvailableDatasetsReceiver,

    zones_api: Arc<dyn illumos_utils::zone::Api>,
}

impl ProbeManager {
    pub(crate) fn new(
        sled_id: Uuid,
        nexus_client: NexusClient,
        etherstub: Etherstub,
        port_manager: SledAgentPortManager,
        metrics_queue: MetricsRequestQueue,
        available_datasets_rx: AvailableDatasetsReceiver,
        log: Logger,
    ) -> Self {
        Self {
            inner: Arc::new(ProbeManagerInner {
                join_handle: Mutex::new(None),
                vnic_allocator: VnicAllocator::new(
                    VNIC_ALLOCATOR_SCOPE,
                    etherstub,
                    Arc::new(illumos_utils::dladm::Dladm::real_api()),
                ),
                running_probes: Mutex::new(RunningProbes {
                    zones: HashMap::new(),
                }),
                nexus_client,
                log,
                sled_id,
                port_manager,
                metrics_queue,
                available_datasets_rx,
                zones_api: Arc::new(illumos_utils::zone::Zones::real_api()),
            }),
        }
    }

    pub(crate) async fn run(
        &self,
        currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
    ) {
        self.inner.run(currently_managed_zpools_rx).await;
    }
}

/// State information about a probe. This is a common representation that
/// captures elements from both the nexus and running-zone representation of a
/// probe.
#[derive(Debug, Clone)]
struct ProbeState {
    /// Id as determined by nexus
    id: Uuid,
    /// Runtime state on this sled
    status: zone::State,
    /// The external IP addresses the probe has been assigned.
    external_ips: Vec<ProbeExternalIp>,
    /// The probes networking interface.
    interface: Option<NetworkInterface>,
}

impl PartialEq for ProbeState {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for ProbeState {}

impl Hash for ProbeState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

/// Translate from the nexus API `ProbeInfo` into a `ProbeState`
impl From<ProbeInfo> for ProbeState {
    fn from(value: ProbeInfo) -> Self {
        Self {
            id: value.id,
            status: zone::State::Running,
            external_ips: value.external_ips,
            interface: Some(value.interface),
        }
    }
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
        })
    }
}

impl ProbeManagerInner {
    /// Run the probe manager. If it's already running this is a no-op.
    async fn run(
        self: &Arc<Self>,
        currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
    ) {
        let mut join_handle = self.join_handle.lock().await;
        if join_handle.is_none() {
            *join_handle =
                Some(self.clone().reconciler(currently_managed_zpools_rx))
        }
    }

    /// Run the reconciler loop on a background thread.
    fn reconciler(
        self: Arc<Self>,
        mut currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let sleep_fut = sleep(RECONCILIATION_INTERVAL);
                tokio::pin!(sleep_fut);

                // Wait until the next reconciliation tick, but handle any
                // changes to the set of disks in the meantime.
                loop {
                    tokio::select! {
                        _ = &mut sleep_fut => break,

                        // Cancel-safe per docs on `changed()`
                        result = currently_managed_zpools_rx.changed() => {
                            match result {
                                Ok(()) => {
                                    self.use_only_these_disks(
                                        &currently_managed_zpools_rx
                                            .current_and_update()
                                    ).await;
                                    continue;
                                }
                                Err(_) => {
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
                }

                // Collect the target and current state. Use set operations
                // to determine what probes need to be added, removed and/or
                // modified.

                let target = match self.target_state().await {
                    Ok(state) => state,
                    Err(e) => {
                        error!(self.log, "get target probe state: {e}");
                        continue;
                    }
                };

                let current = match self.current_state().await {
                    Ok(state) => state,
                    Err(e) => {
                        error!(self.log, "get current probe state: {e}");
                        continue;
                    }
                };

                let n_added = self.add(target.difference(&current)).await;
                self.remove(current.difference(&target)).await;
                self.check(current.intersection(&target)).await;

                // If we have created some new probes, we may need the control plane
                // to provide us with valid routes for the VPC the probe belongs to.
                if n_added > 0 {
                    if let Err(e) = self.nexus_client.refresh_vpc_routes().await
                    {
                        error!(self.log, "get routes for probe: {e}");
                    }
                }
            }
        })
    }

    /// Removes any probes using filesystem roots on zpools that are not
    /// contained in the set of "disks".
    async fn use_only_these_disks(&self, disks: &CurrentlyManagedZpools) {
        let mut probes = self.running_probes.lock().await;

        let to_remove = probes
            .zones
            .iter()
            .filter_map(|(id, probe)| {
                let probe_pool = match probe.root_zpool() {
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
            self.remove_probe_locked(&mut probes, probe_id).await;
        }
    }

    /// Add a set of probes to this sled.
    ///
    /// Returns the number of inserted probes.
    async fn add<'a, I>(self: &Arc<Self>, probes: I) -> usize
    where
        I: Iterator<Item = &'a ProbeState>,
    {
        let mut i = 0;
        for probe in probes {
            info!(self.log, "adding probe {}", probe.id);
            if let Err(e) = self.add_probe(probe).await {
                error!(self.log, "add probe: {e}");
            }
            i += 1;
        }
        i
    }

    /// Add a probe to this sled. This sets up resources for the probe zone
    /// such as storage and networking. Then it configures, installs and
    /// boots the probe zone.
    async fn add_probe(self: &Arc<Self>, probe: &ProbeState) -> Result<()> {
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

        let eip = probe
            .external_ips
            .get(0)
            .ok_or(anyhow!("expected an external ip"))?;

        let port = self.port_manager.create_port(PortCreateParams {
            nic,
            source_nat: None,
            ephemeral_ip: Some(eip.ip),
            floating_ips: &[],
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
        })?;

        let installed_zone = ZoneBuilderFactory::new()
            .builder()
            .with_log(self.log.clone())
            .with_underlay_vnic_allocator(&self.vnic_allocator)
            .with_zone_root_path(zone_root_path)
            .with_file_source(&ramdisk_file_source("probe"))
            .with_zone_type("probe")
            .with_unique_name(OmicronZoneUuid::from_untyped_uuid(probe.id))
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

        //TODO SMF properties for probe services?

        let rz = RunningZone::boot(installed_zone).await?;
        rz.ensure_address_for_port("overlay", 0).await?;
        info!(self.log, "started probe {}", probe.id);

        // Notify the sled-agent's metrics task to start tracking the VNIC and
        // any OPTE ports in the zone.
        match self.metrics_queue.track_zone_links(&rz) {
            Ok(_) => debug!(
                self.log,
                "started tracking zone datalinks";
                "zone_name" => rz.name(),
            ),
            Err(errors) => error!(
                self.log,
                "Failed to track one or more datalinks in the zone, \
                some metrics will not be produced";
                "zone_name" => rz.name(),
                "errors" => ?errors,
            ),
        }

        self.running_probes.lock().await.zones.insert(probe.id, rz);

        Ok(())
    }

    /// Remove a set of probes from this sled.
    async fn remove<'a, I>(&self, probes: I)
    where
        I: Iterator<Item = &'a ProbeState>,
    {
        for probe in probes {
            info!(self.log, "removing probe {}", probe.id);
            self.remove_probe(probe.id).await;
        }
    }

    /// Remove a probe from this sled. This tears down the zone and it's
    /// network resources.
    async fn remove_probe(&self, id: Uuid) {
        let mut probes = self.running_probes.lock().await;
        self.remove_probe_locked(&mut probes, id).await
    }

    async fn remove_probe_locked(
        &self,
        probes: &mut MutexGuard<'_, RunningProbes>,
        id: Uuid,
    ) {
        match probes.zones.remove(&id) {
            Some(mut running_zone) => {
                // TODO-correctness: There are no physical links in the zone, is
                // this intended to delete the control VNIC?
                for l in running_zone.links_mut() {
                    if let Err(e) = l.delete().await {
                        error!(self.log, "delete probe link {}: {e}", l.name());
                    }
                }

                // Ask the sled-agent to stop tracking our datalinks, and then
                // delete the OPTE ports.
                match self.metrics_queue.untrack_zone_links(&running_zone) {
                    Ok(_) => debug!(
                        self.log,
                        "stopped tracking zone datalinks";
                        "zone_name" => running_zone.name(),
                    ),
                    Err(errors) => error!(
                        self.log,
                        "Failed to stop tracking one or more datalinks in the \
                        zone, some metrics may still be produced";
                        "zone_name" => running_zone.name(),
                        "errors" => ?errors,
                    ),
                }
                running_zone.release_opte_ports();

                if let Err(e) = running_zone.stop().await {
                    error!(self.log, "stop probe: {e}")
                }
                // TODO are there storage resources that need to be cleared
                // out here too?
            }
            None => {
                warn!(self.log, "attempt to stop non-running probe: {id}")
            }
        }
    }

    /// Check that probes that should be running are running, and with the
    /// correct configuration.
    async fn check<'a, I>(self: &Arc<Self>, probes: I)
    where
        I: Iterator<Item = &'a ProbeState>,
    {
        for probe in probes {
            if probe.status == zone::State::Running {
                continue;
            }
            warn!(
                self.log,
                "probe {} found in unexpected state {:?}",
                probe.id,
                probe.status
            )
            //TODO somehow handle the hooligans here?
        }
    }

    /// Collect target probe state from the nexus internal API.
    async fn target_state(self: &Arc<Self>) -> Result<HashSet<ProbeState>> {
        Ok(self
            .nexus_client
            .probes_get(
                &self.sled_id,
                None, //limit
                None, //page token
                None, //sort by
            )
            .await?
            .into_inner()
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Collect the current probe state from the running zones on this sled.
    async fn current_state(self: &Arc<Self>) -> Result<HashSet<ProbeState>> {
        Ok(self
            .zones_api
            .get()
            .await?
            .into_iter()
            .filter_map(|z| ProbeState::try_from(z).ok())
            .collect())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn probe_state_set_ops() {
        let a = ProbeState {
            id: Uuid::new_v4(),
            status: zone::State::Configured,
            external_ips: Vec::new(),
            interface: None,
        };

        let mut b = a.clone();
        b.status = zone::State::Running;

        let target = HashSet::from([a]);
        let current = HashSet::from([b]);

        let to_add = target.difference(&current);
        let to_remove = current.difference(&target);

        assert_eq!(to_add.count(), 0);
        assert_eq!(to_remove.count(), 0);
    }
}
