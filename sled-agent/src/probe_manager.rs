use crate::config_reconciler::ReconcilerStateReceiver;
use crate::metrics::MetricsRequestQueue;
use crate::nexus::NexusClient;
use anyhow::{Result, anyhow};
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::{DhcpCfg, PortCreateParams, PortManager};
use illumos_utils::running_zone::{RunningZone, ZoneBuilderFactory};
use illumos_utils::zpool::ZpoolOrRamdisk;
use nexus_client::types::{
    BackgroundTasksActivateRequest, ProbeExternalIp, ProbeInfo,
};
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
use slog::{Logger, error, warn};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::sleep;
use uuid::Uuid;
use zone::Zone;

/// Prefix used for probe zone names
const PROBE_ZONE_PREFIX: &str = "oxz_probe";

/// How long to wait between check-ins with nexus
const RECONCILIATION_INTERVAL: Duration = Duration::from_secs(1);

/// The scope to use when allocating VNICs
const VNIC_ALLOCATOR_SCOPE: &str = "probe";

struct RunningProbes {
    zones: HashMap<Uuid, RunningZone>,
}

/// The probe manager periodically asks nexus what probes it should be running.
/// It checks the probes it should be running versus the probes it's actually
/// running and reconciles any differences.
pub(crate) struct ProbeManager {
    nexus_client: NexusClient,
    log: Logger,
    sled_id: Uuid,
    vnic_allocator: VnicAllocator<Etherstub>,
    reconciler_state_rx: ReconcilerStateReceiver,
    port_manager: PortManager,
    metrics_queue: MetricsRequestQueue,
    running_probes: Mutex<RunningProbes>,

    zones_api: Arc<dyn illumos_utils::zone::Api>,
}

impl ProbeManager {
    pub(crate) fn spawn(
        sled_id: Uuid,
        nexus_client: NexusClient,
        etherstub: Etherstub,
        reconciler_state_rx: ReconcilerStateReceiver,
        port_manager: PortManager,
        metrics_queue: MetricsRequestQueue,
        log: Logger,
    ) {
        let slf = Self {
            vnic_allocator: VnicAllocator::new(
                VNIC_ALLOCATOR_SCOPE,
                etherstub,
                Arc::new(illumos_utils::dladm::Dladm::real_api()),
            ),
            running_probes: Mutex::new(RunningProbes { zones: HashMap::new() }),
            nexus_client,
            log,
            sled_id,
            reconciler_state_rx,
            port_manager,
            metrics_queue,
            zones_api: Arc::new(illumos_utils::zone::Zones::real_api()),
        };
        tokio::spawn(slf.run());
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

impl ProbeManager {
    /// Run the probe manager. This should be spawned on a background task.
    async fn run(mut self) {
        loop {
            let sleep_interval = sleep(RECONCILIATION_INTERVAL);
            tokio::pin!(sleep_interval);

            // Wait for `sleep_interval`; while doing so, prune any probes on
            // disks that are no longer managed (if there have been any changes
            // in our disks).
            'wait_for_reconcilation_interval: loop {
                tokio::select! {
                    res = self.reconciler_state_rx.changed() => {
                        if res.is_ok() {
                            self.remove_probes_not_on_managed_disks().await;
                            continue 'wait_for_reconcilation_interval;
                        } else {
                            // This should never happen in production, but may
                            // in tests.
                            warn!(
                                self.log,
                                "reconciler watch channel closed; exiting"
                            );
                            return;
                        }
                    }

                    _ = &mut sleep_interval => {
                        break 'wait_for_reconcilation_interval;
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
                if let Err(e) = self
                    .nexus_client
                    .bgtask_activate(&BackgroundTasksActivateRequest {
                        bgtask_names: vec!["vpc_route_manager".into()],
                    })
                    .await
                {
                    error!(self.log, "get routes for probe: {e}");
                }
            }
        }
    }

    /// Removes any probes using filesystem roots on zpools that are not
    /// contained in the set of "disks".
    async fn remove_probes_not_on_managed_disks(&mut self) {
        let u2_set: HashSet<_> = self
            .reconciler_state_rx
            .current_and_update()
            .all_managed_external_disk_pools()
            .cloned()
            .collect();
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

                if !u2_set.contains(probe_pool) { Some(*id) } else { None }
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
    async fn add<'a, I>(&self, probes: I) -> usize
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
    async fn add_probe(&self, probe: &ProbeState) -> Result<()> {
        let mut rng = rand::rngs::StdRng::from_entropy();
        let zone_root_path = self
            .reconciler_state_rx
            .current()
            .all_mounted_zone_root_datasets()
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
            .with_zone_image_paths(&["/opt/oxide".into()])
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
        if !self.metrics_queue.track_zone_links(&rz).await {
            error!(
                self.log,
                "Failed to track one or more datalinks in the zone, \
                some metrics will not be produced";
                "zone_name" => rz.name(),
            );
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
                    if let Err(e) = l.delete() {
                        error!(self.log, "delete probe link {}: {e}", l.name());
                    }
                }

                // Ask the sled-agent to stop tracking our datalinks, and then
                // delete the OPTE ports.
                self.metrics_queue.untrack_zone_links(&running_zone).await;
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
    async fn check<'a, I>(&self, probes: I)
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
    async fn target_state(&self) -> Result<HashSet<ProbeState>> {
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
    async fn current_state(&self) -> Result<HashSet<ProbeState>> {
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
