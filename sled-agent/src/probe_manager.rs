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
use illumos_utils::zpool::ZpoolOrRamdisk;
use omicron_common::api::external::{
    VpcFirewallRuleAction, VpcFirewallRuleDirection, VpcFirewallRulePriority,
    VpcFirewallRuleStatus,
};
use omicron_common::api::internal::shared::{
    NetworkInterface, ResolvedVpcFirewallRule,
};
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid, ProbeUuid};
use rand::SeedableRng;
use rand::prelude::IteratorRandom;
use sled_agent_config_reconciler::{
    AvailableDatasetsReceiver, CurrentlyManagedZpools,
    CurrentlyManagedZpoolsReceiver,
};
use sled_agent_types::probes::ExternalIp;
use sled_agent_types::probes::ProbeCreate;
use sled_agent_zone_images::ramdisk_file_source;
use slog::{Logger, error, warn};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
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
    running_probes: HashMap<ProbeUuid, RunningZone>,
    available_datasets_rx: AvailableDatasetsReceiver,
    zones_api: Arc<dyn illumos_utils::zone::Api>,
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
        }
    }
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
    /// Run the reconciler loop.
    fn reconciler(
        mut self,
        mut expected_probes_rx: watch::Receiver<IdHashMap<ProbeState>>,
        mut currently_managed_zpools_rx: CurrentlyManagedZpoolsReceiver,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
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
                        let expected_probes = expected_probes_rx
                            .borrow_and_update()
                            .clone()
                            .into_iter()
                            .collect();
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

    /// Reconcile the target set of zones with the actual current set.
    async fn do_reconcile(&mut self, target: HashSet<ProbeState>) {
        let current = match self.current_state().await {
            Ok(state) => state,
            Err(e) => {
                error!(self.log, "get current probe state: {e}");
                return;
            }
        };

        self.add(target.difference(&current)).await;
        self.remove(current.difference(&target)).await;
        self.check(current.intersection(&target));
    }

    /// Removes any probes using filesystem roots on zpools that are not
    /// contained in the set of "disks".
    async fn use_only_these_disks(&mut self, disks: &CurrentlyManagedZpools) {
        let to_remove = self
            .running_probes
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
            self.remove_probe(probe_id).await;
        }
    }

    /// Add a set of probes to this sled.
    async fn add<'a, I>(&mut self, probes: I)
    where
        I: Iterator<Item = &'a ProbeState>,
    {
        for probe in probes {
            info!(self.log, "adding probe {}", probe.id);
            if let Err(e) = self.add_probe(probe).await {
                error!(self.log, "add probe: {e}");
            }
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

        self.running_probes.insert(probe.id, rz);

        Ok(())
    }

    /// Remove a set of probes from this sled.
    async fn remove<'a, I>(&mut self, probes: I)
    where
        I: Iterator<Item = &'a ProbeState>,
    {
        for probe in probes {
            info!(self.log, "removing probe {}", probe.id);
            self.remove_probe(probe.id).await;
        }
    }

    /// Remove a probe from this sled. This tears down the zone and its
    /// network resources.
    async fn remove_probe(&mut self, id: ProbeUuid) {
        match self.running_probes.remove(&id) {
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
    fn check<'a, I>(&self, probes: I)
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

    #[test]
    fn probe_state_set_ops() {
        let a = ProbeState {
            id: ProbeUuid::new_v4(),
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
