// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use id_map::IdMap;
use id_map::IdMappable;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::dladm::EtherstubVnic;
use illumos_utils::running_zone::RunningZone;
use illumos_utils::zone::AdmError;
use illumos_utils::zone::DeleteAddressError;
use illumos_utils::zone::Zones;
use illumos_utils::zpool::ZpoolName;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use omicron_common::address::Ipv6Subnet;
use omicron_uuid_kinds::OmicronZoneUuid;
use sled_agent_types::zone_bundle::ZoneBundleCause;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::ddm_reconciler::DdmReconciler;
use crate::metrics::MetricsRequestQueue;
use crate::params::OmicronZoneConfigExt;
use crate::services::internal_dns_addrobj_name;
use crate::zone_bundle::ZoneBundler;

#[derive(Debug, thiserror::Error)]
pub enum ZoneError {
    #[error("bad zone config: zone is running on ramdisk ?!")]
    ZoneIsRunningOnRamdisk,
    #[error(
        "invalid filesystem_pool in new zone config: \
         expected pool {expected_pool} but got {got_pool}"
    )]
    InvalidFilesystemPoolZoneConfig {
        expected_pool: ZpoolName,
        got_pool: ZpoolName,
    },
}

#[derive(Debug, Clone)]
pub struct ZoneMap {
    zones: IdMap<OmicronZone>,
}

impl ZoneMap {
    /// Attempt to shut down any zones that aren't present in `desired_zones`,
    /// or that weren't present in some prior call but which didn't succeed in
    /// shutting down and are in a partially-shut-down state.
    ///
    /// If any changes are made, returns a new instance of `Self`.
    #[must_use]
    pub(super) async fn shut_down_zones_if_needed(
        &self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        metrics_queue: &MetricsRequestQueue,
        zone_bundler: &ZoneBundler,
        ddm_reconciler: &DdmReconciler,
        underlay_vnic: &EtherstubVnic,
        log: &Logger,
    ) -> Option<Self> {
        let mut zones_fully_shut_down = Vec::new();
        let mut zones_partially_shut_down = BTreeMap::new();

        for current_zone in self.zones.iter() {
            // Skip zones that don't need to be shut down or cleaned up.
            if let Some(desired) = desired_zones.get(&current_zone.config.id) {
                match &current_zone.state {
                    ZoneState::Running { .. } => {
                        // The zone is running and its config hasn't changed; we
                        // should not touch it.
                        if current_zone.config == *desired {
                            continue;
                        }
                    }
                    // If we have a partially shutdown zone, fall through below
                    // and we'll try to resume shutdown (so we can then restart
                    // it with the desired config).
                    ZoneState::PartiallyShutDown(_) => {}
                }
            }

            match current_zone
                .try_shut_down(
                    metrics_queue,
                    zone_bundler,
                    ddm_reconciler,
                    underlay_vnic,
                    log,
                )
                .await
            {
                Ok(()) => {
                    zones_fully_shut_down.push(current_zone.config.id);
                }
                Err(new_state) => {
                    zones_partially_shut_down
                        .insert(current_zone.config.id, new_state);
                }
            }
        }

        if zones_fully_shut_down.is_empty()
            && zones_partially_shut_down.is_empty()
        {
            None
        } else {
            let mut new_self = self.clone();

            for zone_id in zones_fully_shut_down {
                new_self.zones.remove(&zone_id);
            }

            for (zone_id, new_state) in zones_partially_shut_down {
                new_self
                    .zones
                    .get_mut(&zone_id)
                    .expect("only contains keys also present in self.zones")
                    .state = ZoneState::PartiallyShutDown(new_state);
            }

            Some(new_self)
        }
    }
}

#[derive(Debug, Clone)]
enum ZoneState {
    PartiallyShutDown(PartiallyShutDownState),
    Running(Arc<RunningZone>),
}

#[derive(Debug, Clone)]
enum PartiallyShutDownState {
    FailedToStop { running_zone: Arc<RunningZone>, err: Arc<AdmError> },
    FailedToDeleteGzAddress(Arc<DeleteAddressError>),
}

// A running zone and the configuration which started it.
#[derive(Debug, Clone)]
struct OmicronZone {
    config: OmicronZoneConfig,
    state: ZoneState,
}

impl OmicronZone {
    async fn try_shut_down(
        &self,
        metrics_queue: &MetricsRequestQueue,
        zone_bundler: &ZoneBundler,
        ddm_reconciler: &DdmReconciler,
        underlay_vnic: &EtherstubVnic,
        log: &Logger,
    ) -> Result<(), PartiallyShutDownState> {
        let log = log.new(o!("zone" => self.config.zone_name()));

        match &self.state {
            ZoneState::Running(running_zone) => {
                info!(log, "shutting down running zone");

                // We only try once to create a zone bundle; if this fails we
                // move on to the rest of the shutdown process.
                if let Err(err) = zone_bundler
                    .create(&running_zone, ZoneBundleCause::UnexpectedZone)
                    .await
                {
                    warn!(
                        log,
                        "Failed to take bundle of zone we're shutting down";
                        InlineErrorChain::new(&err),
                    );
                }

                // Ensure that the sled agent's metrics task is not tracking the
                // zone's VNICs or OPTE ports.
                //
                // TODO-correctness This should be fallible
                // (https://github.com/oxidecomputer/omicron/issues/7869).
                metrics_queue.untrack_zone_links(running_zone).await;

                self.resume_shutdown_from_stop(
                    ddm_reconciler,
                    underlay_vnic,
                    running_zone,
                    &log,
                )
                .await
            }
            ZoneState::PartiallyShutDown(
                PartiallyShutDownState::FailedToStop { running_zone, .. },
            ) => {
                self.resume_shutdown_from_stop(
                    ddm_reconciler,
                    underlay_vnic,
                    running_zone,
                    &log,
                )
                .await
            }
            ZoneState::PartiallyShutDown(
                PartiallyShutDownState::FailedToDeleteGzAddress(_),
            ) => {
                self.resume_shutdown_from_cleanup(
                    ddm_reconciler,
                    underlay_vnic,
                    &log,
                )
                .await
            }
        }
    }

    async fn resume_shutdown_from_stop(
        &self,
        ddm_reconciler: &DdmReconciler,
        underlay_vnic: &EtherstubVnic,
        running_zone: &Arc<RunningZone>,
        log: &Logger,
    ) -> Result<(), PartiallyShutDownState> {
        // We don't use `running_zone.stop()` here because it doesn't allow
        // repeated attempts after a failure:
        // https://github.com/oxidecomputer/omicron/issues/7881. Instead, use
        // the lower-level `Zones::halt_and_remove_logged()` function directly.
        // This may leave our `RunningZone` is a bogus state where it still
        // holds a `zoneid_t` that doesn't exist anymore, but if we're in the
        // shutdown path we never use that `zoneid_t`.
        if let Err(err) =
            Zones::halt_and_remove_logged(log, running_zone.name()).await
        {
            warn!(
                log,
                "Failed to stop running zone";
                InlineErrorChain::new(&err),
            );
            return Err(PartiallyShutDownState::FailedToStop {
                running_zone: Arc::clone(running_zone),
                err: Arc::new(err),
            });
        }

        self.resume_shutdown_from_cleanup(ddm_reconciler, underlay_vnic, log)
            .await
    }

    async fn resume_shutdown_from_cleanup(
        &self,
        ddm_reconciler: &DdmReconciler,
        underlay_vnic: &EtherstubVnic,
        log: &Logger,
    ) -> Result<(), PartiallyShutDownState> {
        // Special teardown for internal DNS zones: delete the global zone
        // address we created for it, and tell DDM to stop advertising the
        // prefix of that address.
        if let OmicronZoneType::InternalDns {
            gz_address,
            gz_address_index,
            ..
        } = &self.config.zone_type
        {
            let addrobj = AddrObject::new(
                &underlay_vnic.0,
                &internal_dns_addrobj_name(*gz_address_index),
            )
            .expect("internal DNS address object name is well-formed");
            if let Err(err) = Zones::delete_address(None, &addrobj) {
                warn!(
                    log,
                    "Failed to delete internal-dns gz address";
                    InlineErrorChain::new(&err),
                );
                return Err(PartiallyShutDownState::FailedToDeleteGzAddress(
                    Arc::new(err),
                ));
            }

            ddm_reconciler
                .remove_internal_dns_subnet(Ipv6Subnet::new(*gz_address));
        }

        Ok(())
    }
}

impl IdMappable for OmicronZone {
    type Id = OmicronZoneUuid;

    fn id(&self) -> Self::Id {
        self.config.id
    }
}
