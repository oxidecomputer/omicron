// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for Omicron zones.
//!
//! There is no separate tokio task here; our parent reconciler task owns this
//! set of zones and is able to mutate it in place during reconciliation.

use crate::InternalDisks;
use crate::SledAgentFacilities;
use crate::TimeSyncConfig;
use camino::Utf8PathBuf;
use futures::FutureExt as _;
use futures::future;
use id_map::IdMap;
use id_map::IdMappable;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::running_zone::RunCommandError;
use illumos_utils::running_zone::RunningZone;
use illumos_utils::zone::AdmError;
use illumos_utils::zone::Api as _;
use illumos_utils::zone::DeleteAddressError;
use illumos_utils::zone::Zones;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use nexus_sled_agent_shared::inventory::ZoneKind;
use ntp_admin_client::types::TimeSync;
use omicron_common::address::Ipv6Subnet;
use omicron_common::zone_images::ZoneImageFileSource;
use omicron_uuid_kinds::OmicronZoneUuid;
use sled_agent_types::zone_bundle::ZoneBundleCause;
use sled_agent_types::zone_images::MupdateOverrideReadError;
use sled_agent_types::zone_images::OmicronZoneFileSource;
use sled_agent_types::zone_images::OmicronZoneImageLocation;
use sled_agent_types::zone_images::PreparedOmicronZone;
use sled_agent_types::zone_images::RAMDISK_IMAGE_PATH;
use sled_agent_types::zone_images::ResolverStatus;
use sled_agent_types::zone_images::RunningZoneImageLocation;
use sled_agent_types::zone_images::ZoneImageLocationError;
use sled_storage::config::MountConfig;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tufaceous_artifact::ArtifactHash;

use super::OmicronDatasets;
use super::datasets::ZoneDatasetDependencyError;

#[derive(Debug, Clone)]
pub enum TimeSyncStatus {
    NotYetChecked,
    ConfiguredToSkip,
    FailedToGetSyncStatus(Arc<TimeSyncError>),
    TimeSync(TimeSync),
}

impl TimeSyncStatus {
    pub(crate) fn is_synchronized(&self) -> bool {
        match self {
            TimeSyncStatus::NotYetChecked
            | TimeSyncStatus::FailedToGetSyncStatus(_) => false,
            TimeSyncStatus::ConfiguredToSkip => true,
            TimeSyncStatus::TimeSync(time_sync) => time_sync.sync,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TimeSyncError {
    #[error("no running NTP zone")]
    NoRunningNtpZone,
    #[error("multiple running NTP zones - this should never happen!")]
    MultipleRunningNtpZones,
    #[error("failed to communicate with NTP admin server")]
    NtpAdmin(#[from] ntp_admin_client::Error<ntp_admin_client::types::Error>),
    #[error("failed to execute chronyc within NTP zone")]
    ExecuteChronyc(#[source] RunCommandError),
    #[error(
        "failed to parse chronyc tracking output: {reason} (stdout: {stdout:?})"
    )]
    FailedToParse { reason: &'static str, stdout: String },
}

#[derive(Debug)]
pub(super) struct OmicronZones {
    zones: IdMap<OmicronZone>,
    mount_config: Arc<MountConfig>,
    timesync_config: TimeSyncConfig,
}

impl OmicronZones {
    pub(super) fn new(
        mount_config: Arc<MountConfig>,
        timesync_config: TimeSyncConfig,
    ) -> Self {
        Self { zones: IdMap::default(), mount_config, timesync_config }
    }

    pub(crate) fn has_retryable_error(&self) -> bool {
        self.zones.iter().any(|zone| match &zone.state {
            ZoneState::Running { .. } => false,
            // Assume any error is retryable. This might not be right? We can
            // narrow this down in the future.
            ZoneState::PartiallyShutDown { .. }
            | ZoneState::FailedToStart(_) => true,
        })
    }

    pub(crate) fn to_inventory(
        &self,
    ) -> BTreeMap<OmicronZoneUuid, ConfigReconcilerInventoryResult> {
        self.zones
            .iter()
            .map(|zone| match &zone.state {
                ZoneState::Running { .. } => {
                    (zone.config.id, ConfigReconcilerInventoryResult::Ok)
                }
                ZoneState::PartiallyShutDown { err, .. } => (
                    zone.config.id,
                    ConfigReconcilerInventoryResult::Err {
                        message: InlineErrorChain::new(err).to_string(),
                    },
                ),
                ZoneState::FailedToStart(err) => (
                    zone.config.id,
                    ConfigReconcilerInventoryResult::Err {
                        message: InlineErrorChain::new(err).to_string(),
                    },
                ),
            })
            .collect()
    }

    /// Attempt to shut down any zones that aren't present in `desired_zones`,
    /// or that weren't present in some prior call but which didn't succeed in
    /// shutting down and are in a partially-shut-down state.
    ///
    /// On failure, returns the number of zones that failed to shut down.
    pub(super) async fn shut_down_zones_if_needed<T: SledAgentFacilities>(
        &mut self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        resolver_status: &ResolverStatus,
        internal_disks: &InternalDisks,
        sled_agent_facilities: &T,
        log: &Logger,
    ) -> Result<(), NonZeroUsize> {
        self.shut_down_zones_if_needed_impl(
            desired_zones,
            sled_agent_facilities,
            &RealZoneFacilities { resolver_status, internal_disks },
            log,
        )
        .await
    }

    async fn shut_down_zones_if_needed_impl<
        T: SledAgentFacilities,
        U: ZoneFacilities,
    >(
        &mut self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        sled_agent_facilities: &T,
        zone_facilities: &U,
        log: &Logger,
    ) -> Result<(), NonZeroUsize> {
        // Filter desired zones down to just those that we need to stop. See
        // [`ZoneState`] for more discussion of why we're willing (or unwilling)
        // to stop zones in various current states.
        let mut zones_to_shut_down = Vec::new();
        for mut z in self.zones.iter_mut() {
            let zone_name = z.config.zone_name();

            let should_shut_down = match desired_zones.get(&z.config.id) {
                // We no longer want this zone to be running.
                None => true,

                // We do want this zone to be running; check the current
                // state.
                Some(desired_config) => match &z.state {
                    // Only shut down a running zone if the desired config
                    // has changes that necessitate a restart.
                    ZoneState::Running {
                        location: existing_location, ..
                    } => {
                        let prepared_desired_zone = zone_facilities
                            .prepare_omicron_zone(log, desired_config);

                        debug!(
                            log,
                            "obtained desired location for zone, \
                             determining whether to restart it";
                            "zone" => &zone_name,
                            "existing-location" => ?existing_location,
                            "desired-file-source" =>
                                ?prepared_desired_zone.file_source(),
                        );

                        if does_new_config_require_zone_restart(
                            &z.config,
                            existing_location,
                            &prepared_desired_zone,
                            log,
                        ) {
                            info!(
                                log,
                                "starting shutdown of running zone; config \
                                 has changed";
                                "zone" => &zone_name,
                                "old-config" => ?z.config,
                                "new-config" => ?desired_config,
                            );
                            true
                        } else {
                            // The desired `OmicronZoneConfig` might or might
                            // not be the same as the existing one, particularly
                            // in case of noop image source switches and mupdate
                            // overrides going away. If we're not shutting down
                            // the zone, always replace the config with the new
                            // one.
                            //
                            // This can probably be optimized in the future, but
                            // is good enough for now.
                            z.config = desired_config.clone();
                            // We don't update the location, though, because it
                            // represents the point-in-time determination of
                            // when the zone was actually started.

                            false
                        }
                    }

                    // Shut down zones in other states, but log why first.
                    ZoneState::PartiallyShutDown { err, .. } => {
                        info!(
                            log,
                            "resuming shutdown of partially-shut-down zone";
                            "zone" => zone_name,
                            "prev_err" => InlineErrorChain::new(err),
                        );
                        true
                    }

                    ZoneState::FailedToStart(err) => {
                        info!(
                            log,
                            "starting shutdown of a failed-to-start zone";
                            "zone" => zone_name,
                            "prev_err" => InlineErrorChain::new(err),
                        );
                        true
                    }
                },
            };

            if should_shut_down {
                zones_to_shut_down.push(z.id());
            }
        }

        // Map the zones to the futures that will try to shut them down.
        let shutdown_futures = zones_to_shut_down.iter().map(|zone_id| {
            let zone = self.zones.get(zone_id).expect(
                "zones_to_shut_down only has IDs present in self.zones",
            );
            zone.try_shut_down(sled_agent_facilities, zone_facilities, log)
                .map(|result| (zone.config.id, result))
        });

        // Concurrently stop the zones, then record the results.
        let shutdown_results = future::join_all(shutdown_futures).await;

        let mut nfailed = 0;
        for (zone_id, result) in shutdown_results {
            match result {
                Ok(()) => {
                    self.zones.remove(&zone_id);
                }
                Err((state, err)) => {
                    nfailed += 1;
                    self.zones
                        .get_mut(&zone_id)
                        .expect("shutdown task operates on existing zone")
                        .state = ZoneState::PartiallyShutDown { state, err };
                }
            }
        }

        // Report how many shutdowns failed.
        match NonZeroUsize::new(nfailed) {
            None => Ok(()),
            Some(nfailed) => Err(nfailed),
        }
    }

    /// Attempt to start any zones that are present in `desired_zones` but not
    /// in `self`.
    #[expect(clippy::too_many_arguments)]
    pub(super) async fn start_zones_if_needed<T: SledAgentFacilities>(
        &mut self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        resolver_status: &ResolverStatus,
        internal_disks: &InternalDisks,
        sled_agent_facilities: &T,
        is_time_synchronized: bool,
        datasets: &OmicronDatasets,
        log: &Logger,
    ) {
        self.start_zones_if_needed_impl(
            desired_zones,
            sled_agent_facilities,
            &RealZoneFacilities { resolver_status, internal_disks },
            is_time_synchronized,
            datasets,
            log,
        )
        .await
    }

    async fn start_zones_if_needed_impl<
        T: SledAgentFacilities,
        U: ZoneFacilities,
    >(
        &mut self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        sled_agent_facilities: &T,
        zone_facilities: &U,
        is_time_synchronized: bool,
        datasets: &OmicronDatasets,
        log: &Logger,
    ) {
        // Filter desired zones down to just those that we need to start. See
        // [`ZoneState`] for more discussion of why we're willing (or unwilling)
        // to start zones in various current states.
        let zones_to_start = desired_zones.iter().filter(|zone| {
            match self.zones.get(&zone.id).map(|z| &z.state) {
                // This is entirely new zone - start it!
                None => {
                    info!(
                        log, "starting new zone";
                        "config" => ?zone,
                    );
                    true
                }

                // This is a zone we've tried to start before; try again!
                Some(ZoneState::FailedToStart(err)) => {
                    info!(
                        log,
                        "retrying start of zone";
                        "config" => ?zone,
                        "prev_err" => InlineErrorChain::new(err),
                    );
                    true
                }

                // We want this zone to be running now but previously needed
                // to stop it and failed to do so: don't try to start it
                // again until we succeed in stopping it.
                Some(ZoneState::PartiallyShutDown { err, .. }) => {
                    warn!(
                        log,
                        "refusing to start zone (partially shut down)";
                        "config" => ?zone,
                        "shutdown_err" => InlineErrorChain::new(err),
                    );
                    false
                }

                // The common case: this zone is already running.
                Some(ZoneState::Running { .. }) => false,
            }
        });

        // Build up the futures for starting each zone.
        let start_futures = zones_to_start.map(|zone| {
            let prepared_zone = zone_facilities.prepare_omicron_zone(log, zone);

            debug!(
                log,
                "obtained file source for zone, going to start it";
                "zone_name" => prepared_zone.config().zone_name(),
                "file_source" => ?prepared_zone.file_source(),
            );

            self.start_single_zone(
                prepared_zone,
                sled_agent_facilities,
                zone_facilities,
                is_time_synchronized,
                datasets,
                log,
            )
            .map(move |result| (zone.clone(), result))
        });

        // Concurrently start all zones, then record the results.
        let start_results = future::join_all(start_futures).await;
        for (config, result) in start_results {
            let state = match result {
                Ok(state) => state,
                Err(err) => ZoneState::FailedToStart(err),
            };
            self.zones.insert(OmicronZone { config, state });
        }
    }

    async fn start_single_zone<T: SledAgentFacilities, U: ZoneFacilities>(
        &self,
        zone: PreparedOmicronZone<'_>,
        sled_agent_facilities: &T,
        zone_facilities: &U,
        is_time_synchronized: bool,
        datasets: &OmicronDatasets,
        log: &Logger,
    ) -> Result<ZoneState, ZoneStartError> {
        // Ensure no zone by this name exists. This should only happen in the
        // event of a sled-agent restart, in which case all the zones the
        // previous sled-agent process had started are still running.
        if let Some(state) = self
            .ensure_removed_before_starting(
                zone.config(),
                sled_agent_facilities,
                zone_facilities,
                log,
            )
            .await?
        {
            return Ok(state);
        }

        // The zone cannot be started if we can't convert the prepared location
        // into a running one due to a mupdate override error.
        let location = match zone.file_source().location.to_running() {
            Ok(location) => location,
            Err(error) => {
                return Err(ZoneStartError::MupdateOverride(error));
            }
        };

        // Ensure that time is sync'd, if needed by this zone.
        if zone.config().zone_type.requires_timesync() && !is_time_synchronized
        {
            return Err(ZoneStartError::TimeNotSynchronized);
        }

        // Ensure all dataset dependencies of this zone are okay.
        let zone_root_path = datasets
            .validate_zone_storage(zone.config(), &self.mount_config)?;

        // The zone is not running - start it.
        match sled_agent_facilities
            .start_omicron_zone(zone, zone_root_path)
            .await
        {
            Ok(running_zone) => Ok(ZoneState::Running {
                running_zone: Arc::new(running_zone),
                location,
            }),
            Err(err) => Err(ZoneStartError::SledAgentStartFailed(err)),
        }
    }

    // The return type of this function is strange. The possible values are:
    //
    // * `Ok(None)` - the zone is not running
    // * `Err(_)` - we had an error related to zone startup
    // * `Ok(Some(state))` - the zone is still running and is in some state that
    //    we need to do more work to handle (e.g., we found a running zone but
    //    failed to shut it down cleanly, in which case we'll return
    //    `Ok(Some(ZoneState::PartiallyShutDown { .. }))`). In this case, our
    //    caller should do no further work to try to start `zone`, and should
    //    instead bubble the `state` up to be recorded.
    async fn ensure_removed_before_starting<
        T: SledAgentFacilities,
        U: ZoneFacilities,
    >(
        &self,
        zone: &OmicronZoneConfig,
        sled_agent_facilities: &T,
        zone_facilities: &U,
        log: &Logger,
    ) -> Result<Option<ZoneState>, ZoneStartError> {
        let zone_name = ZoneName::new(zone);

        // If no zone by this name exists, there's nothing to remove.
        if !zone_facilities.zone_with_name_exists(&zone_name).await? {
            return Ok(None);
        }

        // NOTE: We might want to tell the sled-agent's metrics task to stop
        // tracking any links in this zone. However, we don't have very easy
        // access to them, without running a command in the zone. These links
        // are about to be deleted, and the metrics task will expire them after
        // a while anyway, but it might be worth the trouble to do that in the
        // future.
        //
        // Skipping that for now, follow the normal zone shutdown process
        // _after_ metrics (i.e., shut down and clean up the zone).
        //
        // TODO-correctness There's a (very unlikely?) chance that this cleanup
        // isn't right: if the running zone (which we have no active knowledge
        // of) was started with a different `OmicronZoneConfig`, the cleanup
        // steps we do here might not be right.
        match resume_shutdown_from_stop(
            zone,
            sled_agent_facilities,
            zone_facilities,
            &zone_name,
            log,
        )
        .await
        {
            Ok(()) => Ok(None),
            Err((state, err)) => {
                // We didn't fail to _start_ the zone, so it doesn't make sense
                // to return a `ZoneStartError`, but the zone is in a state that
                // we need to remember.
                Ok(Some(ZoneState::PartiallyShutDown { state, err }))
            }
        }
    }

    /// Check the timesync status from a running NTP zone (if it exists)
    pub(super) async fn check_timesync(&self, log: &Logger) -> TimeSyncStatus {
        match &self.timesync_config {
            TimeSyncConfig::Normal => {
                match self.timesync_status_from_ntp_zone(log).await {
                    Ok(timesync) => TimeSyncStatus::TimeSync(timesync),
                    Err(err) => {
                        TimeSyncStatus::FailedToGetSyncStatus(Arc::new(err))
                    }
                }
            }
            TimeSyncConfig::Skip => TimeSyncStatus::ConfiguredToSkip,
        }
    }

    async fn timesync_status_from_ntp_zone(
        &self,
        log: &Logger,
    ) -> Result<TimeSync, TimeSyncError> {
        // Get the one and only running NTP zone, or return an error.
        let mut ntp_admin_addresses = self.zones.iter().filter_map(|z| {
            if !z.config.zone_type.is_ntp() {
                return None;
            }
            // TODO(https://github.com/oxidecomputer/omicron/issues/6796):
            //
            // We could avoid hard-coding the port here if the zone was fully
            // specified to include both NTP and Admin server addresses.
            let mut addr = match z.config.zone_type {
                OmicronZoneType::BoundaryNtp { address, .. } => address,
                OmicronZoneType::InternalNtp { address, .. } => address,
                _ => return None,
            };
            addr.set_port(omicron_common::address::NTP_ADMIN_PORT);

            match &z.state {
                ZoneState::Running { .. } => Some(addr),
                ZoneState::PartiallyShutDown { .. }
                | ZoneState::FailedToStart(_) => None,
            }
        });
        let ntp_admin_address = ntp_admin_addresses
            .next()
            .ok_or(TimeSyncError::NoRunningNtpZone)?;
        if ntp_admin_addresses.next().is_some() {
            return Err(TimeSyncError::MultipleRunningNtpZones);
        }

        let client = ntp_admin_client::Client::new(
            &format!("http://{ntp_admin_address}"),
            log.clone(),
        );

        let timesync = client.timesync().await?.into_inner();

        Ok(timesync)
    }
}

#[derive(Debug)]
struct OmicronZone {
    config: OmicronZoneConfig,
    state: ZoneState,
}

impl IdMappable for OmicronZone {
    type Id = OmicronZoneUuid;

    fn id(&self) -> Self::Id {
        self.config.id
    }
}

impl OmicronZone {
    async fn try_shut_down<T: SledAgentFacilities, U: ZoneFacilities>(
        &self,
        sled_agent_facilities: &T,
        zone_facilities: &U,
        log: &Logger,
    ) -> Result<(), (PartiallyShutDownState, ZoneShutdownError)> {
        let log = log.new(slog::o!("zone" => self.config.zone_name()));

        match &self.state {
            ZoneState::Running { running_zone, location: _ } => {
                info!(log, "shutting down running zone");

                // We only try once to create a zone bundle; if this fails we
                // move on to the rest of the shutdown process.
                if let Err(err) = sled_agent_facilities
                    .zone_bundle_create(
                        running_zone,
                        ZoneBundleCause::UnexpectedZone,
                    )
                    .await
                {
                    warn!(
                        log,
                        "Failed to take bundle of zone we're shutting down";
                        InlineErrorChain::new(err.as_ref()),
                    );
                }

                self.resume_shutdown_from_untrack_metrics(
                    sled_agent_facilities,
                    zone_facilities,
                    running_zone,
                    &log,
                )
                .await
            }
            ZoneState::PartiallyShutDown {
                state:
                    PartiallyShutDownState::FailedToUntrackMetrics(running_zone),
                ..
            } => {
                self.resume_shutdown_from_untrack_metrics(
                    sled_agent_facilities,
                    zone_facilities,
                    running_zone,
                    &log,
                )
                .await
            }
            ZoneState::PartiallyShutDown {
                state: PartiallyShutDownState::FailedToStop(running_zone),
                ..
            } => {
                resume_shutdown_from_stop(
                    &self.config,
                    sled_agent_facilities,
                    zone_facilities,
                    running_zone,
                    &log,
                )
                .await
            }
            ZoneState::PartiallyShutDown {
                state: PartiallyShutDownState::FailedToCleanUp,
                ..
            } => {
                resume_shutdown_from_cleanup(
                    &self.config,
                    sled_agent_facilities,
                    zone_facilities,
                    &log,
                )
                .await
            }
            // With these errors, we never even tried to start the zone, so
            // there's no cleanup required: we can just return.
            ZoneState::FailedToStart(ZoneStartError::MupdateOverride(_))
            | ZoneState::FailedToStart(ZoneStartError::TimeNotSynchronized)
            | ZoneState::FailedToStart(ZoneStartError::CheckZoneExists(_))
            | ZoneState::FailedToStart(ZoneStartError::DatasetDependency(_)) => {
                Ok(())
            }
            ZoneState::FailedToStart(ZoneStartError::SledAgentStartFailed(
                err,
            )) => {
                // TODO-correctness What do we need to do to try to shut down a
                // zone that we tried to start? We need fine-grained status of
                // what startup things succeeded that need to be cleaned up. For
                // now, warn that we're assuming we have no work to do.
                warn!(
                    log,
                    "need to shut down zone that failed to start, but this \
                     is currently unimplemented: assuming no cleanup work \
                     required";
                    "start-err" => InlineErrorChain::new(err.as_ref()),
                );
                Ok(())
            }
        }
    }

    async fn resume_shutdown_from_untrack_metrics<
        T: SledAgentFacilities,
        U: ZoneFacilities,
    >(
        &self,
        sled_agent_facilities: &T,
        zone_facilities: &U,
        running_zone: &Arc<RunningZone>,
        log: &Logger,
    ) -> Result<(), (PartiallyShutDownState, ZoneShutdownError)> {
        if let Err(err) = sled_agent_facilities
            .metrics_untrack_zone_links(running_zone)
            .map_err(ZoneShutdownError::UntrackMetrics)
        {
            warn!(
                log,
                "Failed to untrack metrics for running zone";
                InlineErrorChain::new(&err),
            );
            return Err((
                PartiallyShutDownState::FailedToUntrackMetrics(Arc::clone(
                    running_zone,
                )),
                err,
            ));
        }

        resume_shutdown_from_stop(
            &self.config,
            sled_agent_facilities,
            zone_facilities,
            &ZoneName::from(running_zone.name()),
            log,
        )
        .await
    }
}

async fn resume_shutdown_from_stop<
    T: SledAgentFacilities,
    U: ZoneFacilities,
>(
    config: &OmicronZoneConfig,
    sled_agent_facilities: &T,
    zone_facilities: &U,
    zone_name: &ZoneName<'_>,
    log: &Logger,
) -> Result<(), (PartiallyShutDownState, ZoneShutdownError)> {
    if let Err(err) = zone_facilities.halt_zone(zone_name, log).await {
        warn!(
            log,
            "Failed to stop running zone";
            InlineErrorChain::new(&err),
        );
        return Err((
            PartiallyShutDownState::FailedToStop(zone_name.to_static()),
            err,
        ));
    }

    resume_shutdown_from_cleanup(
        config,
        sled_agent_facilities,
        zone_facilities,
        log,
    )
    .await
}

async fn resume_shutdown_from_cleanup<
    T: SledAgentFacilities,
    U: ZoneFacilities,
>(
    config: &OmicronZoneConfig,
    sled_agent_facilities: &T,
    zone_facilities: &U,
    log: &Logger,
) -> Result<(), (PartiallyShutDownState, ZoneShutdownError)> {
    // Special teardown for internal DNS zones: delete the global zone
    // address we created for it, and tell DDM to stop advertising the
    // prefix of that address.
    if let OmicronZoneType::InternalDns {
        gz_address, gz_address_index, ..
    } = &config.zone_type
    {
        let addrobj = AddrObject::new(
            &sled_agent_facilities.underlay_vnic().0,
            &internal_dns_addrobj_name(*gz_address_index),
        )
        .expect("internal DNS address object name is well-formed");
        if let Err(err) = zone_facilities.delete_gz_address(addrobj).await {
            warn!(
                log,
                "Failed to delete internal-dns gz address";
                InlineErrorChain::new(&err),
            );
            return Err((PartiallyShutDownState::FailedToCleanUp, err));
        }

        sled_agent_facilities
            .ddm_remove_internal_dns_prefix(Ipv6Subnet::new(*gz_address));
    }

    Ok(())
}

fn internal_dns_addrobj_name(gz_address_index: u32) -> String {
    format!("internaldns{gz_address_index}")
}

/// State of a zone.
///
/// The only way to have any `ZoneState` is for a zone to have been in an
/// `OmicronSledConfig` that was passed to
/// [`OmicronZones::start_zones_if_needed()`] at some point.
/// `start_zones_if_needed()` and `shut_down_zones_if_needed()` act on these
/// states as documented below on each variant.
#[derive(Debug)]
enum ZoneState {
    /// A zone that is currently running.
    ///
    /// `shut_down_zones_if_needed()` will attempt to shut down a zone in this
    /// state if either:
    ///
    /// 1. The zone is no longer present in the current `OmicronSledConfig`
    /// 2. The zone is still present but its properties have changed. (In the
    ///    future we probably want to be able to reconfigure some zone
    ///    properties on the fly; for now, however, any property change requires
    ///    us to shut down the zone and restart it.)
    ///
    /// `start_zones_if_needed()` will skip zones in this state.
    ///
    /// We keep the running zone in an `Arc` so we can "move" it into some
    /// [`PartiallyShutDownState`] variants via [`Arc::clone()`]. (We can't
    /// truly move ownership of it out a `&mut ZoneState` without some kind of
    /// shenanigans like keep an `Option<ZoneState>` or temporarily replacing
    /// the state with some sentinel value. Using an `Arc` is one workaround.)
    Running {
        /// A reference to the running zone.
        running_zone: Arc<RunningZone>,

        /// The location of the zone, as of the time the zone was initially
        /// started.
        location: RunningZoneImageLocation,
    },

    /// A zone that we tried to shut down but failed at some point during the
    /// shutdown process (tracked by `state`).
    ///
    /// `shut_down_zones_if_needed()` will always try to resume shutting down a
    /// zone in this state, picking up from the step that failed.
    ///
    /// `start_zones_if_needed()` will skip zones in this state.
    PartiallyShutDown { state: PartiallyShutDownState, err: ZoneShutdownError },

    /// A zone that we tried to start but failed at some point during the start
    /// process. (We currently delegate "the start process" back to sled-agent,
    /// so have no specific tracking for this yet.)
    ///
    /// `shut_down_zones_if_needed()` will always try to shut down a zone in
    /// this state. (As noted above, once we track the start process more
    /// carefully this may not make sense. But for now we assume any kind of
    /// partially-started zone might need to be shutdown.)
    ///
    /// `start_zones_if_needed()` will try to start a zone in this state if it's
    /// still present in the current `OmicronSledConfig`. However, in practice
    /// it's currently not possible for `start_zones_if_needed()` to see a zone
    /// in this state: the reconciler always calls `shut_down_zones_if_needed()`
    /// first, which will attempt to shut down any zone in this state,
    /// transitioning it to either `PartiallyShutDown` (if shutdown failed) or
    /// removed entirely (if shutdown succeeded).
    FailedToStart(ZoneStartError),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ZoneName<'a>(Cow<'a, str>);

impl<'a> From<&'a str> for ZoneName<'a> {
    fn from(value: &'a str) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl From<String> for ZoneName<'static> {
    fn from(value: String) -> Self {
        Self(Cow::Owned(value))
    }
}

impl ZoneName<'_> {
    fn new(config: &OmicronZoneConfig) -> Self {
        Self(Cow::Owned(config.zone_name()))
    }

    fn to_string(&self) -> String {
        self.0.clone().into_owned()
    }

    fn to_static(&self) -> ZoneName<'static> {
        ZoneName(Cow::Owned(self.0.clone().into_owned()))
    }
}

#[derive(Debug)]
enum PartiallyShutDownState {
    FailedToUntrackMetrics(Arc<RunningZone>),
    FailedToStop(ZoneName<'static>),
    FailedToCleanUp,
}

#[derive(Debug, thiserror::Error)]
enum ZoneStartError {
    #[error("could not determine whether zone already exists")]
    CheckZoneExists(#[from] CheckZoneExistsError),
    #[error(
        "mupdate override error; zone with Artifact \
         image source cannot be started"
    )]
    MupdateOverride(#[source] MupdateOverrideReadError),
    #[error("Time not yet synchronized")]
    TimeNotSynchronized,
    #[error(transparent)]
    DatasetDependency(#[from] ZoneDatasetDependencyError),
    #[error("sled agent failed to start service")]
    SledAgentStartFailed(#[source] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
enum CheckZoneExistsError {
    #[error("failed to find zone {name}")]
    FindByName {
        name: String,
        #[source]
        err: AdmError,
    },
}

#[derive(Debug, thiserror::Error)]
enum ZoneShutdownError {
    #[error("failed to untrack metrics")]
    UntrackMetrics(#[source] anyhow::Error),
    #[error("failed to halt and remove zone")]
    HaltAndRemove(#[source] AdmError),
    #[error("failed to delete global zone address object")]
    DeleteGzAddrObj(#[source] DeleteAddressError),
}

trait ZoneFacilities {
    fn prepare_omicron_zone<'cfg>(
        &self,
        log: &Logger,
        zone_config: &'cfg OmicronZoneConfig,
    ) -> PreparedOmicronZone<'cfg>;

    async fn zone_with_name_exists(
        &self,
        name: &ZoneName<'_>,
    ) -> Result<bool, CheckZoneExistsError>;

    async fn halt_zone(
        &self,
        zone: &ZoneName,
        log: &Logger,
    ) -> Result<(), ZoneShutdownError>;

    async fn delete_gz_address(
        &self,
        addrobj: AddrObject,
    ) -> Result<(), ZoneShutdownError>;
}

struct RealZoneFacilities<'a> {
    resolver_status: &'a ResolverStatus,
    internal_disks: &'a InternalDisks,
}

impl ZoneFacilities for RealZoneFacilities<'_> {
    fn prepare_omicron_zone<'cfg>(
        &self,
        log: &Logger,
        zone_config: &'cfg OmicronZoneConfig,
    ) -> PreparedOmicronZone<'cfg> {
        self.resolver_status.prepare_omicron_zone(
            log,
            zone_config,
            self.internal_disks,
        )
    }

    async fn zone_with_name_exists(
        &self,
        name: &ZoneName<'_>,
    ) -> Result<bool, CheckZoneExistsError> {
        match Zones::real_api().find(&name.0).await {
            Ok(maybe_zone) => Ok(maybe_zone.is_some()),
            Err(err) => Err(CheckZoneExistsError::FindByName {
                name: name.to_string(),
                err,
            }),
        }
    }

    async fn halt_zone(
        &self,
        zone: &ZoneName<'_>,
        log: &Logger,
    ) -> Result<(), ZoneShutdownError> {
        // We don't use `RunningZone::stop()` here because it doesn't allow
        // repeated attempts after a failure
        // (https://github.com/oxidecomputer/omicron/issues/7881) and because in
        // the case of "an unexpected zone is running", all we have is the name.
        // Instead, use the lower-level `Zones::halt_and_remove_logged()`
        // function directly.
        Zones::real_api()
            .halt_and_remove_logged(log, &zone.0)
            .await
            .map_err(ZoneShutdownError::HaltAndRemove)
    }

    async fn delete_gz_address(
        &self,
        addrobj: AddrObject,
    ) -> Result<(), ZoneShutdownError> {
        Zones::delete_address(None, &addrobj)
            .await
            .map_err(ZoneShutdownError::DeleteGzAddrObj)
    }
}

// It's possible some new zone configs do not require us to restart the zone.
// A trivial case is if the config didn't change. We currently support one
// nontrivial kind of change:
//
// If the only change is to the location, and the change is from
// `InstallDataset` to `Artifact { hash }` (or vice versa), and the hash of the
// zone in the install dataset is exactly equal to the hash specified by
// `Artifact { hash }`, then we know the zone has not had any meaningful
// changes: it's running the exact bits with the exact config it would have if
// we restarted it, so we don't need to.
fn does_new_config_require_zone_restart(
    existing_config: &OmicronZoneConfig,
    existing_location: &RunningZoneImageLocation,
    desired_prepared_zone: &PreparedOmicronZone<'_>,
    log: &Logger,
) -> bool {
    // Compare the before and after *locations* (this considers
    // potentially-present mupdate overrides).
    let desired_location = &desired_prepared_zone.file_source().location;

    match (existing_location, desired_location) {
        (
            RunningZoneImageLocation::Artifact { hash: existing_hash },
            OmicronZoneImageLocation::Artifact { hash: Ok(desired_hash) },
        ) => {
            if existing_hash == desired_hash
                && config_differs_only_by_image_source(
                    existing_config,
                    desired_prepared_zone.config(),
                )
            {
                // No changes other than the image source.
                false
            } else {
                // Non-image source changes require a bounce.
                true
            }
        }
        (
            RunningZoneImageLocation::InstallDataset { hash: existing_hash },
            OmicronZoneImageLocation::InstallDataset { hash: Ok(desired_hash) },
        ) => {
            // In this case, the existing hash and the desired hash must match,
            // because the install dataset zones don't change within a
            // particular sled-agent process's lifetime.
            if existing_hash == desired_hash {
                if config_differs_only_by_image_source(
                    existing_config,
                    desired_prepared_zone.config(),
                ) {
                    // No changes other than the image source.
                    false
                } else {
                    // Non-image source changes require a bounce.
                    true
                }
            } else {
                error!(
                    log,
                    "existing hash on install dataset \
                     does not match desired hash -- this should never happen. \
                     Bouncing the zone in any case.";
                     "zone_name" => desired_prepared_zone.config().zone_name(),
                     "existing_hash" => %existing_hash,
                     "desired_hash" => %desired_hash,
                );
                true
            }
        }
        (
            RunningZoneImageLocation::Artifact { hash: existing_hash },
            OmicronZoneImageLocation::InstallDataset { hash: Ok(desired_hash) },
        )
        | (
            RunningZoneImageLocation::InstallDataset { hash: existing_hash },
            OmicronZoneImageLocation::Artifact { hash: Ok(desired_hash) },
        ) => {
            if existing_hash == desired_hash
                && config_differs_only_by_image_source(
                    existing_config,
                    desired_prepared_zone.config(),
                )
            {
                info!(
                    log,
                    "updating config for zone without restarting; \
                     only change is zone location (install dataset <-> \
                     artifact; hash of zone image matches in both)";
                    "zone_name" => desired_prepared_zone.config().zone_name(),
                    "hash" => %desired_hash,
                );
                false
            } else {
                true
            }
        }
        (
            RunningZoneImageLocation::Ramdisk,
            OmicronZoneImageLocation::InstallDataset {
                hash: Err(desired_error),
            },
        ) => {
            // Err means we're going to start the desired zone from the ramdisk
            // (if it is at all successful). Log a warning, but otherwise don't
            // bounce the zone.
            warn!(
                log,
                "existing zone running from ramdisk, and desired zone \
                 would be running from ramdisk; not restarting zone";
                "zone_name" => desired_prepared_zone.config().zone_name(),
                "desired_error" => InlineErrorChain::new(desired_error),
            );
            false
        }
        (
            RunningZoneImageLocation::InstallDataset { hash: existing_hash },
            OmicronZoneImageLocation::InstallDataset {
                hash: Err(desired_error),
            },
        ) => {
            // In this case, an existing zone was run from the install dataset,
            // but an error occurred after that. This would indicate some kind
            // of issue fetching the zone from the install dataset. Hopefully
            // the failure is transient -- we don't want to bounce the zone in
            // this case.
            warn!(
                log,
                "existing zone running from install dataset, but an error \
                 occurred looking up the zone image from the install dataset; \
                 not restarting zone";
                "zone_name" => desired_prepared_zone.config().zone_name(),
                "existing_hash" => %existing_hash,
                "desired_error" => InlineErrorChain::new(desired_error),
            );
            false
        }
        (
            RunningZoneImageLocation::Artifact { hash: existing_hash },
            OmicronZoneImageLocation::InstallDataset {
                hash: Err(desired_error),
            },
        ) => {
            // A transition from artifact to install dataset, but an error
            // occurred while looking up the zone in the install dataset. We
            // should bounce the zone in this case to obey the instruction to
            // change from artifact to install dataset.
            warn!(
                log,
                "existing zone running from artifact, transitioning to \
                 install dataset, but an error occurred looking up the zone \
                 image from the install dataset; bouncing zone, but it will
                 be restarted from the ramdisk";
                "zone_name" => desired_prepared_zone.config().zone_name(),
                "existing_hash" => %existing_hash,
                "desired_error" => InlineErrorChain::new(desired_error),
            );
            true
        }
        (
            _,
            OmicronZoneImageLocation::Artifact { hash: Err(desired_error) },
        ) => {
            // We're doomed in this case -- will not be able to start the zone.
            // Log an error nothing this.
            error!(
                log,
                "desired zone would run from Artifact, but there was an error; \
                 shutting down the existing zone, but starting the new zone will \
                 fail";
                "zone_name" => desired_prepared_zone.config().zone_name(),
                "existing_location" => ?existing_location,
                "desired_error" => InlineErrorChain::new(desired_error),
            );
            true
        }
        (
            RunningZoneImageLocation::Ramdisk,
            OmicronZoneImageLocation::InstallDataset { hash: Ok(desired_hash) },
        ) => {
            // Some kind of transient error cleared up -- great, let's start the
            // zone from the install dataset.
            info!(
                log,
                "existing zone with InstallDataset source was run from ramdisk \
                 due to an error, but the error appears to have been transient; \
                 restarting the zone from the install dataset";
                "zone_name" => desired_prepared_zone.config().zone_name(),
                "desired_hash" => %desired_hash,
            );
            true
        }
        (
            RunningZoneImageLocation::Ramdisk,
            OmicronZoneImageLocation::Artifact { hash: Ok(desired_hash) },
        ) => {
            info!(
                log,
                "existing zone with InstallDataset source was run from ramdisk \
                 due to an error, and desired zone is Artifact; restarting the zone \
                 from the artifact store";
                "zone_name" => desired_prepared_zone.config().zone_name(),
                "desired_hash" => %desired_hash,
            );
            true
        }
    }
}

fn config_differs_only_by_image_source(
    config1: &OmicronZoneConfig,
    config2: &OmicronZoneConfig,
) -> bool {
    // Unpack both configs, ignoring the one field we want to ignore.
    let OmicronZoneConfig {
        id: id1,
        filesystem_pool: filesystem_pool1,
        zone_type: zone_type1,
        image_source: _,
    } = config1;
    let OmicronZoneConfig {
        id: id2,
        filesystem_pool: filesystem_pool2,
        zone_type: zone_type2,
        image_source: _,
    } = config2;

    id1 == id2
        && filesystem_pool1 == filesystem_pool2
        && zone_type1 == zone_type2
}

/// An extension trait for `ResolverStatus`.
///
/// This trait only exists because it refers to types that aren't available
/// within `sled-agent-types`.
pub trait ResolverStatusExt {
    /// Look up the file source for an Omicron zone.
    fn omicron_file_source(
        &self,
        log: &slog::Logger,
        zone_kind: ZoneKind,
        image_source: &OmicronZoneImageSource,
        internal_disks: &InternalDisks,
    ) -> OmicronZoneFileSource;

    /// Prepare an Omicron zone for installation.
    fn prepare_omicron_zone<'a>(
        &self,
        log: &slog::Logger,
        zone_config: &'a OmicronZoneConfig,
        internal_disks: &InternalDisks,
    ) -> PreparedOmicronZone<'a> {
        let file_source = self.omicron_file_source(
            log,
            zone_config.zone_type.kind(),
            &zone_config.image_source,
            internal_disks,
        );
        PreparedOmicronZone::new(zone_config, file_source)
    }
}

impl ResolverStatusExt for ResolverStatus {
    fn omicron_file_source(
        &self,
        log: &slog::Logger,
        zone_kind: ZoneKind,
        image_source: &OmicronZoneImageSource,
        internal_disks: &InternalDisks,
    ) -> OmicronZoneFileSource {
        match image_source {
            OmicronZoneImageSource::InstallDataset => {
                let file_name = zone_kind.artifact_in_install_dataset();

                // There's always at least one image path (the RAM disk below).
                let mut search_paths = Vec::with_capacity(1);

                // Inject an image path if requested by a test.
                if let Some(path) = &self.image_directory_override {
                    search_paths.push(path.clone());
                };

                // Any zones not part of the RAM disk are managed via the zone
                // manifest.
                let hash = install_dataset_hash(
                    log,
                    self,
                    zone_kind,
                    internal_disks,
                    |path| search_paths.push(path),
                );

                // Look for the image in the RAM disk as a fallback. Note that
                // install dataset images are not stored on the RAM disk in
                // production, just in development or test workflows.
                search_paths.push(Utf8PathBuf::from(RAMDISK_IMAGE_PATH));

                OmicronZoneFileSource {
                    location: OmicronZoneImageLocation::InstallDataset { hash },
                    file_source: ZoneImageFileSource {
                        file_name: file_name.to_owned(),
                        search_paths,
                    },
                }
            }
            OmicronZoneImageSource::Artifact { hash } => {
                // TODO: implement mupdate override here.
                //
                // Search both artifact datasets. This iterator starts with the
                // dataset for the boot disk (if it exists), and then is followed
                // by all other disks.
                let search_paths =
                    internal_disks.all_artifact_datasets().collect();
                OmicronZoneFileSource {
                    // TODO: with mupdate overrides, return InstallDataset here
                    location: OmicronZoneImageLocation::Artifact {
                        hash: Ok(*hash),
                    },
                    file_source: ZoneImageFileSource {
                        file_name: hash.to_string(),
                        search_paths,
                    },
                }
            }
        }
    }
}

fn install_dataset_hash<F>(
    log: &slog::Logger,
    resolver_status: &ResolverStatus,
    zone_kind: ZoneKind,
    internal_disks: &InternalDisks,
    mut search_paths_cb: F,
) -> Result<ArtifactHash, ZoneImageLocationError>
where
    F: FnMut(Utf8PathBuf),
{
    // XXX: we ask for the boot zpool to be passed in here. But
    // `AllZoneImages` also caches the boot zpool. How should we
    // reconcile the two?
    let hash = if let Some(path) = internal_disks.boot_disk_install_dataset() {
        let hash = resolver_status.zone_manifest.zone_hash(zone_kind);
        match hash {
            Ok(hash) => {
                search_paths_cb(path);
                Ok(hash)
            }
            Err(error) => {
                error!(
                    log,
                    "zone {} not found in the boot disk zone manifest, \
                     not returning it as a source",
                    zone_kind.report_str();
                    "file_name" => zone_kind.artifact_in_install_dataset(),
                    "error" => InlineErrorChain::new(&error),
                );
                Err(ZoneImageLocationError::ZoneHash(error))
            }
        }
    } else {
        // The boot disk is not available, so we cannot add the
        // install dataset path from it.
        error!(
            log,
            "boot disk install dataset not available, \
             not returning it as a source";
            "zone_kind" => zone_kind.report_str(),
        );
        Err(ZoneImageLocationError::BootDiskMissing)
    };
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InternalDisks;
    use crate::dataset_serialization_task::DatasetEnsureError;
    use anyhow::anyhow;
    use assert_matches::assert_matches;
    use camino::Utf8PathBuf;
    use camino_tempfile::Utf8TempDir;
    use iddqd::IdOrdMap;
    use illumos_utils::dladm::Etherstub;
    use illumos_utils::dladm::EtherstubVnic;
    use illumos_utils::link::VnicAllocator;
    use illumos_utils::running_zone::ZoneBuilderFactory;
    use illumos_utils::zpool::PathInPool;
    use illumos_utils::zpool::ZpoolName;
    use illumos_utils::zpool::ZpoolOrRamdisk;
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
    use nexus_sled_agent_shared::inventory::ZoneKind;
    use omicron_common::address::SLED_PREFIX;
    use omicron_common::disk::DatasetConfig;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DatasetName;
    use omicron_common::disk::SharedDatasetConfig;
    use omicron_common::update::OmicronZoneManifest;
    use omicron_common::update::OmicronZoneManifestSource;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::MupdateOverrideUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_types::zone_images::ClearMupdateOverrideBootSuccess;
    use sled_agent_types::zone_images::ClearMupdateOverrideResult;
    use sled_agent_types::zone_images::MupdateOverrideStatus;
    use sled_agent_types::zone_images::OmicronZoneFileSource;
    use sled_agent_types::zone_images::ResolverStatus;
    use sled_agent_types::zone_images::ZoneManifestArtifactsResult;
    use sled_agent_types::zone_images::ZoneManifestStatus;
    use sled_agent_zone_images_examples::deserialize_error;
    use std::collections::BTreeSet;
    use std::collections::VecDeque;
    use std::net::Ipv6Addr;
    use std::sync::Mutex;
    use tufaceous_artifact::ArtifactHash;

    // Helper to construct a `RunningZone` even on non-illumos systems.
    struct FakeZoneBuilder {
        vnic_alloc: VnicAllocator<Etherstub>,
        factory: ZoneBuilderFactory,
        _tempdir: Utf8TempDir,
    }

    impl FakeZoneBuilder {
        fn new() -> Self {
            let vnic_source = Etherstub("teststubvnic".to_string());
            let vnic_alloc = VnicAllocator::new(
                "testvnic",
                vnic_source,
                illumos_utils::fakes::dladm::Dladm::new(),
            );
            let tempdir =
                Utf8TempDir::with_prefix("test-config-reconciler-zones-")
                    .expect("created temp dir");
            let factory = ZoneBuilderFactory::fake(
                Some(tempdir.path().as_str()),
                illumos_utils::fakes::zone::Zones::new(),
            );
            Self { vnic_alloc, factory, _tempdir: tempdir }
        }

        async fn make_running_zone(
            &self,
            name: &str,
            log: Logger,
        ) -> RunningZone {
            let installed_fake_zone = self
                .factory
                .builder()
                .with_zone_type(name)
                .with_zone_root_path(PathInPool {
                    pool: ZpoolOrRamdisk::Ramdisk,
                    path: "/test-zone-root".into(),
                })
                .with_underlay_vnic_allocator(&self.vnic_alloc)
                .with_log(log)
                .with_opte_ports(Vec::new())
                .with_links(Vec::new())
                .install()
                .await
                .expect("installed fake zone");

            RunningZone::fake_boot(0, installed_fake_zone)
        }

        async fn make_running_zone_with_location(
            &self,
            name: &str,
            log: Logger,
        ) -> (RunningZone, RunningZoneImageLocation) {
            let running_zone = self.make_running_zone(name, log).await;
            let location = RunningZoneImageLocation::InstallDataset {
                // Use a dummy artifact hash for testing purposes.
                hash: ArtifactHash([0; 32]),
            };
            (running_zone, location)
        }
    }

    #[derive(Debug, Default)]
    struct FakeZoneFacilities {
        inner: Mutex<FakeZoneFacilitiesInner>,
    }

    #[derive(Debug, Default)]
    struct FakeZoneFacilitiesInner {
        existing_zones: BTreeSet<String>,
        halt_responses: Option<VecDeque<Result<(), ZoneShutdownError>>>,
        removed_gz_addresses: BTreeSet<AddrObject>,
        zone_image_locations: BTreeMap<ZoneKind, OmicronZoneImageLocation>,
    }

    impl FakeZoneFacilities {
        fn push_existing_zone(&self, name: String) {
            let mut inner = self.inner.lock().unwrap();
            inner.existing_zones.insert(name);
        }

        fn push_halt_response(&self, response: Result<(), ZoneShutdownError>) {
            let mut inner = self.inner.lock().unwrap();
            inner.halt_responses.get_or_insert_default().push_back(response);
        }

        fn insert_zone_image_location(
            &self,
            zone_kind: ZoneKind,
            location: OmicronZoneImageLocation,
        ) {
            let mut inner = self.inner.lock().unwrap();
            inner.zone_image_locations.insert(zone_kind, location);
        }
    }

    impl ZoneFacilities for FakeZoneFacilities {
        fn prepare_omicron_zone<'cfg>(
            &self,
            _log: &Logger,
            zone_config: &'cfg OmicronZoneConfig,
        ) -> PreparedOmicronZone<'cfg> {
            let zone_kind = zone_config.zone_type.kind();
            let inner = self.inner.lock().unwrap();
            let location = inner
                .zone_image_locations
                .get(&zone_kind)
                .unwrap_or_else(|| {
                    panic!(
                        "no fake zone image location found for zone \
                        with kind {zone_kind:?}"
                    )
                });

            PreparedOmicronZone::new(
                zone_config,
                OmicronZoneFileSource {
                    location: location.clone(),
                    file_source: ZoneImageFileSource {
                        file_name: zone_kind
                            .artifact_in_install_dataset()
                            .to_string(),
                        // Return a dummy value for search_paths.
                        search_paths: Vec::new(),
                    },
                },
            )
        }

        async fn zone_with_name_exists(
            &self,
            name: &ZoneName<'_>,
        ) -> Result<bool, CheckZoneExistsError> {
            let inner = self.inner.lock().unwrap();
            Ok(inner.existing_zones.contains(&*name.0))
        }

        async fn halt_zone(
            &self,
            zone: &ZoneName<'_>,
            _log: &Logger,
        ) -> Result<(), ZoneShutdownError> {
            // If a test has called `push_halt_response`, respsect that;
            // otherwise, vacuously succeed.
            let mut inner = self.inner.lock().unwrap();
            match inner.halt_responses.as_mut() {
                Some(resp) => {
                    let resp = resp
                        .pop_front()
                        .expect("have a response for halt_zone()");
                    if resp.is_ok() {
                        inner.existing_zones.remove(&*zone.0);
                    }
                    resp
                }
                None => {
                    inner.existing_zones.remove(&*zone.0);
                    Ok(())
                }
            }
        }

        async fn delete_gz_address(
            &self,
            addrobj: AddrObject,
        ) -> Result<(), ZoneShutdownError> {
            self.inner.lock().unwrap().removed_gz_addresses.insert(addrobj);
            Ok(())
        }
    }

    const BOOT_DISK_PATH: &str = "/test/boot/disk";

    #[derive(Debug)]
    struct FakeSledAgentFacilitiesInner {
        start_responses: VecDeque<anyhow::Result<RunningZone>>,
        removed_ddm_prefixes: BTreeSet<Ipv6Subnet<SLED_PREFIX>>,
        resolver_status: ResolverStatus,
    }

    impl Default for FakeSledAgentFacilitiesInner {
        fn default() -> Self {
            let boot_disk_path = Utf8PathBuf::from(BOOT_DISK_PATH);
            Self {
                start_responses: Default::default(),
                removed_ddm_prefixes: Default::default(),
                // successful status containing no artifacts
                resolver_status: ResolverStatus {
                    zone_manifest: ZoneManifestStatus {
                        boot_disk_path: boot_disk_path.clone(),
                        boot_disk_result: Ok(ZoneManifestArtifactsResult {
                            manifest: OmicronZoneManifest {
                                source: OmicronZoneManifestSource::SledAgent,
                                zones: IdOrdMap::new(),
                            },
                            data: IdOrdMap::new(),
                        }),
                        non_boot_disk_metadata: IdOrdMap::new(),
                    },
                    mupdate_override: MupdateOverrideStatus {
                        boot_disk_path,
                        boot_disk_override: Ok(None),
                        non_boot_disk_overrides: IdOrdMap::new(),
                    },
                    image_directory_override: None,
                },
            }
        }
    }

    #[derive(Debug)]
    struct FakeSledAgentFacilities {
        inner: Mutex<FakeSledAgentFacilitiesInner>,
        underlay_vnic: EtherstubVnic,
    }

    impl Default for FakeSledAgentFacilities {
        fn default() -> Self {
            Self {
                inner: Default::default(),
                underlay_vnic: EtherstubVnic("zoneunittest".into()),
            }
        }
    }

    impl FakeSledAgentFacilities {
        fn push_start_response(
            &self,
            start_response: anyhow::Result<RunningZone>,
        ) {
            let mut inner = self.inner.lock().unwrap();
            inner.start_responses.push_back(start_response);
        }
    }

    impl SledAgentFacilities for FakeSledAgentFacilities {
        fn underlay_vnic(&self) -> &EtherstubVnic {
            &self.underlay_vnic
        }

        fn on_time_sync(&self) {}

        async fn start_omicron_zone(
            &self,
            _prepared_zone: PreparedOmicronZone<'_>,
            _zone_root_path: PathInPool,
        ) -> anyhow::Result<RunningZone> {
            let mut inner = self.inner.lock().unwrap();
            inner
                .start_responses
                .pop_front()
                .expect("test should populate responses for start_omicron_zone")
        }

        fn zone_image_resolver_status(&self) -> ResolverStatus {
            self.inner.lock().unwrap().resolver_status.clone()
        }

        fn clear_mupdate_override(
            &self,
            _override_id: MupdateOverrideUuid,
            _internal_disks: &InternalDisks,
        ) -> ClearMupdateOverrideResult {
            // TODO: In the future, we'll probably want to model this better and
            // not just return no-override.
            ClearMupdateOverrideResult {
                boot_disk_path: Utf8PathBuf::from(BOOT_DISK_PATH),
                boot_disk_result: Ok(
                    ClearMupdateOverrideBootSuccess::NoOverride,
                ),
                non_boot_disk_info: IdOrdMap::new(),
            }
        }

        fn metrics_untrack_zone_links(
            &self,
            _zone: &RunningZone,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn ddm_remove_internal_dns_prefix(
            &self,
            prefix: Ipv6Subnet<SLED_PREFIX>,
        ) {
            self.inner.lock().unwrap().removed_ddm_prefixes.insert(prefix);
        }

        async fn zone_bundle_create(
            &self,
            _zone: &RunningZone,
            _cause: ZoneBundleCause,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    // All our tests operate on fake in-memory disks, so the mount config
    // shouldn't matter. Populate something that won't exist on real systems so
    // if we miss something and try to operate on a real disk it will fail.
    fn nonexistent_mount_config() -> Arc<MountConfig> {
        Arc::new(MountConfig {
            root: "/tmp/test-zones/bogus/root".into(),
            synthetic_disk_root: "/tmp/test-zones/bogus/disk".into(),
        })
    }

    // Helper to build an `OmicronZoneConfig` when the only thing we care about
    // is its zone ID.
    fn make_zone_config(id: OmicronZoneUuid) -> OmicronZoneConfig {
        OmicronZoneConfig {
            id,
            filesystem_pool: Some(ZpoolName::new_external(ZpoolUuid::new_v4())),
            zone_type: OmicronZoneType::Oximeter {
                address: "[::1]:0".parse().unwrap(),
            },
            image_source: OmicronZoneImageSource::InstallDataset,
        }
    }

    #[derive(Default)]
    struct DatasetsBuilder {
        datasets: Vec<(DatasetConfig, Result<(), DatasetEnsureError>)>,
    }

    impl DatasetsBuilder {
        fn push_root(
            &mut self,
            zone: &OmicronZoneConfig,
            result: Result<(), DatasetEnsureError>,
        ) {
            let Some(pool) = zone.filesystem_pool else {
                return;
            };
            self.datasets.push((
                DatasetConfig {
                    id: DatasetUuid::new_v4(),
                    name: DatasetName::new(
                        pool,
                        DatasetKind::TransientZone { name: zone.zone_name() },
                    ),
                    inner: SharedDatasetConfig::default(),
                },
                result,
            ));
        }

        fn push_durable(
            &mut self,
            zone: &OmicronZoneConfig,
            result: Result<(), DatasetEnsureError>,
        ) {
            let Some(dataset) = zone.dataset_name() else {
                return;
            };
            self.datasets.push((
                DatasetConfig {
                    id: DatasetUuid::new_v4(),
                    name: dataset,
                    inner: SharedDatasetConfig::default(),
                },
                result,
            ));
        }

        fn build(self) -> OmicronDatasets {
            OmicronDatasets::with_datasets(self.datasets.into_iter())
        }
    }

    // Helper to build an all-dependencies-met `OmicronDatasets` for the given
    // zone config.
    fn make_datasets<'a>(
        zones: impl Iterator<Item = &'a OmicronZoneConfig>,
    ) -> OmicronDatasets {
        let mut builder = DatasetsBuilder::default();
        for zone in zones {
            builder.push_root(zone, Ok(()));
            builder.push_durable(zone, Ok(()));
        }
        builder.build()
    }

    #[tokio::test]
    async fn shutdown_retries_after_failed_halt() {
        let logctx = dev::test_setup_log("shutdown_retries_after_failed_halt");

        // Construct an initial `OmicronZones` that holds a running zone.
        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);

        let fake_zone_id = OmicronZoneUuid::new_v4();
        let fake_zone_builder = FakeZoneBuilder::new();
        let (fake_zone, location) = fake_zone_builder
            .make_running_zone_with_location("test", logctx.log.clone())
            .await;
        zones.zones.insert(OmicronZone {
            config: make_zone_config(fake_zone_id),
            state: ZoneState::Running {
                running_zone: Arc::new(fake_zone),
                location,
            },
        });

        let sled_agent_facilities = FakeSledAgentFacilities::default();
        let zone_facilities = FakeZoneFacilities::default();
        let desired_zones = IdMap::default();

        // Cause zone halting to fail; this error variant isn't quite right, but
        // that doesn't affect the test. We'll just look for this string in the
        // returned error.
        zone_facilities.push_halt_response(Err(
            ZoneShutdownError::UntrackMetrics(anyhow!("test-boom")),
        ));

        let num_errs = zones
            .shut_down_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                &logctx.log,
            )
            .await
            .expect_err("zone shutdown should fail");
        assert_eq!(usize::from(num_errs), 1);

        let new_zone =
            zones.zones.get(&fake_zone_id).expect("zone ID should be present");

        // We should have recorded that we failed to stop the zone with the
        // error specified above.
        match &new_zone.state {
            ZoneState::PartiallyShutDown {
                state: PartiallyShutDownState::FailedToStop(_),
                err,
            } => {
                let err = InlineErrorChain::new(err).to_string();
                assert!(err.contains("test-boom"), "unexpected error: {err:?}");
            }
            other => panic!("unexpected zone state: {other:?}"),
        }

        // Add this zone to the "should be running zones" list and try shutting
        // down again.
        //
        // Even though this zone should be running, per the config, it's in the
        // `PartiallyShutDown` state, so we should finish shutting it down.
        let desired_zones = [new_zone.config.clone()].into_iter().collect();
        zone_facilities.push_halt_response(Ok(()));
        zones
            .shut_down_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                &logctx.log,
            )
            .await
            .expect("shut down should succeed");

        assert!(
            zones.zones.is_empty(),
            "expected zones to be empty but got {zones:?}"
        );

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn start_zones_that_previously_failed_to_start() {
        let logctx =
            dev::test_setup_log("start_zones_that_previously_failed_to_start");

        // Construct a zone we want to start.
        let fake_zone_id = OmicronZoneUuid::new_v4();
        let desired_zones: IdMap<_> =
            [make_zone_config(fake_zone_id)].into_iter().collect();
        let datasets = make_datasets(desired_zones.iter());

        // Configure our fake sled-agent to fail to start a zone.
        let sled_agent_facilities = FakeSledAgentFacilities::default();
        sled_agent_facilities.push_start_response(Err(anyhow!("test-boom")));
        let zone_facilities = FakeZoneFacilities::default();
        zone_facilities.insert_zone_image_location(
            ZoneKind::Oximeter,
            OmicronZoneImageLocation::InstallDataset {
                hash: Ok(ArtifactHash([0; 32])),
            },
        );

        // Starting with no zones, we should try and fail to start the one zone
        // in `desired_zones`.
        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);
        zones
            .start_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                true, // is_time_synchronized
                &datasets,
                &logctx.log,
            )
            .await;

        assert_eq!(zones.zones.len(), 1);
        let zone_should_be_failed_to_start =
            zones.zones.get(&fake_zone_id).expect("zone is present");
        assert_eq!(
            zone_should_be_failed_to_start.config,
            *desired_zones.get(&fake_zone_id).unwrap()
        );
        match &zone_should_be_failed_to_start.state {
            ZoneState::FailedToStart(err) => {
                let err = InlineErrorChain::new(err).to_string();
                assert!(err.contains("test-boom"), "unexpected error: {err:?}");
            }
            other => panic!("unexpected zone state: {other:?}"),
        }

        // Set up our fake sled-agent to return success.
        let fake_zone_builder = FakeZoneBuilder::new();
        let fake_zone = fake_zone_builder
            .make_running_zone("test", logctx.log.clone())
            .await;
        sled_agent_facilities.push_start_response(Ok(fake_zone));

        // Starting from the "zone failed to start" state, we should try again
        // to start the zone (and succeed this time).
        zones
            .start_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                true,
                &datasets,
                &logctx.log,
            )
            .await;

        assert_eq!(zones.zones.len(), 1);
        let zone_should_be_running =
            zones.zones.get(&fake_zone_id).expect("zone is present");
        assert_eq!(
            zone_should_be_running.config,
            *desired_zones.get(&fake_zone_id).unwrap()
        );
        match &zone_should_be_running.state {
            ZoneState::Running { .. } => (),
            other => panic!("unexpected zone state: {other:?}"),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn start_zone_stops_preexisting_zones() {
        let logctx = dev::test_setup_log("start_zone_stops_preexisting_zones");

        // Construct a zone we want to start.
        let fake_zone = make_zone_config(OmicronZoneUuid::new_v4());
        let desired_zones: IdMap<_> = [fake_zone.clone()].into_iter().collect();
        let datasets = make_datasets(desired_zones.iter());

        // Configure our fake zone facilities to report a zone with this name as
        // already running.
        let sled_agent_facilities = FakeSledAgentFacilities::default();
        let zone_facilities = FakeZoneFacilities::default();
        zone_facilities.push_existing_zone(fake_zone.zone_name());
        zone_facilities.insert_zone_image_location(
            fake_zone.zone_type.kind(),
            OmicronZoneImageLocation::Artifact {
                hash: Ok(ArtifactHash([0; 32])),
            },
        );

        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);

        // Set up our fake sled-agent to return success once the old zone has
        // been halted.
        let fake_zone_builder = FakeZoneBuilder::new();
        sled_agent_facilities.push_start_response(Ok(fake_zone_builder
            .make_running_zone("test", logctx.log.clone())
            .await));

        // Start zones: this should halt the preexisting zone.
        zones
            .start_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                true,
                &datasets,
                &logctx.log,
            )
            .await;

        assert_eq!(
            zone_facilities.inner.lock().unwrap().existing_zones,
            BTreeSet::new()
        );

        assert_eq!(zones.zones.len(), 1);
        let zone_should_be_running =
            zones.zones.get(&fake_zone.id).expect("zone is present");
        assert_eq!(
            zone_should_be_running.config,
            *desired_zones.get(&fake_zone.id).unwrap()
        );
        match &zone_should_be_running.state {
            ZoneState::Running { .. } => (),
            other => panic!("unexpected zone state: {other:?}"),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn start_zone_fails_if_time_not_synced_when_required() {
        let logctx = dev::test_setup_log(
            "start_zone_fails_if_time_not_synced_when_required",
        );

        // Construct a zone we want to start, of a type that requires time to be
        // sync'd.
        let fake_zone = make_zone_config(OmicronZoneUuid::new_v4());
        assert!(fake_zone.zone_type.requires_timesync());
        let desired_zones: IdMap<_> = [fake_zone.clone()].into_iter().collect();
        let datasets = make_datasets(desired_zones.iter());

        let zone_facilities = FakeZoneFacilities::default();
        zone_facilities.insert_zone_image_location(
            ZoneKind::Oximeter,
            OmicronZoneImageLocation::InstallDataset {
                hash: Ok(ArtifactHash([0; 32])),
            },
        );
        let sled_agent_facilities = FakeSledAgentFacilities::default();

        let mut zones = OmicronZones::new(
            nonexistent_mount_config(),
            TimeSyncConfig::Normal,
        );

        // Start zones: this should refuse to start the zone.
        zones
            .start_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                false, // is_time_synchronized
                &datasets,
                &logctx.log,
            )
            .await;

        assert_eq!(zones.zones.len(), 1);
        let zone = zones.zones.get(&fake_zone.id).expect("zone is present");
        assert_eq!(zone.config, *desired_zones.get(&fake_zone.id).unwrap());

        // The zone should now be in the expected error state.
        match &zone.state {
            ZoneState::FailedToStart(err) => {
                assert_matches!(err, ZoneStartError::TimeNotSynchronized);
            }
            other => panic!("unexpected zone state: {other:?}"),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn start_zone_fails_if_halting_preexisting_zone_fails() {
        let logctx = dev::test_setup_log(
            "start_zone_fails_if_halting_preexisting_zone_fails",
        );

        // Construct a zone we want to start.
        let fake_zone = make_zone_config(OmicronZoneUuid::new_v4());
        let desired_zones: IdMap<_> = [fake_zone.clone()].into_iter().collect();
        let datasets = make_datasets(desired_zones.iter());

        // Configure our fake zone facilities to report a zone with this name as
        // already running, and configure halting this zone to fail.
        let zone_facilities = FakeZoneFacilities::default();
        zone_facilities.push_existing_zone(fake_zone.zone_name());
        zone_facilities.push_halt_response(Err(
            ZoneShutdownError::UntrackMetrics(anyhow!("boom")),
        ));
        zone_facilities.insert_zone_image_location(
            ZoneKind::Oximeter,
            OmicronZoneImageLocation::InstallDataset {
                hash: Ok(ArtifactHash([0; 32])),
            },
        );
        let sled_agent_facilities = FakeSledAgentFacilities::default();

        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);

        // Start zones: this should try and fail to halt the preexisting zone.
        zones
            .start_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                true,
                &datasets,
                &logctx.log,
            )
            .await;

        assert_eq!(
            zone_facilities.inner.lock().unwrap().existing_zones,
            [fake_zone.zone_name()].into_iter().collect::<BTreeSet<_>>(),
        );

        assert_eq!(zones.zones.len(), 1);
        let zone = zones.zones.get(&fake_zone.id).expect("zone is present");
        assert_eq!(zone.config, *desired_zones.get(&fake_zone.id).unwrap());

        // The zone should now be in the "partially shut down" state.
        match &zone.state {
            ZoneState::PartiallyShutDown { state, err } => {
                assert_matches!(state, PartiallyShutDownState::FailedToStop(_));
                let err = InlineErrorChain::new(err).to_string();
                assert!(err.contains("boom"), "unexpected error: {err}");
            }
            other => panic!("unexpected zone state: {other:?}"),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn start_zone_fails_if_mupdate_override_error() {
        let logctx =
            dev::test_setup_log("start_zone_fails_if_mupdate_override_error");

        // Construct a zone we want to start.
        let fake_zone = make_zone_config(OmicronZoneUuid::new_v4());
        let desired_zones: IdMap<_> = [fake_zone.clone()].into_iter().collect();
        let datasets = make_datasets(desired_zones.iter());

        // Configure zone facilities to return a mupdate override error.
        let zone_facilities = FakeZoneFacilities::default();
        zone_facilities.insert_zone_image_location(
            ZoneKind::Oximeter,
            OmicronZoneImageLocation::Artifact {
                hash: Err(MupdateOverrideReadError::InstallMetadata(
                    deserialize_error(
                        "fake-dir-path".into(),
                        "fake-json-path".into(),
                        "",
                    ),
                )),
            },
        );
        let sled_agent_facilities = FakeSledAgentFacilities::default();

        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);

        // Start zones: this should fail due to the mupdate override error.
        zones
            .start_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                true, // is_time_synchronized
                &datasets,
                &logctx.log,
            )
            .await;

        assert_eq!(zones.zones.len(), 1);
        let zone = zones.zones.get(&fake_zone.id).expect("zone is present");
        assert_eq!(zone.config, *desired_zones.get(&fake_zone.id).unwrap());

        // The zone should now be in the expected error state.
        match &zone.state {
            ZoneState::FailedToStart(err) => {
                assert_matches!(err, ZoneStartError::MupdateOverride(_));
            }
            other => panic!("unexpected zone state: {other:?}"),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn shutdown_dns_does_dns_specific_cleanup() {
        let logctx =
            dev::test_setup_log("shutdown_dns_does_dns_specific_cleanup");

        // Construct an internal DNS zone and assume it's running.
        let fake_zone_id = OmicronZoneUuid::new_v4();
        let fake_pool = ZpoolName::new_external(ZpoolUuid::new_v4());
        let gz_address = "3fff::1".parse::<Ipv6Addr>().unwrap();
        let gz_address_index = 3;
        let expected_ddm_prefix = Ipv6Subnet::new(gz_address);
        let zone_config = OmicronZoneConfig {
            id: fake_zone_id,
            filesystem_pool: Some(fake_pool),
            zone_type: OmicronZoneType::InternalDns {
                dataset: OmicronZoneDataset { pool_name: fake_pool },
                http_address: "[::1]:0".parse().unwrap(),
                dns_address: "[::1]:0".parse().unwrap(),
                gz_address,
                gz_address_index,
            },
            image_source: OmicronZoneImageSource::InstallDataset,
        };
        let fake_zone_builder = FakeZoneBuilder::new();
        let (fake_zone, fake_zone_location) = fake_zone_builder
            .make_running_zone_with_location("test", logctx.log.clone())
            .await;

        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);
        zones.zones.insert(OmicronZone {
            config: zone_config,
            state: ZoneState::Running {
                running_zone: Arc::new(fake_zone),
                location: fake_zone_location,
            },
        });

        // Shut down the zone. This should succeed and request the removal of
        // the DDM prefix from sled-agent, and should remove the address object
        // from zone-facilities.
        let sled_agent_facilities = FakeSledAgentFacilities::default();
        let zone_facilities = FakeZoneFacilities::default();
        zones
            .shut_down_zones_if_needed_impl(
                &IdMap::default(),
                &sled_agent_facilities,
                &zone_facilities,
                &logctx.log,
            )
            .await
            .expect("zone shutdown should succeed");
        assert_eq!(zones.zones.len(), 0);
        assert_eq!(
            sled_agent_facilities.inner.lock().unwrap().removed_ddm_prefixes,
            [expected_ddm_prefix].into_iter().collect::<BTreeSet<_>>(),
        );
        let expected_addr = AddrObject::new(
            &sled_agent_facilities.underlay_vnic().0,
            &internal_dns_addrobj_name(gz_address_index),
        )
        .unwrap();
        assert_eq!(
            zone_facilities.inner.lock().unwrap().removed_gz_addresses,
            [expected_addr].into_iter().collect::<BTreeSet<_>>(),
        );

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn start_zone_fails_if_missing_root_dataset() {
        let logctx =
            dev::test_setup_log("start_zone_fails_if_missing_root_dataset");

        // Construct a zone we want to start.
        let fake_zone = make_zone_config(OmicronZoneUuid::new_v4());
        let desired_zones: IdMap<_> = [fake_zone.clone()].into_iter().collect();

        // datasets0: missing root dataset entirely
        let datasets0 = {
            let mut builder = DatasetsBuilder::default();
            for zone in &desired_zones {
                builder.push_durable(zone, Ok(()));
            }
            builder.build()
        };

        // datasets1: root exists but failed to ensure
        let datasets1 = {
            let mut builder = DatasetsBuilder::default();
            for zone in &desired_zones {
                builder.push_root(
                    zone,
                    Err(DatasetEnsureError::TestError("boom")),
                );
                builder.push_durable(zone, Ok(()));
            }
            builder.build()
        };

        let zone_facilities = FakeZoneFacilities::default();
        zone_facilities.insert_zone_image_location(
            ZoneKind::Oximeter,
            OmicronZoneImageLocation::InstallDataset {
                hash: Ok(ArtifactHash([0; 32])),
            },
        );
        let sled_agent_facilities = FakeSledAgentFacilities::default();

        // Both dataset variations should fail the same way.
        for datasets in [&datasets0, &datasets1] {
            let mut zones = OmicronZones::new(
                nonexistent_mount_config(),
                TimeSyncConfig::Skip,
            );

            zones
                .start_zones_if_needed_impl(
                    &desired_zones,
                    &sled_agent_facilities,
                    &zone_facilities,
                    true,
                    datasets,
                    &logctx.log,
                )
                .await;

            assert_eq!(zones.zones.len(), 1);
            let zone = zones.zones.get(&fake_zone.id).expect("zone is present");
            assert_eq!(zone.config, *desired_zones.get(&fake_zone.id).unwrap());

            // The zone should now be in the "partially shut down" state.
            match &zone.state {
                ZoneState::FailedToStart(err) => {
                    assert_matches!(
                        err,
                        ZoneStartError::DatasetDependency(
                            ZoneDatasetDependencyError::TransientZoneDatasetNotAvailable(_)
                        )
                    );
                }
                other => panic!("unexpected zone state: {other:?}"),
            }
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn start_zone_fails_if_missing_durable_dataset() {
        let logctx =
            dev::test_setup_log("start_zone_fails_if_missing_durable_dataset");

        // Construct a zone we want to start, using a zone type that has a
        // durable dataset.
        let fake_zone = OmicronZoneConfig {
            id: OmicronZoneUuid::new_v4(),
            filesystem_pool: Some(ZpoolName::new_external(ZpoolUuid::new_v4())),
            zone_type: OmicronZoneType::Crucible {
                address: "[::1]:0".parse().unwrap(),
                dataset: OmicronZoneDataset {
                    pool_name: ZpoolName::new_external(ZpoolUuid::new_v4()),
                },
            },
            image_source: OmicronZoneImageSource::InstallDataset,
        };
        let desired_zones: IdMap<_> = [fake_zone.clone()].into_iter().collect();

        // datasets0: missing durable dataset entirely
        let datasets0 = {
            let mut builder = DatasetsBuilder::default();
            for zone in &desired_zones {
                builder.push_root(zone, Ok(()));
            }
            builder.build()
        };

        // datasets1: durable exists but failed to ensure
        let datasets1 = {
            let mut builder = DatasetsBuilder::default();
            for zone in &desired_zones {
                builder.push_root(zone, Ok(()));
                builder.push_durable(
                    zone,
                    Err(DatasetEnsureError::TestError("boom")),
                );
            }
            builder.build()
        };

        let zone_facilities = FakeZoneFacilities::default();
        zone_facilities.insert_zone_image_location(
            ZoneKind::Crucible,
            OmicronZoneImageLocation::InstallDataset {
                hash: Ok(ArtifactHash([0; 32])),
            },
        );
        let sled_agent_facilities = FakeSledAgentFacilities::default();

        // Both dataset variations should fail the same way.
        for datasets in [&datasets0, &datasets1] {
            let mut zones = OmicronZones::new(
                nonexistent_mount_config(),
                TimeSyncConfig::Skip,
            );

            zones
                .start_zones_if_needed_impl(
                    &desired_zones,
                    &sled_agent_facilities,
                    &zone_facilities,
                    true,
                    datasets,
                    &logctx.log,
                )
                .await;

            assert_eq!(zones.zones.len(), 1);
            let zone = zones.zones.get(&fake_zone.id).expect("zone is present");
            assert_eq!(zone.config, *desired_zones.get(&fake_zone.id).unwrap());

            // The zone should now be in the "partially shut down" state.
            match &zone.state {
                ZoneState::FailedToStart(err) => {
                    assert_matches!(
                        err,
                        ZoneStartError::DatasetDependency(
                            ZoneDatasetDependencyError::DurableDatasetNotAvailable(_)
                        )
                    );
                }
                other => panic!("unexpected zone state: {other:?}"),
            }
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn dont_restart_zone_if_only_change_is_location() {
        let logctx =
            dev::test_setup_log("dont_restart_zone_if_only_change_is_location");

        // Construct two zones we want to start; one runs out of the install
        // dataset, and one runs a specific artifact.
        let install_dataset_zone_hash = ArtifactHash([1; 32]);
        let artifact_zone_hash = ArtifactHash([2; 32]);
        let mut install_dataset_zone =
            make_zone_config(OmicronZoneUuid::new_v4());
        let mut artifact_zone = make_zone_config(OmicronZoneUuid::new_v4());
        artifact_zone.image_source =
            OmicronZoneImageSource::Artifact { hash: artifact_zone_hash };

        // Ensure our two zones are using different zone types (so we can
        // populate our fake insert_zone_image_location with two images).
        install_dataset_zone.zone_type =
            OmicronZoneType::Oximeter { address: "[::1]:0".parse().unwrap() };
        artifact_zone.zone_type = OmicronZoneType::InternalNtp {
            address: "[::1]:0".parse().unwrap(),
        };

        let sled_agent_facilities = FakeSledAgentFacilities::default();

        let desired_zones: IdMap<_> =
            [install_dataset_zone.clone(), artifact_zone.clone()]
                .into_iter()
                .collect();
        let datasets = make_datasets(desired_zones.iter());

        let zone_facilities = FakeZoneFacilities::default();

        let fake_zone_builder = FakeZoneBuilder::new();
        let fake_zone1 = fake_zone_builder
            .make_running_zone("test1", logctx.log.clone())
            .await;
        let fake_zone2 = fake_zone_builder
            .make_running_zone("test2", logctx.log.clone())
            .await;
        sled_agent_facilities.push_start_response(Ok(fake_zone1));
        sled_agent_facilities.push_start_response(Ok(fake_zone2));

        // These match the image sources above.
        zone_facilities.insert_zone_image_location(
            ZoneKind::Oximeter,
            OmicronZoneImageLocation::InstallDataset {
                hash: Ok(install_dataset_zone_hash),
            },
        );
        zone_facilities.insert_zone_image_location(
            ZoneKind::InternalNtp,
            OmicronZoneImageLocation::Artifact { hash: Ok(artifact_zone_hash) },
        );

        // "start" both fake zones.
        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);
        zones
            .start_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                true, // is_time_synchronized
                &datasets,
                &logctx.log,
            )
            .await;

        assert_eq!(zones.zones.len(), 2);
        assert!(
            zones
                .zones
                .iter()
                .all(|z| matches!(z.state, ZoneState::Running { .. }))
        );

        // Change both zone image locations, but only by swapping the image zone
        // sources with ones with matching hashes. Note that we do not consider
        // the image source on the OmicronZoneConfig, just the
        // `PreparedOmicronZone`.
        zone_facilities.insert_zone_image_location(
            ZoneKind::Oximeter,
            OmicronZoneImageLocation::Artifact {
                hash: Ok(install_dataset_zone_hash),
            },
        );
        zone_facilities.insert_zone_image_location(
            ZoneKind::InternalNtp,
            OmicronZoneImageLocation::InstallDataset {
                hash: Ok(artifact_zone_hash),
            },
        );
        let desired_zones: IdMap<_> =
            [install_dataset_zone.clone(), artifact_zone.clone()]
                .into_iter()
                .collect();

        // See if we try to shut down zones; we shouldn't shut down either.
        // Their configs have only changed by the image source, and the image
        // source hash matches in both cases.
        zones
            .shut_down_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                &logctx.log,
            )
            .await
            .expect("zone shutdown should succeed");
        assert_eq!(zones.zones.len(), 2);
        assert!(
            zones
                .zones
                .iter()
                .all(|z| matches!(z.state, ZoneState::Running { .. }))
        );

        // The configs should have been updated to match the new request.
        for z in &desired_zones {
            assert_eq!(*z, zones.zones.get(&z.id).unwrap().config);
        }

        // Change the artifact (internal NTP) zone's hash.
        zone_facilities.insert_zone_image_location(
            ZoneKind::InternalNtp,
            OmicronZoneImageLocation::InstallDataset {
                hash: Ok(install_dataset_zone_hash),
            },
        );

        let desired_zones: IdMap<_> =
            [install_dataset_zone.clone(), artifact_zone.clone()]
                .into_iter()
                .collect();
        zones
            .shut_down_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                &logctx.log,
            )
            .await
            .expect("zone shutdown should succeed");
        assert_eq!(zones.zones.len(), 1);
        assert!(
            zones
                .zones
                .iter()
                .all(|z| matches!(z.state, ZoneState::Running { .. }))
        );
        assert_eq!(
            install_dataset_zone,
            zones.zones.get(&install_dataset_zone.id).unwrap().config,
        );

        // Swap it one more time, but this time also change a different part of
        // the config (`filesystem_pool` here, but any property would do). We
        // _should_ shut it down; even though the hash matches, there are other
        // changes.
        zone_facilities.insert_zone_image_location(
            ZoneKind::Oximeter,
            OmicronZoneImageLocation::Artifact {
                hash: Ok(install_dataset_zone_hash),
            },
        );

        install_dataset_zone.filesystem_pool =
            Some(ZpoolName::new_external(ZpoolUuid::new_v4()));
        let desired_zones: IdMap<_> =
            [install_dataset_zone.clone()].into_iter().collect();
        zones
            .shut_down_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                &logctx.log,
            )
            .await
            .expect("zone shutdown should succeed");
        assert_eq!(zones.zones.len(), 0);

        logctx.cleanup_successful();
    }

    #[test]
    fn does_new_config_require_zone_restart_examples() {
        // Test various cases for does_new_config_require_zone_restart.
        let logctx = dev::test_setup_log(
            "does_new_config_require_zone_restart_examples",
        );

        let fake_zone_id = OmicronZoneUuid::new_v4();
        let base_config = make_zone_config(fake_zone_id);

        let install_dataset_file_source = OmicronZoneFileSource {
            location: OmicronZoneImageLocation::InstallDataset {
                hash: Ok(ArtifactHash([1; 32])),
            },
            file_source: ZoneImageFileSource {
                file_name: "test.tar.gz".to_string(),
                search_paths: Vec::new(),
            },
        };
        let install_dataset_location =
            RunningZoneImageLocation::InstallDataset {
                hash: ArtifactHash([1; 32]),
            };

        // Case 1: Identical configs should not require a restart.
        let identical_config = base_config.clone();
        let prepared_zone = PreparedOmicronZone::new(
            &identical_config,
            install_dataset_file_source.clone(),
        );

        assert!(!does_new_config_require_zone_restart(
            &base_config,
            &install_dataset_location,
            &prepared_zone,
            &logctx.log,
        ));

        // Case 2: Different non-image-source config should require a restart.
        let mut other_config = base_config.clone();
        other_config.filesystem_pool =
            Some(ZpoolName::new_external(ZpoolUuid::new_v4()));
        let prepared_zone_different = PreparedOmicronZone::new(
            &other_config,
            install_dataset_file_source.clone(),
        );

        assert!(does_new_config_require_zone_restart(
            &base_config,
            &install_dataset_location,
            &prepared_zone_different,
            &logctx.log,
        ));

        // Case 3: Same config but different hash should require a restart.
        let prepared_zone_different_hash = PreparedOmicronZone::new(
            &base_config,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::InstallDataset {
                    hash: Ok(ArtifactHash([2; 32])),
                },
                file_source: ZoneImageFileSource {
                    file_name: "test.tar.gz".to_string(),
                    search_paths: Vec::new(),
                },
            },
        );

        assert!(does_new_config_require_zone_restart(
            &base_config,
            &install_dataset_location,
            &prepared_zone_different_hash,
            &logctx.log,
        ));

        // Case 4: Ramdisk -> InstallDataset source (transient error cleared)
        let ramdisk_location = RunningZoneImageLocation::Ramdisk;
        let prepared_zone_install = PreparedOmicronZone::new(
            &base_config,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::InstallDataset {
                    hash: Ok(ArtifactHash([1; 32])),
                },
                file_source: ZoneImageFileSource {
                    file_name: "test.tar.gz".to_string(),
                    search_paths: Vec::new(),
                },
            },
        );

        assert!(does_new_config_require_zone_restart(
            &base_config,
            &ramdisk_location,
            &prepared_zone_install,
            &logctx.log,
        ));

        // Case 5: Ramdisk -> Artifact, restart needed.
        let prepared_zone_artifact = PreparedOmicronZone::new(
            &base_config,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::Artifact {
                    hash: Ok(ArtifactHash([1; 32])),
                },
                file_source: ZoneImageFileSource {
                    file_name: "test.tar.gz".to_string(),
                    search_paths: Vec::new(),
                },
            },
        );

        assert!(does_new_config_require_zone_restart(
            &base_config,
            &ramdisk_location,
            &prepared_zone_artifact,
            &logctx.log,
        ));

        // Case 6: InstallDataset -> Artifact with the same hash, restart
        // needed.
        let artifact_location = RunningZoneImageLocation::InstallDataset {
            hash: ArtifactHash([1; 32]),
        };

        assert!(!does_new_config_require_zone_restart(
            &base_config,
            &artifact_location,
            &prepared_zone,
            &logctx.log,
        ));

        // Case 7: Artifact -> InstallDataset with the same hash, no restart
        // needed.
        let artifact_prepared_zone = PreparedOmicronZone::new(
            &base_config,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::Artifact {
                    hash: Ok(ArtifactHash([1; 32])),
                },
                file_source: ZoneImageFileSource {
                    file_name: "test.tar.gz".to_string(),
                    search_paths: Vec::new(),
                },
            },
        );

        assert!(!does_new_config_require_zone_restart(
            &base_config,
            &install_dataset_location,
            &artifact_prepared_zone,
            &logctx.log,
        ));

        // Case 8: Install dataset hash lookup error does *not* require a
        // restart if we're already running out of the install dataset.
        let prepared_zone_hash_error = PreparedOmicronZone::new(
            &base_config,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::InstallDataset {
                    hash: Err(ZoneImageLocationError::BootDiskMissing),
                },
                file_source: ZoneImageFileSource {
                    file_name: "test.tar.gz".to_string(),
                    search_paths: Vec::new(),
                },
            },
        );

        assert!(!does_new_config_require_zone_restart(
            &base_config,
            &install_dataset_location,
            &prepared_zone_hash_error,
            &logctx.log,
        ));

        // Case 9: Ramdisk to (effectively) Ramdisk, no restart needed.
        let prepared_zone_ramdisk = PreparedOmicronZone::new(
            &base_config,
            OmicronZoneFileSource {
                location: OmicronZoneImageLocation::InstallDataset {
                    hash: Err(ZoneImageLocationError::BootDiskMissing),
                },
                file_source: ZoneImageFileSource {
                    file_name: "test.tar.gz".to_string(),
                    search_paths: Vec::new(),
                },
            },
        );

        assert!(!does_new_config_require_zone_restart(
            &base_config,
            &ramdisk_location,
            &prepared_zone_ramdisk,
            &logctx.log,
        ));

        // Case 10: Artifact -> InstallDataset with different hash, restart
        // needed.
        let artifact_location_different =
            RunningZoneImageLocation::Artifact { hash: ArtifactHash([2; 32]) };

        assert!(does_new_config_require_zone_restart(
            &base_config,
            &artifact_location_different,
            &prepared_zone,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }
}
