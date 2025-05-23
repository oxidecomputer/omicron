// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for Omicron zones.
//!
//! There is no separate tokio task here; our parent reconciler task owns this
//! set of zones and is able to mutate it in place during reconciliation.

use crate::SledAgentFacilities;
use crate::TimeSyncConfig;
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
use illumos_utils::zpool::ZpoolName;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use omicron_common::address::Ipv6Subnet;
use omicron_uuid_kinds::OmicronZoneUuid;
use sled_agent_types::time_sync::TimeSync;
use sled_agent_types::zone_bundle::ZoneBundleCause;
use sled_storage::config::MountConfig;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::num::NonZeroUsize;
use std::str::FromStr as _;
use std::sync::Arc;

use super::CurrentlyManagedZpools;

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
            ZoneState::Running(_) => false,
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
                ZoneState::Running(_) => {
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
        sled_agent_facilities: &T,
        log: &Logger,
    ) -> Result<(), NonZeroUsize> {
        self.shut_down_zones_if_needed_impl(
            desired_zones,
            sled_agent_facilities,
            &RealZoneFacilities,
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
        let zones_to_shut_down = self.zones.iter().filter(|z| {
            match desired_zones.get(&z.config.id) {
                // We no longer want this zone to be running.
                None => true,

                // We do want this zone to be running; check the current
                // state.
                Some(desired_config) => match &z.state {
                    // Only shut down a running zone if the desired config
                    // has changed from the config used to start it.
                    ZoneState::Running(_) => {
                        if z.config == *desired_config {
                            false
                        } else {
                            info!(
                                log,
                                "starting shutdown of running zone; config \
                                 has changed";
                                "zone" => z.config.zone_name(),
                                "old-config" => ?z.config,
                                "new-config" => ?desired_config,
                            );
                            true
                        }
                    }

                    // Shut down zones in other states, but log why first.
                    ZoneState::PartiallyShutDown { err, .. } => {
                        info!(
                            log,
                            "resuming shutdown of partially-shut-down zone";
                            "zone" => z.config.zone_name(),
                            "prev_err" => InlineErrorChain::new(err),
                        );
                        true
                    }

                    ZoneState::FailedToStart(err) => {
                        info!(
                            log,
                            "starting shutdown of a failed-to-start zone";
                            "zone" => z.config.zone_name(),
                            "prev_err" => InlineErrorChain::new(err),
                        );
                        true
                    }
                },
            }
        });

        // Map the zones to the futures that will try to shut them down.
        let shutdown_futures = zones_to_shut_down.map(|zone| {
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
    pub(super) async fn start_zones_if_needed<T: SledAgentFacilities>(
        &mut self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        sled_agent_facilities: &T,
        is_time_synchronized: bool,
        all_u2_pools: &CurrentlyManagedZpools,
        log: &Logger,
    ) {
        self.start_zones_if_needed_impl(
            desired_zones,
            sled_agent_facilities,
            &RealZoneFacilities,
            is_time_synchronized,
            all_u2_pools,
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
        all_u2_pools: &CurrentlyManagedZpools,
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
                Some(ZoneState::Running(_)) => false,
            }
        });

        // Build up the futures for starting each zone.
        let all_u2_pools = all_u2_pools.clone().into_vec();
        let start_futures = zones_to_start.map(|zone| {
            self.start_single_zone(
                zone,
                sled_agent_facilities,
                zone_facilities,
                is_time_synchronized,
                &all_u2_pools,
            )
            .map(move |result| (zone.clone(), result))
        });

        // Concurrently start all zones, then record the results.
        let start_results = future::join_all(start_futures).await;
        for (config, result) in start_results {
            let state = match result {
                Ok(running_zone) => ZoneState::Running(Arc::new(running_zone)),
                Err(err) => ZoneState::FailedToStart(err),
            };
            self.zones.insert(OmicronZone { config, state });
        }
    }

    async fn start_single_zone<T: SledAgentFacilities, U: ZoneFacilities>(
        &self,
        zone: &OmicronZoneConfig,
        sled_agent_facilities: &T,
        zone_facilities: &U,
        is_time_synchronized: bool,
        all_u2_pools: &[ZpoolName],
    ) -> Result<RunningZone, ZoneStartError> {
        // Ensure no zone by this name exists. This should only happen in the
        // event of a sled-agent restart, in which case all the zones the
        // previous sled-agent process had started are still running.
        self.ensure_removed_before_starting(zone, zone_facilities).await?;

        sled_agent_facilities
            .start_omicron_zone(
                zone,
                &self.mount_config,
                is_time_synchronized,
                all_u2_pools,
            )
            .await
            .map_err(ZoneStartError::SledAgentStartFailed)
    }

    async fn ensure_removed_before_starting<U: ZoneFacilities>(
        &self,
        zone: &OmicronZoneConfig,
        zone_facilities: &U,
    ) -> Result<(), ZoneStartError> {
        let zone_name = ZoneName::new(zone);

        // If no zone by this name exists, there's nothing to remove.
        if !zone_facilities.zone_with_name_exists(&zone_name).await? {
            return Ok(());
        }

        Ok(())
    }

    /// Check the timesync status from a running NTP zone (if it exists)
    pub(super) async fn check_timesync(&self) -> TimeSyncStatus {
        match &self.timesync_config {
            TimeSyncConfig::Normal => {
                match self.timesync_status_from_ntp_zone().await {
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
    ) -> Result<TimeSync, TimeSyncError> {
        // Get the one and only running NTP zone, or return an error.
        let mut running_ntp_zones = self.zones.iter().filter_map(|z| {
            if !z.config.zone_type.is_ntp() {
                return None;
            }

            match &z.state {
                ZoneState::Running(running_zone) => Some(running_zone),
                ZoneState::PartiallyShutDown { .. }
                | ZoneState::FailedToStart(_) => None,
            }
        });
        let running_ntp_zone =
            running_ntp_zones.next().ok_or(TimeSyncError::NoRunningNtpZone)?;
        if running_ntp_zones.next().is_some() {
            return Err(TimeSyncError::MultipleRunningNtpZones);
        }

        // XXXNTP - This could be replaced with a direct connection to the
        // daemon using a patched version of the chrony_candm crate to allow
        // a custom server socket path. From the GZ, it should be possible to
        // connect to the UNIX socket at
        // format!("{}/var/run/chrony/chronyd.sock", ntp_zone.root())

        let stdout = running_ntp_zone
            .run_cmd(&["/usr/bin/chronyc", "-c", "tracking"])
            .map_err(TimeSyncError::ExecuteChronyc)?;

        let v: Vec<&str> = stdout.split(',').collect();

        if v.len() < 10 {
            return Err(TimeSyncError::FailedToParse {
                reason: "too few fields",
                stdout,
            });
        }

        let Ok(ref_id) = u32::from_str_radix(v[0], 16) else {
            return Err(TimeSyncError::FailedToParse {
                reason: "bad ref_id",
                stdout,
            });
        };
        let ip_addr =
            IpAddr::from_str(v[1]).unwrap_or(Ipv6Addr::UNSPECIFIED.into());
        let Ok(stratum) = u8::from_str(v[2]) else {
            return Err(TimeSyncError::FailedToParse {
                reason: "bad stratum",
                stdout,
            });
        };
        let Ok(ref_time) = f64::from_str(v[3]) else {
            return Err(TimeSyncError::FailedToParse {
                reason: "bad ref_time",
                stdout,
            });
        };
        let Ok(correction) = f64::from_str(v[4]) else {
            return Err(TimeSyncError::FailedToParse {
                reason: "bad correction",
                stdout,
            });
        };

        // Per `chronyc waitsync`'s implementation, if either the
        // reference IP address is not unspecified or the reference
        // ID is not 0 or 0x7f7f0101, we are synchronized to a peer.
        let peer_sync =
            !ip_addr.is_unspecified() || (ref_id != 0 && ref_id != 0x7f7f0101);

        let sync = stratum < 10
            && ref_time > 1234567890.0
            && peer_sync
            && correction.abs() <= 0.05;

        Ok(TimeSync { sync, ref_id, ip_addr, stratum, ref_time, correction })
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
            ZoneState::Running(running_zone) => {
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
                self.resume_shutdown_from_stop(
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
                self.resume_shutdown_from_cleanup(
                    sled_agent_facilities,
                    zone_facilities,
                    &log,
                )
                .await
            }
            ZoneState::FailedToStart(_) => {
                // TODO-correctness What do we need to do to try to shut down a
                // zone that we tried to start? We need fine-grained status of
                // what startup things succeeded that need to be cleaned up. For
                // now, warn that we're assuming we have no work to do.
                warn!(
                    log,
                    "need to shut down zone that failed to start, but this \
                     is currently unimplemented: assuming no cleanup work \
                     required"
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

        self.resume_shutdown_from_stop(
            sled_agent_facilities,
            zone_facilities,
            &ZoneName::from(running_zone.name()),
            log,
        )
        .await
    }

    async fn resume_shutdown_from_stop<
        T: SledAgentFacilities,
        U: ZoneFacilities,
    >(
        &self,
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

        self.resume_shutdown_from_cleanup(
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
        &self,
        sled_agent_facilities: &T,
        zone_facilities: &U,
        log: &Logger,
    ) -> Result<(), (PartiallyShutDownState, ZoneShutdownError)> {
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
    Running(Arc<RunningZone>),

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

#[derive(Debug, Clone)]
struct ZoneName<'a>(Cow<'a, str>);

impl<'a> From<&'a str> for ZoneName<'a> {
    fn from(value: &'a str) -> Self {
        Self(Cow::Borrowed(value))
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

struct RealZoneFacilities;

impl ZoneFacilities for RealZoneFacilities {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CurrentlyManagedZpoolsReceiver;
    use anyhow::anyhow;
    use camino_tempfile::Utf8TempDir;
    use illumos_utils::dladm::Etherstub;
    use illumos_utils::dladm::EtherstubVnic;
    use illumos_utils::link::VnicAllocator;
    use illumos_utils::running_zone::ZoneBuilderFactory;
    use illumos_utils::zpool::PathInPool;
    use illumos_utils::zpool::ZpoolName;
    use illumos_utils::zpool::ZpoolOrRamdisk;
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
    use omicron_common::address::SLED_PREFIX;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::BTreeSet;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    // Helper to construct a `RunningZone` even on non-illumos systems.
    struct FakeZoneBuilder {
        vnic_alloc: VnicAllocator<Etherstub>,
        factory: ZoneBuilderFactory,
        tempdir: Utf8TempDir,
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
            Self { vnic_alloc, factory, tempdir }
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
    }

    #[derive(Debug, Default)]
    struct FakeZoneFacilities {
        inner: Mutex<FakeZoneFacilitiesInner>,
    }

    #[derive(Debug, Default)]
    struct FakeZoneFacilitiesInner {
        halt_responses: Option<VecDeque<Result<(), ZoneShutdownError>>>,
        removed_gz_addresses: BTreeSet<AddrObject>,
    }

    impl FakeZoneFacilities {
        fn push_halt_response(&self, response: Result<(), ZoneShutdownError>) {
            let mut inner = self.inner.lock().unwrap();
            inner.halt_responses.get_or_insert_default().push_back(response);
        }
    }

    impl ZoneFacilities for FakeZoneFacilities {
        async fn zone_with_name_exists(
            &self,
            _name: &ZoneName<'_>,
        ) -> Result<bool, CheckZoneExistsError> {
            Ok(false)
        }

        async fn halt_zone(
            &self,
            _zone: &ZoneName<'_>,
            _log: &Logger,
        ) -> Result<(), ZoneShutdownError> {
            // If a test has called `push_halt_response`, respsect that;
            // otherwise, vacuously succeed.
            let mut inner = self.inner.lock().unwrap();
            match inner.halt_responses.as_mut() {
                Some(resp) => {
                    resp.pop_front().expect("have a response for halt_zone()")
                }
                None => Ok(()),
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

    #[derive(Debug)]
    struct FakeSledAgentFacilities {
        inner: Mutex<FakeSledAgentFacilitiesInner>,
        underlay_vnic: EtherstubVnic,
    }

    #[derive(Debug, Default)]
    struct FakeSledAgentFacilitiesInner {
        start_responses: VecDeque<anyhow::Result<RunningZone>>,
        removed_ddm_prefixes: BTreeSet<Ipv6Subnet<SLED_PREFIX>>,
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
        fn push_start_response(&self, response: anyhow::Result<RunningZone>) {
            let mut inner = self.inner.lock().unwrap();
            inner.start_responses.push_back(response);
        }
    }

    impl SledAgentFacilities for FakeSledAgentFacilities {
        fn underlay_vnic(&self) -> &EtherstubVnic {
            &self.underlay_vnic
        }

        async fn on_time_sync(&self) {}

        async fn start_omicron_zone(
            &self,
            _zone_config: &OmicronZoneConfig,
            _mount_config: &MountConfig,
            _is_time_synchronized: bool,
            _all_u2_pools: &[ZpoolName],
        ) -> anyhow::Result<RunningZone> {
            let mut inner = self.inner.lock().unwrap();
            inner
                .start_responses
                .pop_front()
                .expect("test should populate responses for start_omicron_zone")
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

    #[tokio::test]
    async fn shutdown_retries_after_failed_halt() {
        let logctx = dev::test_setup_log("shutdown_retries_after_failed_halt");

        // Construct an initial `OmicronZones` that holds a running zone.
        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);

        let fake_zone_id = OmicronZoneUuid::new_v4();
        let fake_zone_builder = FakeZoneBuilder::new();
        let fake_zone = fake_zone_builder
            .make_running_zone("test", logctx.log.clone())
            .await;
        zones.zones.insert(OmicronZone {
            config: make_zone_config(fake_zone_id),
            state: ZoneState::Running(Arc::new(fake_zone)),
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
        let currently_managed_zpools =
            CurrentlyManagedZpoolsReceiver::fake_static(
                desired_zones.iter().map(|z| z.filesystem_pool.unwrap()),
            )
            .current();

        // Configure our fake sled-agent to fail to start a zone.
        let sled_agent_facilities = FakeSledAgentFacilities::default();
        sled_agent_facilities.push_start_response(Err(anyhow!("test-boom")));
        let zone_facilities = FakeZoneFacilities::default();

        // Starting with no zones, we should try and fail to start the one zone
        // in `desired_zones`.
        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);
        zones
            .start_zones_if_needed_impl(
                &desired_zones,
                &sled_agent_facilities,
                &zone_facilities,
                true,
                &currently_managed_zpools,
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
                &currently_managed_zpools,
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
            ZoneState::Running(_) => (),
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
        let fake_zone = fake_zone_builder
            .make_running_zone("test", logctx.log.clone())
            .await;

        let mut zones =
            OmicronZones::new(nonexistent_mount_config(), TimeSyncConfig::Skip);
        zones.zones.insert(OmicronZone {
            config: zone_config,
            state: ZoneState::Running(Arc::new(fake_zone)),
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
}
