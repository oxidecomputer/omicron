// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use futures::FutureExt;
use futures::future;
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
use omicron_common::address::SLED_PREFIX;
use omicron_uuid_kinds::OmicronZoneUuid;
use sled_agent_types::zone_bundle::ZoneBundleCause;
use sled_storage::config::MountConfig;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

use crate::ddm_reconciler::DdmReconciler;
use crate::metrics::MetricsRequestQueue;
use crate::params::OmicronZoneConfigExt;
use crate::services::ServiceManager;
use crate::services::internal_dns_addrobj_name;
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;

#[derive(Debug, thiserror::Error)]
pub enum ZoneShutdownError {
    #[error("failed to halt and remove zone")]
    HaltAndRemove(#[source] AdmError),
    #[error("failed to delete global zone address object")]
    DeleteGzAddrObj(#[source] DeleteAddressError),

    #[cfg(test)]
    #[error("fake error for tests: {0}")]
    FakeErrorForTests(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ZoneStartError {
    // We need to break this error up into better cases.
    #[error("failed to start zone")]
    FixThisError(#[source] crate::services::Error),

    #[cfg(test)]
    #[error("fake error for tests: {0}")]
    FakeErrorForTests(String),
}

#[derive(Debug, Clone, Default)]
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
        let deps = RealShutdownDependencies {
            metrics_queue,
            zone_bundler,
            ddm_reconciler,
            underlay_vnic,
        };
        self.shut_down_zones_if_needed_impl(desired_zones, &deps, log).await
    }

    /// Attempt to start any zones that are present in `desired_zones` but not
    /// in `self`.
    ///
    /// If any changes are made, returns a new instance of `Self`.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // TODO remove once we trim this down
    pub(super) async fn start_zones_if_needed(
        &self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        service_manager: &ServiceManager,
        mount_config: &MountConfig,
        zone: &OmicronZoneConfig,
        time_is_synchronized: bool,
        all_u2_pools: &Vec<ZpoolName>,
        log: &Logger,
    ) -> Option<Self> {
        let deps = RealStartDependencies {
            service_manager,
            mount_config,
            zone,
            time_is_synchronized,
            all_u2_pools,
        };
        self.start_zones_if_needed_impl(desired_zones, &deps, log).await
    }

    async fn shut_down_zones_if_needed_impl<T: ShutdownDependencies>(
        &self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        deps: &T,
        log: &Logger,
    ) -> Option<Self> {
        let mut shutdown_futures = Vec::new();

        for current_zone in self.zones.iter() {
            // Skip zones that don't need to be shut down or cleaned up.
            if let Some(desired) = desired_zones.get(&current_zone.config.id) {
                match &current_zone.state {
                    ZoneState::Running { .. } => {
                        // The zone is running and its config hasn't changed; we
                        // should not touch it.
                        if current_zone.config == *desired {
                            continue;
                        } else {
                            info!(
                                log,
                                "starting shutdown of running zone";
                                "zone" => current_zone.config.zone_name(),
                            );
                        }
                    }
                    ZoneState::FailedToStart(err) => {
                        info!(
                            log,
                            "starting shutdown of a failed-to-start zone";
                            "zone" => current_zone.config.zone_name(),
                            "prev_err" => InlineErrorChain::new(err),
                        );
                    }
                    // If we have a partially shutdown zone, fall through below
                    // and we'll try to resume shutdown (so we can then restart
                    // it with the desired config).
                    ZoneState::PartiallyShutDown { err, .. } => {
                        info!(
                            log,
                            "resuming shutdown of partially-shut-down zone";
                            "zone" => current_zone.config.zone_name(),
                            "prev_err" => InlineErrorChain::new(err),
                        );
                    }
                }
            }

            shutdown_futures.push(
                current_zone
                    .try_shut_down(deps, log)
                    .map(|result| (current_zone.config.id, result)),
            );
        }

        if shutdown_futures.is_empty() {
            // If we have no shutdown work to do, we're done and made no
            // changes.
            None
        } else {
            // Otherwise, clone ourself and update all zones that needed
            // shutdown work.
            let shutdown_results = future::join_all(shutdown_futures).await;
            let mut new_self = self.clone();

            for (zone_id, result) in shutdown_results {
                match result {
                    Ok(()) => {
                        new_self.zones.remove(&zone_id);
                    }
                    Err((state, err)) => {
                        new_self
                            .zones
                            .get_mut(&zone_id)
                            .expect("shutdown task operates on existing zone")
                            .state =
                            ZoneState::PartiallyShutDown { state, err };
                    }
                }
            }

            Some(new_self)
        }
    }

    async fn start_zones_if_needed_impl<T: StartDependencies>(
        &self,
        desired_zones: &IdMap<OmicronZoneConfig>,
        deps: &T,
        log: &Logger,
    ) -> Option<Self> {
        let zones_to_start = desired_zones
            .iter()
            .filter(|zone| {
                // Start any zones in desired_zones that we have no record of at
                // all...
                if !self.zones.contains_key(&zone.id) {
                    info!(
                        log,
                        "starting zone";
                        "config" => ?zone,
                    );
                    true
                } else {
                    false
                }
            })
            .chain(self.zones.iter().filter_map(|zone| {
                match &zone.state {
                    ZoneState::PartiallyShutDown { .. }
                    | ZoneState::Running(_) => None,
                    // ... and also retry starting any zone we failed to start
                    // previously.
                    ZoneState::FailedToStart(err) => {
                        info!(
                            log,
                            "retrying start of zone";
                            "config" => ?zone.config,
                            "prev_err" => InlineErrorChain::new(err),
                        );
                        Some(&zone.config)
                    }
                }
            }));

        let mut start_futures = Vec::new();
        for zone in zones_to_start {
            start_futures
                .push(deps.start_zone(zone).map(move |result| (zone, result)));
        }

        if start_futures.is_empty() {
            // If we have no work to do, we're done and made no changes.
            None
        } else {
            // Otherwise, clone ourself and insert records for all the zones we
            // tried to start.
            let start_results = future::join_all(start_futures).await;
            let mut new_self = self.clone();
            for (config, result) in start_results {
                let state = match result {
                    Ok(running_zone) => {
                        ZoneState::Running(Arc::new(running_zone))
                    }
                    Err(err) => ZoneState::FailedToStart(Arc::new(err)),
                };
                new_self
                    .zones
                    .insert(OmicronZone { config: config.clone(), state });
            }
            Some(new_self)
        }
    }
}

#[derive(Debug, Clone)]
enum ZoneState {
    PartiallyShutDown {
        state: PartiallyShutDownState,
        err: Arc<ZoneShutdownError>,
    },
    Running(Arc<RunningZone>),
    FailedToStart(Arc<ZoneStartError>),
}

#[derive(Debug, Clone)]
enum PartiallyShutDownState {
    FailedToStop(Arc<RunningZone>),
    FailedToDeleteGzAddress,
}

// A running zone and the configuration which started it.
#[derive(Debug, Clone)]
struct OmicronZone {
    config: OmicronZoneConfig,
    state: ZoneState,
}

impl OmicronZone {
    async fn try_shut_down<T: ShutdownDependencies>(
        &self,
        deps: &T,
        log: &Logger,
    ) -> Result<(), (PartiallyShutDownState, Arc<ZoneShutdownError>)> {
        let log = log.new(o!("zone" => self.config.zone_name()));

        match &self.state {
            ZoneState::Running(running_zone) => {
                info!(log, "shutting down running zone");

                // We only try once to create a zone bundle; if this fails we
                // move on to the rest of the shutdown process.
                if let Err(err) = deps
                    .create_zone_bundle(
                        running_zone,
                        ZoneBundleCause::UnexpectedZone,
                    )
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
                deps.untrack_metrics(running_zone).await;

                self.resume_shutdown_from_stop(deps, running_zone, &log).await
            }
            ZoneState::PartiallyShutDown {
                state: PartiallyShutDownState::FailedToStop(running_zone),
                ..
            } => self.resume_shutdown_from_stop(deps, running_zone, &log).await,
            ZoneState::PartiallyShutDown {
                state: PartiallyShutDownState::FailedToDeleteGzAddress,
                ..
            } => self.resume_shutdown_from_cleanup(deps, &log).await,
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

    async fn resume_shutdown_from_stop<T: ShutdownDependencies>(
        &self,
        deps: &T,
        running_zone: &Arc<RunningZone>,
        log: &Logger,
    ) -> Result<(), (PartiallyShutDownState, Arc<ZoneShutdownError>)> {
        if let Err(err) = deps.halt_zone(running_zone, log).await {
            warn!(
                log,
                "Failed to stop running zone";
                InlineErrorChain::new(&err),
            );
            return Err((
                PartiallyShutDownState::FailedToStop(Arc::clone(running_zone)),
                Arc::new(err),
            ));
        }

        self.resume_shutdown_from_cleanup(deps, log).await
    }

    async fn resume_shutdown_from_cleanup<T: ShutdownDependencies>(
        &self,
        deps: &T,
        log: &Logger,
    ) -> Result<(), (PartiallyShutDownState, Arc<ZoneShutdownError>)> {
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
                deps.underlay_vnic_interface(),
                &internal_dns_addrobj_name(*gz_address_index),
            )
            .expect("internal DNS address object name is well-formed");
            if let Err(err) = deps.delete_gz_address(addrobj).await {
                warn!(
                    log,
                    "Failed to delete internal-dns gz address";
                    InlineErrorChain::new(&err),
                );
                return Err((
                    PartiallyShutDownState::FailedToDeleteGzAddress,
                    Arc::new(err),
                ));
            }

            deps.remove_internal_dns_subnet_advertisement(Ipv6Subnet::new(
                *gz_address,
            ));
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

trait ShutdownDependencies {
    async fn create_zone_bundle(
        &self,
        zone: &RunningZone,
        cause: ZoneBundleCause,
    ) -> Result<(), BundleError>;

    async fn untrack_metrics(&self, zone: &RunningZone);

    async fn halt_zone(
        &self,
        zone: &RunningZone,
        log: &Logger,
    ) -> Result<(), ZoneShutdownError>;

    fn underlay_vnic_interface(&self) -> &str;

    async fn delete_gz_address(
        &self,
        addrobj: AddrObject,
    ) -> Result<(), ZoneShutdownError>;

    fn remove_internal_dns_subnet_advertisement(
        &self,
        subnet: Ipv6Subnet<SLED_PREFIX>,
    );
}

struct RealShutdownDependencies<'a> {
    metrics_queue: &'a MetricsRequestQueue,
    zone_bundler: &'a ZoneBundler,
    ddm_reconciler: &'a DdmReconciler,
    underlay_vnic: &'a EtherstubVnic,
}

impl ShutdownDependencies for RealShutdownDependencies<'_> {
    async fn create_zone_bundle(
        &self,
        zone: &RunningZone,
        cause: ZoneBundleCause,
    ) -> Result<(), BundleError> {
        self.zone_bundler.create(zone, cause).await?;
        Ok(())
    }

    async fn untrack_metrics(&self, zone: &RunningZone) {
        self.metrics_queue.untrack_zone_links(zone).await;
    }

    async fn halt_zone(
        &self,
        zone: &RunningZone,
        log: &Logger,
    ) -> Result<(), ZoneShutdownError> {
        // We don't use `zone.stop()` here because it doesn't allow repeated
        // attempts after a failure:
        // https://github.com/oxidecomputer/omicron/issues/7881. Instead, use
        // the lower-level `Zones::halt_and_remove_logged()` function directly.
        // This may leave our `RunningZone` is a bogus state where it still
        // holds a `zoneid_t` that doesn't exist anymore, but if we're in the
        // shutdown path we never use that `zoneid_t`.
        Zones::halt_and_remove_logged(log, zone.name())
            .await
            .map_err(ZoneShutdownError::HaltAndRemove)
    }

    fn underlay_vnic_interface(&self) -> &str {
        &self.underlay_vnic.0
    }

    async fn delete_gz_address(
        &self,
        addrobj: AddrObject,
    ) -> Result<(), ZoneShutdownError> {
        tokio::task::spawn_blocking(move || {
            Zones::delete_address(None, &addrobj)
        })
        .await
        .expect("closure did not panic")
        .map_err(ZoneShutdownError::DeleteGzAddrObj)
    }

    fn remove_internal_dns_subnet_advertisement(
        &self,
        subnet: Ipv6Subnet<SLED_PREFIX>,
    ) {
        self.ddm_reconciler.remove_internal_dns_subnet(subnet);
    }
}

trait StartDependencies {
    async fn start_zone(
        &self,
        zone: &OmicronZoneConfig,
    ) -> Result<RunningZone, ZoneStartError>;
}

struct RealStartDependencies<'a> {
    service_manager: &'a ServiceManager,
    mount_config: &'a MountConfig,
    zone: &'a OmicronZoneConfig,
    time_is_synchronized: bool,
    all_u2_pools: &'a Vec<ZpoolName>,
}

impl StartDependencies for RealStartDependencies<'_> {
    async fn start_zone(
        &self,
        zone: &OmicronZoneConfig,
    ) -> Result<RunningZone, ZoneStartError> {
        // TODO-cleanup `start_omicron_zone` probably does too much:
        //
        // 1. Check if a zone with the same name is still running; if so, shut
        //    it down and do cleanup.
        // 2. If the zone has a durable dataset, check that it exists and that
        //    its properties are as expected.
        // 3. Check that the `filesystem_pool` for the zone matches an existing
        //    managed disk.
        // 4. Call `initialize_zone`.
        //
        // I think we (or the reconciler) should handle 1-3 before calling this
        // method, and here we should only call `initialize_zone`. That's a
        // bigger change that can happen after the reconciler lands, though.
        match self
            .service_manager
            .start_omicron_zone(
                self.mount_config,
                zone,
                self.time_is_synchronized,
                self.all_u2_pools,
                None,
            )
            .await
        {
            Ok(zone) => Ok(zone.into_runtime()),
            Err(err) => Err(ZoneStartError::FixThisError(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use illumos_utils::dladm::Etherstub;
    use illumos_utils::dladm::MockDladm;
    use illumos_utils::link::VnicAllocator;
    use illumos_utils::running_zone::ZoneBuilderFactory;
    use illumos_utils::zpool::PathInPool;
    use illumos_utils::zpool::ZpoolName;
    use illumos_utils::zpool::ZpoolOrRamdisk;
    use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;

    struct FakeZoneBuilder {
        vnic_alloc: VnicAllocator<Etherstub>,
        factory: ZoneBuilderFactory,
    }

    impl FakeZoneBuilder {
        fn new() -> Self {
            let vnic_source = Etherstub("teststubvnic".to_string());
            let vnic_alloc = VnicAllocator::new("testvnic", vnic_source);
            let factory = ZoneBuilderFactory::fake(None);
            Self { vnic_alloc, factory }
        }

        async fn make_running_zone(
            &self,
            name: &str,
            log: Logger,
        ) -> RunningZone {
            let create_vnic_ctx = MockDladm::create_vnic_context();
            create_vnic_ctx
                .expect()
                .return_once(|_: &Etherstub, _, _, _, _| Ok(()));
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
            RunningZone::fake_boot(0, installed_fake_zone).await
        }
    }

    #[derive(Debug, Default)]
    struct FakeShutdownDepsInner {
        halt_responses: Option<VecDeque<Result<(), ZoneShutdownError>>>,
    }

    #[derive(Debug, Default)]
    struct FakeShutdownDeps {
        inner: Mutex<FakeShutdownDepsInner>,
    }

    impl FakeShutdownDeps {
        fn push_halt_response(&self, response: Result<(), ZoneShutdownError>) {
            let mut inner = self.inner.lock().unwrap();
            inner.halt_responses.get_or_insert_default().push_back(response);
        }
    }

    impl ShutdownDependencies for FakeShutdownDeps {
        async fn create_zone_bundle(
            &self,
            _zone: &RunningZone,
            _cause: ZoneBundleCause,
        ) -> Result<(), BundleError> {
            Ok(())
        }

        async fn untrack_metrics(&self, _zone: &RunningZone) {}

        async fn halt_zone(
            &self,
            _zone: &RunningZone,
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

        fn underlay_vnic_interface(&self) -> &str {
            "testunderlayvnic"
        }

        async fn delete_gz_address(
            &self,
            _addrobj: AddrObject,
        ) -> Result<(), ZoneShutdownError> {
            Ok(())
        }

        fn remove_internal_dns_subnet_advertisement(
            &self,
            _subnet: Ipv6Subnet<SLED_PREFIX>,
        ) {
        }
    }

    #[derive(Debug, Default)]
    struct FakeStartDepsInner {
        start_responses: VecDeque<Result<RunningZone, ZoneStartError>>,
    }

    #[derive(Debug, Default)]
    struct FakeStartDeps {
        inner: Mutex<FakeStartDepsInner>,
    }

    impl FakeStartDeps {
        fn push_start_response(
            &self,
            response: Result<RunningZone, ZoneStartError>,
        ) {
            let mut inner = self.inner.lock().unwrap();
            inner.start_responses.push_back(response);
        }
    }

    impl StartDependencies for FakeStartDeps {
        async fn start_zone(
            &self,
            _zone: &OmicronZoneConfig,
        ) -> Result<RunningZone, ZoneStartError> {
            let mut inner = self.inner.lock().unwrap();
            inner
                .start_responses
                .pop_front()
                .expect("have a response for start_zone()")
        }
    }

    #[tokio::test]
    async fn test_shutdown_retries_after_failed_halt() {
        let logctx = dev::test_setup_log("test_first_config_is_ledgered");

        let fake_zone_id = OmicronZoneUuid::new_v4();
        let fake_zone = FakeZoneBuilder::new()
            .make_running_zone("test", logctx.log.clone())
            .await;
        let zones0 = ZoneMap {
            zones: [OmicronZone {
                config: OmicronZoneConfig {
                    id: fake_zone_id,
                    filesystem_pool: Some(ZpoolName::new_external(
                        ZpoolUuid::new_v4(),
                    )),
                    zone_type: OmicronZoneType::Oximeter {
                        address: "[::1]:0".parse().unwrap(),
                    },
                    image_source: OmicronZoneImageSource::InstallDataset,
                },
                state: ZoneState::Running(Arc::new(fake_zone)),
            }]
            .into_iter()
            .collect(),
        };

        let fake_deps = FakeShutdownDeps::default();
        let desired_zones = IdMap::default();

        // Cause zone halting to fail
        fake_deps.push_halt_response(Err(
            ZoneShutdownError::FakeErrorForTests("boom".into()),
        ));

        let zones1 = zones0
            .shut_down_zones_if_needed_impl(
                &desired_zones,
                &fake_deps,
                &logctx.log,
            )
            .await
            .expect("tried to shut down zone");

        let new_zone = zones1
            .zones
            .get(&fake_zone_id)
            .expect("zone ID should be in new map");

        // We should have recorded that we failed to stop the zone with the
        // error specified above.
        match &new_zone.state {
            ZoneState::PartiallyShutDown {
                state: PartiallyShutDownState::FailedToStop(_),
                err,
            } if matches!(**err, ZoneShutdownError::FakeErrorForTests(_)) => (),
            other => panic!("unexpected zone state: {other:?}"),
        }

        // Try again, but this time claim the zone should be running: we should
        // _still_ try to resume shutting it down because it's in a
        // partially-shut-down state. (The real reconciler would subsequently
        // restart it.)
        let desired_zones = [new_zone.config.clone()].into_iter().collect();
        fake_deps.push_halt_response(Ok(()));
        let zones2 = zones1
            .shut_down_zones_if_needed_impl(
                &desired_zones,
                &fake_deps,
                &logctx.log,
            )
            .await
            .expect("tried to shut down zone");

        assert!(
            zones2.zones.is_empty(),
            "expected zones2 to be empty but got {zones2:?}"
        );

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_start_zones_that_previously_failed_to_start() {
        let logctx = dev::test_setup_log(
            "test_start_zones_that_previously_failed_to_start",
        );

        let fake_zone_id = OmicronZoneUuid::new_v4();
        let desired_zones = [OmicronZoneConfig {
            id: fake_zone_id,
            filesystem_pool: Some(ZpoolName::new_external(ZpoolUuid::new_v4())),
            zone_type: OmicronZoneType::Oximeter {
                address: "[::1]:0".parse().unwrap(),
            },
            image_source: OmicronZoneImageSource::InstallDataset,
        }]
        .into_iter()
        .collect();

        // Set up our fake deps to return an error when starting a zone.
        let fake_deps = FakeStartDeps::default();
        fake_deps.push_start_response(Err(ZoneStartError::FakeErrorForTests(
            "fail".to_string(),
        )));

        // Starting with no zones, we should try and fail to start the one zone
        // in `desired_zones`.
        let zones0 = ZoneMap::default();
        let zones1 = zones0
            .start_zones_if_needed_impl(&desired_zones, &fake_deps, &logctx.log)
            .await
            .expect("got new zones map");

        assert_eq!(zones1.zones.len(), 1);
        let zones1_zone =
            zones1.zones.get(&fake_zone_id).expect("zone is present");
        assert_eq!(
            zones1_zone.config,
            *desired_zones.get(&fake_zone_id).unwrap()
        );
        match &zones1_zone.state {
            ZoneState::FailedToStart(err)
                if matches!(**err, ZoneStartError::FakeErrorForTests(_)) =>
            {
                ()
            }
            other => panic!("unexpected zone state: {other:?}"),
        }

        // Set up our fake deps to return success.
        let fake_zone = FakeZoneBuilder::new()
            .make_running_zone("test", logctx.log.clone())
            .await;
        fake_deps.push_start_response(Ok(fake_zone));

        // Starting from the "zone failed to start" state, we should try again
        // to start the zone (and succeed this time).
        let zones2 = zones1
            .start_zones_if_needed_impl(&desired_zones, &fake_deps, &logctx.log)
            .await
            .expect("got new zones map");

        assert_eq!(zones2.zones.len(), 1);
        let zones2_zone =
            zones2.zones.get(&fake_zone_id).expect("zone is present");
        assert_eq!(
            zones2_zone.config,
            *desired_zones.get(&fake_zone_id).unwrap()
        );
        match &zones2_zone.state {
            ZoneState::Running(_) => (),
            other => panic!("unexpected zone state: {other:?}"),
        }

        logctx.cleanup_successful();
    }

    // TODO-john
    // more tests:
    // * other removal error paths
}
