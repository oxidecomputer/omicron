// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Traits that allow `sled-agent-config-reconciler` to be a separate crate but
//! still use facilities implemented in `sled-agent` proper.

use illumos_utils::running_zone::RunningZone;
use illumos_utils::zpool::ZpoolName;
use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use sled_agent_types::zone_bundle::ZoneBundleCause;
use sled_storage::config::MountConfig;
use std::future::Future;
use tufaceous_artifact::ArtifactHash;

pub trait SledAgentFacilities: Send + 'static {
    /// Called by the reconciler task to inform sled-agent that time is
    /// sychronized. May be called multiple times.
    // TODO-cleanup should we do this work ourselves instead? This is
    // currently implemented by `ServiceManager` and does a couple one-time
    // setup things (like rewrite the OS boot time). We could probably absorb
    // that work and remove this callback.
    fn on_time_sync(&self) -> impl Future<Output = ()> + Send;

    /// Method to start a zone.
    // TODO-cleanup This is implemented by
    // `ServiceManager::start_omicron_zone()`, which does too much; we should
    // absorb some of its functionality and shrink this interface. We definitely
    // should not need to pass the full list of U2 zpools.
    fn start_omicron_zone(
        &self,
        zone_config: &OmicronZoneConfig,
        mount_config: &MountConfig,
        is_time_synchronized: bool,
        all_u2_pools: &[ZpoolName],
    ) -> impl Future<Output = anyhow::Result<RunningZone>> + Send;

    /// Stop tracking metrics for a zone's datalinks.
    fn metrics_untrack_zone_links(
        &self,
        zone: &RunningZone,
    ) -> anyhow::Result<()>;

    /// Instruct DDM to start advertising a prefix.
    fn ddm_add_internal_dns_prefix(&self, prefix: Ipv6Subnet<SLED_PREFIX>);

    /// Instruct DDM to stop advertising a prefix.
    fn ddm_remove_internal_dns_prefix(&self, prefix: Ipv6Subnet<SLED_PREFIX>);

    /// Create a zone bundle.
    fn zone_bundle_create(
        &self,
        zone: &RunningZone,
        cause: ZoneBundleCause,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub trait SledAgentArtifactStore: Send + Sync + 'static {
    /// Check an artifact exists in the TUF Repo Depot storage.
    fn validate_artifact_exists_in_storage(
        &self,
        artifact: ArtifactHash,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}
