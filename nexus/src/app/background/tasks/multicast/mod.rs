// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reconciling multicast group state with dendrite switch
//! configuration.
//!
//! # Reliable Persistent Workflow (RPW)
//!
//! This module implements the RPW pattern for multicast groups, providing
//! eventual consistency between the database state and the physical network
//! switches (Dendrite). Unlike sagas which handle immediate transactional
//! operations, RPW handles ongoing background reconciliation.
//!
//! ## Why RPW for Multicast?
//!
//! Multicast operations require systematic convergence across multiple
//! distributed components:
//! - Database state (groups, members, routing configuration)
//! - Dataplane state (Match-action tables via Dendrite/DPD)
//! - Instance lifecycle (start/stop/migrate affecting group membership)
//! - Network topology (sled-to-switch mappings, port configurations)
//!
//! ## Architecture: RPW +/- Sagas
//!
//! **Sagas handle immediate operations:**
//! - User API requests (create/delete groups)
//! - Instance lifecycle events (start/stop)
//! - Database state transitions
//! - Initial validation and resource allocation
//!
//! **RPW handles background reconciliation:**
//! - Dataplane state convergence
//! - Group and Member state checks and transitions ("Joining" → "Joined" → "Left")
//! - Drift detection and correction
//! - Cleanup of orphaned resources
//!
//! ## Multicast Group Architecture
//!
//! ### External vs Underlay Groups
//!
//! The multicast implementation uses a bifurcated design with paired groups:
//!
//! **External Groups** (customer-facing):
//! - IPv4/IPv6 addresses allocated from customer IP pools
//! - Exposed via operator APIs and network interfaces
//! - Subject to VPC routing and firewall policies
//!
//! **Underlay Groups** (admin-scoped IPv6):
//! - IPv6 multicast scope values per RFC 7346; admin-local is ff04::/16
//!   <https://www.rfc-editor.org/rfc/rfc7346>
//! - Used for internal rack forwarding to guests
//! - Mapped 1:1 with external groups via deterministic mapping
//!
//! ### Forwarding Architecture
//!
//! Traffic flow: `External Network ←NAT→ External Group ←Bridge→ Underlay Group ←Switch(es)→ Instance`
//!
//! 1. **External traffic** arrives at external multicast address
//! 2. **NAT translation** via 1:1 mapping between external → underlay group
//! 3. **Dataplane forwarding** configured via DPD
//! 4. **Instance delivery** via underlay multicast to target sleds
//!
//! ## Reconciliation Components
//!
//! The reconciler handles:
//! - **Group lifecycle**: "Creating" → "Active" → "Deleting" → "Deleted"
//! - **Member lifecycle**: "Joining" → "Joined" → "Left" (3-state model) -> (timestamp deleted)
//! - **Dataplane updates**: DPD API calls for P4 table updates
//! - **Topology mapping**: Sled-to-switch-port resolution with caching

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use serde_json::json;
use slog::{error, info, trace};
use tokio::sync::RwLock;

use nexus_db_model::MulticastGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::MulticastGroupReconcilerStatus;
use omicron_uuid_kinds::SledUuid;

use crate::app::background::BackgroundTask;
use crate::app::multicast::dataplane::MulticastDataplaneClient;
use crate::app::saga::StartSaga;

pub mod groups;
pub mod members;

/// Type alias for the sled mapping cache.
type SledMappingCache =
    Arc<RwLock<(SystemTime, HashMap<SledUuid, Vec<MulticastSwitchPort>>)>>;

/// Admin-scoped IPv6 multicast prefix (ff04::/16) as u16 for address
/// construction.
const IPV6_ADMIN_SCOPED_MULTICAST_PREFIX: u16 = 0xff04;

/// Result of processing a state transition for multicast entities.
#[derive(Debug)]
pub(crate) enum StateTransition {
    /// No state change needed.
    NoChange,
    /// State changed successfully.
    StateChanged,
    /// Entity needs cleanup/removal.
    NeedsCleanup,
}

/// Switch port configuration for multicast group members.
#[derive(Clone, Debug)]
pub(crate) struct MulticastSwitchPort {
    /// Switch port ID
    pub port_id: dpd_client::types::PortId,
    /// Switch link ID
    pub link_id: dpd_client::types::LinkId,
    /// Direction for multicast traffic (External or Underlay)
    pub direction: dpd_client::types::Direction,
}

/// Background task that reconciles multicast group state with dendrite
/// configuration using the Saga + RPW hybrid pattern.
pub(crate) struct MulticastGroupReconciler {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    sagas: Arc<dyn StartSaga>,
    /// Cache for sled-to-switch-port mappings.
    /// Maps (`cache_id`, `sled_id`) → switch port for multicast traffic.
    sled_mapping_cache: SledMappingCache,
    cache_ttl: Duration,
    /// Maximum number of members to process concurrently per group.
    member_concurrency_limit: usize,
    /// Maximum number of groups to process concurrently.
    group_concurrency_limit: usize,
}

impl MulticastGroupReconciler {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        sagas: Arc<dyn StartSaga>,
    ) -> Self {
        Self {
            datastore,
            resolver,
            sagas,
            sled_mapping_cache: Arc::new(RwLock::new((
                SystemTime::now(),
                HashMap::new(),
            ))),
            cache_ttl: Duration::from_secs(3600), // 1 hour - refresh topology mappings regularly
            member_concurrency_limit: 100,
            group_concurrency_limit: 100,
        }
    }

    /// Generate appropriate tag for multicast groups.
    ///
    /// Both external and underlay groups use the same meaningful tag based on
    /// group name. This creates logical pairing for management and cleanup
    /// operations.
    pub(crate) fn generate_multicast_tag(group: &MulticastGroup) -> String {
        group.name().to_string()
    }
}

impl BackgroundTask for MulticastGroupReconciler {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            trace!(opctx.log, "multicast group reconciler activating");
            let status = self.run_reconciliation_pass(opctx).await;

            let did_work = status.groups_created
                + status.groups_deleted
                + status.groups_verified
                + status.members_processed
                + status.members_deleted
                > 0;

            if status.errors.is_empty() {
                if did_work {
                    info!(
                        opctx.log,
                        "multicast RPW reconciliation pass completed successfully";
                        "external_groups_created" => status.groups_created,
                        "external_groups_deleted" => status.groups_deleted,
                        "active_groups_verified" => status.groups_verified,
                        "member_state_transitions" => status.members_processed,
                        "orphaned_members_cleaned" => status.members_deleted,
                        "dataplane_operations" => status.groups_created + status.groups_deleted + status.members_processed
                    );
                } else {
                    trace!(
                        opctx.log,
                        "multicast RPW reconciliation pass completed - dataplane in sync"
                    );
                }
            } else {
                error!(
                    opctx.log,
                    "multicast RPW reconciliation pass completed with dataplane inconsistencies";
                    "external_groups_created" => status.groups_created,
                    "external_groups_deleted" => status.groups_deleted,
                    "active_groups_verified" => status.groups_verified,
                    "member_state_transitions" => status.members_processed,
                    "orphaned_members_cleaned" => status.members_deleted,
                    "dataplane_error_count" => status.errors.len()
                );
            }

            json!(status)
        }
        .boxed()
    }
}

impl MulticastGroupReconciler {
    /// Execute a full reconciliation pass.
    async fn run_reconciliation_pass(
        &mut self,
        opctx: &OpContext,
    ) -> MulticastGroupReconcilerStatus {
        let mut status = MulticastGroupReconcilerStatus::default();

        trace!(opctx.log, "starting multicast reconciliation pass");

        // Create dataplane client (across switches) once for the entire
        // reconciliation pass (in case anything has changed)
        let dataplane_client = match MulticastDataplaneClient::new(
            self.datastore.clone(),
            self.resolver.clone(),
            opctx.log.clone(),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                let msg = format!(
                    "failed to create multicast dataplane client: {e:#}"
                );
                status.errors.push(msg);
                return status;
            }
        };

        // Process creating groups
        match self.reconcile_creating_groups(opctx).await {
            Ok(count) => status.groups_created += count,
            Err(e) => {
                let msg = format!("failed to reconcile creating groups: {e:#}");
                status.errors.push(msg);
            }
        }

        // Process deleting groups
        match self.reconcile_deleting_groups(opctx, &dataplane_client).await {
            Ok(count) => status.groups_deleted += count,
            Err(e) => {
                let msg = format!("failed to reconcile deleting groups: {e:#}");
                status.errors.push(msg);
            }
        }

        // Reconcile active groups (verify state, update dataplane as needed)
        match self.reconcile_active_groups(opctx, &dataplane_client).await {
            Ok(count) => status.groups_verified += count,
            Err(e) => {
                let msg = format!("failed to reconcile active groups: {e:#}");
                status.errors.push(msg);
            }
        }

        // Process member state changes
        match self.reconcile_member_states(opctx, &dataplane_client).await {
            Ok(count) => status.members_processed += count,
            Err(e) => {
                let msg = format!("failed to reconcile member states: {e:#}");
                status.errors.push(msg);
            }
        }

        // Clean up deleted members ("Left" + `time_deleted`)
        match self.cleanup_deleted_members(opctx).await {
            Ok(count) => status.members_deleted += count,
            Err(e) => {
                let msg = format!("failed to cleanup deleted members: {e:#}");
                status.errors.push(msg);
            }
        }

        trace!(
            opctx.log,
            "multicast RPW reconciliation cycle completed";
            "external_groups_created" => status.groups_created,
            "external_groups_deleted" => status.groups_deleted,
            "active_groups_verified" => status.groups_verified,
            "member_lifecycle_transitions" => status.members_processed,
            "orphaned_member_cleanup" => status.members_deleted,
            "total_dpd_operations" => status.groups_created + status.groups_deleted + status.members_processed,
            "dataplane_consistency_check" => if status.errors.is_empty() { "PASS" } else { "FAIL" }
        );

        status
    }
}

/// Generate admin-scoped IPv6 multicast address from an external multicast
/// address. Uses the IPv6 admin-local scope (ff04::/16) per RFC 7346:
/// <https://www.rfc-editor.org/rfc/rfc7346>.
pub(crate) fn map_external_to_underlay_ip(
    external_ip: IpAddr,
) -> Result<IpAddr, anyhow::Error> {
    match external_ip {
        IpAddr::V4(ipv4) => {
            // Map IPv4 multicast to admin-scoped IPv6 multicast (ff04::/16)
            // Use the IPv4 octets in the lower 32 bits
            let octets = ipv4.octets();
            let underlay_ipv6 = Ipv6Addr::new(
                IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                0x0000,
                0x0000,
                0x0000,
                0x0000,
                0x0000,
                u16::from(octets[0]) << 8 | u16::from(octets[1]),
                u16::from(octets[2]) << 8 | u16::from(octets[3]),
            );
            Ok(IpAddr::V6(underlay_ipv6))
        }
        IpAddr::V6(ipv6) => {
            // For IPv6 input, ensure it's in admin-scoped range
            if ipv6.segments()[0] & 0xff00 == 0xff00 {
                // Already a multicast address - convert to admin-scoped
                let segments = ipv6.segments();
                let underlay_ipv6 = Ipv6Addr::new(
                    0xff04,
                    segments[1],
                    segments[2],
                    segments[3],
                    segments[4],
                    segments[5],
                    segments[6],
                    segments[7],
                );
                Ok(IpAddr::V6(underlay_ipv6))
            } else {
                Err(anyhow::Error::msg(format!(
                    "IPv6 address is not multicast: {ipv6}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_map_ipv4_to_underlay_ipv6() {
        // Test IPv4 multicast mapping to admin-scoped IPv6
        let ipv4 = Ipv4Addr::new(224, 1, 2, 3);
        let result = map_external_to_underlay_ip(IpAddr::V4(ipv4)).unwrap();

        match result {
            IpAddr::V6(ipv6) => {
                // Should be ff04::e001:203 (224=0xe0, 1=0x01, 2=0x02, 3=0x03)
                assert_eq!(
                    ipv6.segments(),
                    [
                        0xff04, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0xe001,
                        0x0203
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv4_edge_cases() {
        // Test minimum IPv4 multicast address
        let ipv4_min = Ipv4Addr::new(224, 0, 0, 1);
        let result = map_external_to_underlay_ip(IpAddr::V4(ipv4_min)).unwrap();
        match result {
            IpAddr::V6(ipv6) => {
                assert_eq!(
                    ipv6.segments(),
                    [
                        0xff04, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0xe000,
                        0x0001
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }

        // Test maximum IPv4 multicast address
        let ipv4_max = Ipv4Addr::new(239, 255, 255, 255);
        let result = map_external_to_underlay_ip(IpAddr::V4(ipv4_max)).unwrap();
        match result {
            IpAddr::V6(ipv6) => {
                assert_eq!(
                    ipv6.segments(),
                    [
                        0xff04, 0x0000, 0x0000, 0x0000, 0x0000, 0x0000, 0xefff,
                        0xffff
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv6_multicast_to_admin_scoped() {
        // Test site-local multicast (ff05::/16) to admin-scoped (ff04::/16)
        let ipv6_site_local = Ipv6Addr::new(
            0xff05, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678, 0x9abc,
        );
        let result =
            map_external_to_underlay_ip(IpAddr::V6(ipv6_site_local)).unwrap();

        match result {
            IpAddr::V6(ipv6) => {
                // Should preserve everything except first segment, which becomes ff04
                assert_eq!(
                    ipv6.segments(),
                    [
                        0xff04, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678,
                        0x9abc
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv6_global_multicast_to_admin_scoped() {
        // Test global multicast (ff0e::/16) to admin-scoped (ff04::/16)
        let ipv6_global = Ipv6Addr::new(
            0xff0e, 0xabcd, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678,
        );
        let result =
            map_external_to_underlay_ip(IpAddr::V6(ipv6_global)).unwrap();

        match result {
            IpAddr::V6(ipv6) => {
                assert_eq!(
                    ipv6.segments(),
                    [
                        0xff04, 0xabcd, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234,
                        0x5678
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv6_already_admin_scoped() {
        // Test admin-scoped multicast (ff04::/16) - should preserve as-is
        let ipv6_admin = Ipv6Addr::new(
            0xff04, 0x1111, 0x2222, 0x3333, 0x4444, 0x5555, 0x6666, 0x7777,
        );
        let result =
            map_external_to_underlay_ip(IpAddr::V6(ipv6_admin)).unwrap();

        match result {
            IpAddr::V6(ipv6) => {
                assert_eq!(
                    ipv6.segments(),
                    [
                        0xff04, 0x1111, 0x2222, 0x3333, 0x4444, 0x5555, 0x6666,
                        0x7777
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv6_non_multicast_fails() {
        // Test unicast IPv6 address - should fail
        let ipv6_unicast = Ipv6Addr::new(
            0x2001, 0xdb8, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678,
        );
        let result = map_external_to_underlay_ip(IpAddr::V6(ipv6_unicast));

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not multicast"));
    }

    #[test]
    fn test_map_ipv6_link_local_unicast_fails() {
        // Test link-local unicast - should fail
        let ipv6_link_local = Ipv6Addr::new(
            0xfe80, 0x0000, 0x0000, 0x0000, 0x1234, 0x5678, 0x9abc, 0xdef0,
        );
        let result = map_external_to_underlay_ip(IpAddr::V6(ipv6_link_local));

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not multicast"));
    }
}
