// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP Pools, collections of external IP addresses for guest instances

use ipnetwork::IpNetwork;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::IpPool;
use nexus_db_model::IpPoolAssignment;
use nexus_db_model::IpPoolType;
use nexus_db_model::IpPoolUpdate;
use nexus_db_model::IpVersion;
use nexus_db_queries::authz;
use nexus_db_queries::authz::ApiResource;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::model::Name;
use nexus_types::external_api::ip_pool;
use nexus_types::identity::Resource;
use omicron_common::address::{
    IPV4_LINK_LOCAL_MULTICAST_SUBNET, IPV4_SSM_RESERVED_SUBNET,
    IPV4_SSM_SUBNET, IPV6_SSM_SUBNETS, Ipv4Range, Ipv6Range,
    UNDERLAY_MULTICAST_SUBNET, UNDERLAY_MULTICAST_SUBNET_LAST,
    ipv6_ssm_allocatable_range,
};
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use ref_cast::RefCast;
use std::matches;
use std::num::NonZeroU32;
use uuid::Uuid;

/// Validate multicast-specific constraints for IP ranges.
///
/// Rejects reserved or special-use addresses:
///
/// - IPv4: link-local (224.0.0.0/24), ASM/SSM boundary spanning, and the
///   reserved SSM /24 (232.0.0.0/24, RFC 4607)
/// - IPv6: scope nibbles outside the usable set (admin-local, site-local,
///   organization-local, global) across all flag variants,
///   underlay (ff04::/64), ASM/SSM boundary spanning, and SSM group IDs
///   outside the dynamically allocatable range (RFC 4607)
///
/// The underlay prefix (ff04::/64) is reserved for internal external→underlay
/// mapping via `UNDERLAY_MULTICAST_SUBNET`. This prefix is static for
/// consistency across racks. External pools may use other admin-local
/// prefixes (e.g., `ff04:0:0:1::/64`) or other scopes (e.g., `ff05::/16`).
///
/// Validates early so operators get immediate feedback rather than errors
/// when allocating addresses later.
fn validate_multicast_range(range: &ip_pool::IpRange) -> Result<(), Error> {
    // ASM/SSM classification and the RFC 4607 reserved and invalid
    // sub-range checks match Dendrite DPD management (see
    // dendrite/dpd/src/mcast/validate.rs). Nexus validates ranges at pool
    // admission and Dendrite validates individual group addresses.
    match range {
        ip_pool::IpRange::V4(v4_range) => {
            let first = v4_range.first_address();
            let last = v4_range.last_address();

            // Reject IPv4 ranges that intersect reserved subnets
            {
                // link-local (224.0.0.0/24)
                let reserved = Ipv4Range {
                    first: IPV4_LINK_LOCAL_MULTICAST_SUBNET.addr(),
                    last: IPV4_LINK_LOCAL_MULTICAST_SUBNET
                        .broadcast()
                        .expect("valid IPv4 subnet"),
                };
                if v4_range.overlaps(&reserved) {
                    return Err(Error::invalid_request(
                        "Cannot add IPv4 link-local multicast range \
                         (224.0.0.0/24) to IP pool",
                    ));
                }
            }

            // Validate range doesn't span the ASM/SSM boundary. Endpoint
            // classification alone misses a range that strictly contains
            // 232/8 with both endpoints in ASM space, so an ASM-classified
            // range must also not overlap the SSM subnet.
            let first_is_ssm = IPV4_SSM_SUBNET.contains(first);
            let last_is_ssm = IPV4_SSM_SUBNET.contains(last);
            let ssm = Ipv4Range {
                first: IPV4_SSM_SUBNET.addr(),
                last: IPV4_SSM_SUBNET.broadcast().expect("valid IPv4 subnet"),
            };

            if first_is_ssm != last_is_ssm
                || (!first_is_ssm && v4_range.overlaps(&ssm))
            {
                return Err(Error::invalid_request(
                    "IP range cannot span ASM and SSM address spaces",
                ));
            }

            // For SSM ranges, reject overlap with the reserved first /24
            // (232.0.0.0/24, RFC 4607 section 4.3)
            if first_is_ssm {
                let reserved = Ipv4Range {
                    first: IPV4_SSM_RESERVED_SUBNET.addr(),
                    last: IPV4_SSM_RESERVED_SUBNET
                        .broadcast()
                        .expect("valid IPv4 subnet"),
                };
                if v4_range.overlaps(&reserved) {
                    return Err(Error::invalid_request(
                        "Cannot add reserved IPv4 SSM range \
                         (232.0.0.0/24) to IP pool",
                    ));
                }
            }
        }
        ip_pool::IpRange::V6(v6_range) => {
            let first = v6_range.first_address();
            let last = v6_range.last_address();

            // Reject IPv6 ranges that intersect reserved subnets
            {
                // Admit only /16 blocks whose scope nibble is usable for
                // inter-sled delivery, independent of the flags nibble:
                // admin-local (4, incl. the non-underlay ff04::/16 space),
                // site-local (5), organization-local (8), and global (e).
                // [RFC 7346 §2] reserves 0 and f, scopes 1 and 2 never
                // leave a host or link, and 6, 7, and 9 through d are
                // unassigned ([RFC 4291 §2.7]).
                //
                // Realm-local (3) is defined per network technology
                // ([RFC 7346 §3] covers only IEEE 802.15.4), so it is
                // excluded absent an Ethernet realm definition. Callers
                // guarantee the range is multicast, so the first segment
                // falls in 0xff00..=0xffff.
                //
                // [RFC 4291 §2.7]: https://www.rfc-editor.org/rfc/rfc4291#section-2.7
                // [RFC 7346 §2]: https://www.rfc-editor.org/rfc/rfc7346#section-2
                // [RFC 7346 §3]: https://www.rfc-editor.org/rfc/rfc7346#section-3
                for seg0 in first.segments()[0]..=last.segments()[0] {
                    let scope_name = match seg0 & 0x000f {
                        0x0 | 0xf => "reserved",
                        0x1 => "interface-local",
                        0x2 => "link-local",
                        0x3 => "realm-local",
                        0x4 | 0x5 | 0x8 | 0xe => continue,
                        _ => "unassigned",
                    };
                    return Err(Error::invalid_request(&format!(
                        "Cannot add IPv6 multicast range overlapping \
                         {scope_name} scope ({seg0:x}::/16) to IP pool"
                    )));
                }

                // underlay multicast (ff04::/64, reserved for internal use)
                let underlay = Ipv6Range {
                    first: UNDERLAY_MULTICAST_SUBNET.addr(),
                    last: UNDERLAY_MULTICAST_SUBNET_LAST,
                };
                if v6_range.overlaps(&underlay) {
                    return Err(Error::invalid_request(
                        "Cannot add IPv6 underlay multicast range \
                         (ff04::/64) to IP pool - reserved for internal use",
                    ));
                }
            }

            // IPv6 SSM consists of sixteen disjoint ff3x::/32 blocks. A range
            // is entirely SSM only when one block contains both endpoints. A
            // range that intersects a block without being contained by it
            // mixes ASM and SSM addresses, even when both endpoints happen to
            // be in different SSM blocks.
            let ssm_block = IPV6_SSM_SUBNETS
                .iter()
                .find(|subnet| subnet.contains(first) && subnet.contains(last));

            match ssm_block {
                Some(subnet) => {
                    // Within a block only the low 32 bits form the group
                    // ID. RFC 4607 section 1 invalidates IDs below
                    // 0x40000000 and section 4.3 reserves 0x40000000
                    // through 0x7fffffff for IANA allocation. Require the
                    // range to sit within the dynamically allocatable
                    // remainder.
                    let allocatable = ipv6_ssm_allocatable_range(subnet);
                    if !allocatable.contains(first)
                        || !allocatable.contains(last)
                    {
                        return Err(Error::invalid_request(
                            "IPv6 SSM range must fall within the dynamically \
                             allocatable group IDs (ff3x::8000:0 through \
                             ff3x::ffff:ffff per RFC 4607)",
                        ));
                    }
                }
                None => {
                    let overlaps_ssm = IPV6_SSM_SUBNETS.iter().any(|subnet| {
                        v6_range.overlaps(&Ipv6Range {
                            first: subnet.addr(),
                            last: subnet.last_addr(),
                        })
                    });
                    if overlaps_ssm {
                        return Err(Error::invalid_request(
                            "IP range cannot span ASM and SSM address spaces",
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}

impl super::Nexus {
    pub fn ip_pool_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool: &'a NameOrId,
    ) -> LookupResult<lookup::IpPool<'a>> {
        match pool {
            NameOrId::Name(name) => {
                let pool = LookupPath::new(opctx, &self.db_datastore)
                    .ip_pool_name(Name::ref_cast(name));
                Ok(pool)
            }
            NameOrId::Id(id) => {
                let pool =
                    LookupPath::new(opctx, &self.db_datastore).ip_pool_id(*id);
                Ok(pool)
            }
        }
    }

    pub(crate) async fn ip_pool_create(
        &self,
        opctx: &OpContext,
        pool_params: &ip_pool::IpPoolCreate,
    ) -> CreateResult<IpPool> {
        let ip_version = pool_params.ip_version.into();

        let assignment = pool_params.assignment.into();
        let pool = match pool_params.pool_type {
            ip_pool::IpPoolType::Unicast => {
                IpPool::new(&pool_params.identity, ip_version, assignment)
            }
            ip_pool::IpPoolType::Multicast => IpPool::new_multicast(
                &pool_params.identity,
                ip_version,
                assignment,
            ),
        };

        self.db_datastore.ip_pool_create(opctx, pool).await
    }

    /// List IP pools visible to the current silo, with optional filtering.
    pub(crate) async fn current_silo_ip_pool_list(
        &self,
        opctx: &OpContext,
        filter: &ip_pool::IpPoolFilter,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(db::model::IpPool, db::model::IpPoolResource)> {
        let authz_silo =
            opctx.authn.silo_required().internal_context("listing IP pools")?;

        // From the developer user's point of view, we treat IP pools linked to
        // their silo as silo resources, so they can list them if they can list
        // silo children
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        self.db_datastore
            .silo_ip_pool_list(
                opctx,
                &authz_silo,
                filter.ip_version.map(Into::into),
                filter.pool_type.map(Into::into),
                pagparams,
            )
            .await
    }

    /// List all operator-visible IP pools, with optional filtering.
    pub(crate) async fn ip_pools_list_operator(
        &self,
        opctx: &OpContext,
        filter: &ip_pool::SystemIpPoolFilter,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::IpPool> {
        self.db_datastore
            .ip_pools_list_paginated(
                opctx,
                filter.assignment.map(Into::into),
                filter.ip_version.map(Into::into),
                filter.pool_type.map(Into::into),
                pagparams,
            )
            .await
    }

    /// Look up linked pool by name or ID. 404 on pools that exist but aren't
    /// linked to the current silo. Special logic to make sure non-fleet users
    /// can read the pool.
    pub async fn silo_ip_pool_fetch<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool: &'a NameOrId,
    ) -> LookupResult<(
        authz::IpPool,
        db::model::IpPool,
        db::model::IpPoolResource,
    )> {
        let (authz_pool, pool) = self
            .ip_pool_lookup(opctx, pool)?
            // TODO-robustness: https://github.com/oxidecomputer/omicron/issues/3995
            // Checking CreateChild works because it is the permission for
            // allocating IPs from a pool, which any authenticated user has.
            // But what we really want to say is that any authenticated user
            // has actual Read permission on any IP pool linked to their silo.
            // Instead we are backing into this with the next line: never fail
            // this auth check as long as you're authed, then 404 if unlinked.
            // This is not a correctness issue per se because the logic as-is is
            // correct. The main problem is that it is fiddly to get right and
            // has to be done manually each time.
            .fetch_for(authz::Action::CreateChild)
            .await?;

        // 404 if no link is found in the current silo
        let link = self.db_datastore.ip_pool_fetch_link(opctx, pool.id()).await;
        match link {
            Ok(link) => Ok((authz_pool, pool, link)),
            Err(_) => Err(authz_pool.not_found()),
        }
    }

    /// List silos for a given pool
    pub(crate) async fn ip_pool_silo_list(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::IpPoolResource> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::ListChildren).await?;

        // check ability to list silos in general
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        self.db_datastore.ip_pool_silo_list(opctx, &authz_pool, pagparams).await
    }

    // List pools for a given silo
    pub(crate) async fn silo_ip_pool_list(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(db::model::IpPool, db::model::IpPoolResource)> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        // check ability to list pools in general
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        self.db_datastore
            .silo_ip_pool_list(opctx, &authz_silo, None, None, pagparams)
            .await
    }

    pub(crate) async fn ip_pool_link_silo(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_link: &ip_pool::IpPoolLinkSilo,
    ) -> CreateResult<db::model::IpPoolResource> {
        let (authz_pool,) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        let (authz_silo,) = self
            .silo_lookup(&opctx, silo_link.silo.clone())?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .ip_pool_link_silo(
                opctx,
                db::model::IncompleteIpPoolResource {
                    ip_pool_id: authz_pool.id(),
                    resource_type: db::model::IpPoolResourceType::Silo,
                    resource_id: authz_silo.id(),
                    is_default: silo_link.is_default,
                },
            )
            .await
    }

    pub(crate) async fn ip_pool_unlink_silo(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_lookup: &lookup::Silo<'_>,
    ) -> DeleteResult {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;

        self.db_datastore
            .ip_pool_unlink_silo(opctx, &authz_pool, &authz_silo)
            .await
    }

    pub(crate) async fn ip_pool_silo_update(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_lookup: &lookup::Silo<'_>,
        update: &ip_pool::IpPoolSiloUpdate,
    ) -> CreateResult<db::model::IpPoolResource> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;

        self.db_datastore
            .ip_pool_set_default(
                opctx,
                &authz_pool,
                &authz_silo,
                update.is_default,
            )
            .await
    }

    pub(crate) async fn ip_pool_delete(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
    ) -> DeleteResult {
        let (.., authz_pool, db_pool) =
            pool_lookup.fetch_for(authz::Action::Delete).await?;

        self.db_datastore.ip_pool_delete(opctx, &authz_pool, &db_pool).await
    }

    pub(crate) async fn ip_pool_update(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        updates: &ip_pool::IpPoolUpdate,
    ) -> UpdateResult<db::model::IpPool> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        let updates_db = IpPoolUpdate::from(updates.clone());

        self.db_datastore.ip_pool_update(opctx, &authz_pool, updates_db).await
    }

    pub(crate) async fn ip_pool_list_ranges(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        pagparams: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::ListChildren).await?;

        self.db_datastore
            .ip_pool_list_ranges(opctx, &authz_pool, pagparams)
            .await
    }

    pub(crate) async fn ip_pool_add_range(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        range: &ip_pool::IpRange,
    ) -> UpdateResult<db::model::IpPoolRange> {
        let (.., authz_pool, db_pool) =
            pool_lookup.fetch_for(authz::Action::Modify).await?;

        // Validate uniformity and pool type constraints.
        // Extract first/last addresses once and reuse for all validation checks.
        match range {
            ip_pool::IpRange::V4(v4_range) => {
                let first = v4_range.first_address();
                let last = v4_range.last_address();
                let first_is_multicast = first.is_multicast();
                let last_is_multicast = last.is_multicast();

                // Ensure range doesn't span multicast/unicast boundary
                if first_is_multicast != last_is_multicast {
                    return Err(Error::invalid_request(
                        "IP range cannot span multicast and unicast address spaces",
                    ));
                }

                // Validate pool type matches range type
                match db_pool.pool_type {
                    IpPoolType::Multicast => {
                        if !first_is_multicast {
                            return Err(Error::invalid_request(
                                "Cannot add unicast address range to multicast IP pool",
                            ));
                        }
                        validate_multicast_range(range)?;
                    }
                    IpPoolType::Unicast => {
                        if first_is_multicast {
                            return Err(Error::invalid_request(
                                "Cannot add multicast address range to unicast IP pool",
                            ));
                        }
                    }
                }
            }
            ip_pool::IpRange::V6(v6_range) => {
                let first = v6_range.first_address();
                let last = v6_range.last_address();
                let first_is_multicast = first.is_multicast();
                let last_is_multicast = last.is_multicast();

                // Ensure range doesn't span multicast/unicast boundary
                if first_is_multicast != last_is_multicast {
                    return Err(Error::invalid_request(
                        "IP range cannot span multicast and unicast address spaces",
                    ));
                }

                // Validate pool type matches range type
                match db_pool.pool_type {
                    IpPoolType::Multicast => {
                        if !first_is_multicast {
                            return Err(Error::invalid_request(
                                "Cannot add unicast address range to multicast IP pool",
                            ));
                        }
                        validate_multicast_range(range)?;
                    }
                    IpPoolType::Unicast => {
                        if first_is_multicast {
                            return Err(Error::invalid_request(
                                "Cannot add multicast address range to unicast IP pool",
                            ));
                        }
                    }
                }
            }
        }

        self.db_datastore
            .ip_pool_add_range(opctx, &authz_pool, &db_pool, range)
            .await
    }

    pub(crate) async fn ip_pool_assign(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        assignment: ip_pool::IpPoolAssignment,
    ) -> UpdateResult<db::model::IpPool> {
        let (.., authz_pool, db_pool) =
            pool_lookup.fetch_for(authz::Action::Modify).await?;
        let db_assignment = IpPoolAssignment::from(assignment);
        self.db_datastore
            .ip_pool_assign(opctx, &authz_pool, &db_pool, db_assignment)
            .await
    }

    pub(crate) async fn ip_pool_delete_range(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        range: &ip_pool::IpRange,
    ) -> DeleteResult {
        let (.., authz_pool, _db_pool) =
            pool_lookup.fetch_for(authz::Action::Modify).await?;

        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }

    // TODO-cleanup:
    //
    // Remove the service-specific methods when the HTTP endpoints they back
    // are also removed.

    // Resolve the single system-service IP pool of the given version backing
    // the deprecated `ip-pools-service` endpoints. These endpoints predate
    // support for more than one service pool per IP version and can't express
    // which they mean. So at this point, we require exactly one, and return an
    // error if that's not the case. This can only really happen if the client
    // is using mixed API versions: a new one to create a second pool, and the
    // old one to manipulate them. That should be really unlikely, but also
    // point the user to the new API version if they do happen to land here.
    async fn ip_pool_service_single(
        &self,
        opctx: &OpContext,
        version: IpVersion,
    ) -> LookupResult<(authz::IpPool, db::model::IpPool)> {
        // Fetch up to two, so we can distinguish none / one / more-than-one.
        let mut pools = self
            .db_datastore
            .ip_pools_service_lookup_by_version(
                opctx,
                version,
                NonZeroU32::new(2).unwrap(),
            )
            .await?;
        if pools.len() > 1 {
            return Err(Error::invalid_request(
                "more than one system-service IP pool of the requested IP \
                 version exists; update the client to manage these pools",
            ));
        }
        let pool = pools.pop().ok_or_else(|| {
            Error::non_resourcetype_not_found(
                "no system-service IP pool of the requested IP version",
            )
        })?;
        Ok((pool.authz_pool, pool.db_pool))
    }

    pub(crate) async fn ip_pool_service_fetch(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<db::model::IpPool> {
        let (authz_pool, db_pool) =
            self.ip_pool_service_single(opctx, IpVersion::V4).await?;
        opctx.authorize(authz::Action::Read, &authz_pool).await?;
        Ok(db_pool)
    }

    pub(crate) async fn ip_pool_service_list_ranges(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        let (authz_pool, ..) =
            self.ip_pool_service_single(opctx, IpVersion::V4).await?;
        opctx.authorize(authz::Action::Read, &authz_pool).await?;
        self.db_datastore
            .ip_pool_list_ranges(opctx, &authz_pool, pagparams)
            .await
    }

    pub(crate) async fn ip_pool_service_add_range(
        &self,
        opctx: &OpContext,
        range: &ip_pool::IpRange,
    ) -> UpdateResult<db::model::IpPoolRange> {
        let (authz_pool, db_pool) =
            self.ip_pool_service_single(opctx, range.version().into()).await?;
        opctx.authorize(authz::Action::Modify, &authz_pool).await?;

        // IPv6 ranges are supported, but only through the current
        // `/v1/system/ip-pools` endpoints. This deprecated service-pool
        // endpoint is still IPv4-only, so we reject V6 ranges.
        if matches!(range, ip_pool::IpRange::V6(_)) {
            return Err(Error::invalid_request(
                "IPv6 ranges are not allowed yet",
            ));
        }

        // Validate uniformity and pool type constraints.
        // Extract first/last addresses once and reuse for all validation checks.
        match range {
            ip_pool::IpRange::V4(v4_range) => {
                let first = v4_range.first_address();
                let last = v4_range.last_address();
                let first_is_multicast = first.is_multicast();
                let last_is_multicast = last.is_multicast();

                // Ensure range doesn't span multicast/unicast boundary
                if first_is_multicast != last_is_multicast {
                    return Err(Error::invalid_request(
                        "IP range cannot span multicast and unicast address spaces",
                    ));
                }

                // Validate pool type matches range type
                match db_pool.pool_type {
                    IpPoolType::Multicast => {
                        if !first_is_multicast {
                            return Err(Error::invalid_request(
                                "Cannot add unicast address range to multicast IP pool",
                            ));
                        }
                        validate_multicast_range(range)?;
                    }
                    IpPoolType::Unicast => {
                        if first_is_multicast {
                            return Err(Error::invalid_request(
                                "Cannot add multicast address range to unicast IP pool",
                            ));
                        }
                    }
                }
            }
            ip_pool::IpRange::V6(v6_range) => {
                let first = v6_range.first_address();
                let last = v6_range.last_address();
                let first_is_multicast = first.is_multicast();
                let last_is_multicast = last.is_multicast();

                // Ensure range doesn't span multicast/unicast boundary
                if first_is_multicast != last_is_multicast {
                    return Err(Error::invalid_request(
                        "IP range cannot span multicast and unicast address spaces",
                    ));
                }

                // Validate pool type matches range type
                match db_pool.pool_type {
                    IpPoolType::Multicast => {
                        if !first_is_multicast {
                            return Err(Error::invalid_request(
                                "Cannot add unicast address range to multicast IP pool",
                            ));
                        }
                        validate_multicast_range(range)?;
                    }
                    IpPoolType::Unicast => {
                        if first_is_multicast {
                            return Err(Error::invalid_request(
                                "Cannot add multicast address range to unicast IP pool",
                            ));
                        }
                    }
                }
            }
        }

        self.db_datastore
            .ip_pool_add_range(opctx, &authz_pool, &db_pool, range)
            .await
    }

    pub(crate) async fn ip_pool_service_delete_range(
        &self,
        opctx: &OpContext,
        range: &ip_pool::IpRange,
    ) -> DeleteResult {
        // The range already lives in a specific pool; resolve it by a contained
        // address rather than assuming a single service pool.
        let (authz_pool, ..) = self
            .db_datastore
            .ip_pool_fetch_containing_address_for_services(
                opctx,
                range.first_address(),
            )
            .await?;
        opctx.authorize(authz::Action::Modify, &authz_pool).await?;
        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_queries::db::pub_test_utils::helpers::create_service_ip_pool;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::address::IpRange;
    use omicron_common::address::Ipv4Range;
    use slog::o;
    use std::net::{Ipv4Addr, Ipv6Addr};

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // The deprecated `ip-pools-service` range endpoints resolve "the" service
    // pool by IP version. With exactly one such pool they behave as before.
    // With more than one, they can no longer tell which pool the caller means
    // and must fail rather than guess.
    #[nexus_test(server = crate::Server)]
    async fn test_ip_pool_service_add_range_requires_single_pool(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = nexus_db_queries::context::OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );

        // Test context has exactly one IPv4 service pool, so the API should
        // work fine in this case.
        let range = IpRange::V4(
            Ipv4Range::new(
                "203.0.113.10".parse().unwrap(),
                "203.0.113.20".parse().unwrap(),
            )
            .unwrap(),
        );
        nexus
            .ip_pool_service_add_range(&opctx, &range)
            .await
            .expect("add range succeeds with a single service pool");

        // Create a second IPv4 system-service pool. The deprecated path can no
        // longer disambiguate, so it fails rather than guessing.
        create_service_ip_pool(
            &opctx,
            datastore,
            "oxide-service-pool-v4",
            omicron_common::api::external::IpVersion::V4,
        )
        .await;
        let v4_service_pools = datastore
            .ip_pools_service_lookup_by_version(
                &opctx,
                IpVersion::V4,
                NonZeroU32::new(3).unwrap(),
            )
            .await
            .expect("listed v4 system-service pools");
        assert_eq!(
            v4_service_pools.len(),
            2,
            "expected exactly two IPv4 system-service pools",
        );

        let range2 = IpRange::V4(
            Ipv4Range::new(
                "203.0.113.30".parse().unwrap(),
                "203.0.113.40".parse().unwrap(),
            )
            .unwrap(),
        );
        let err = nexus
            .ip_pool_service_add_range(&opctx, &range2)
            .await
            .expect_err("add range fails with more than one service pool");
        assert!(
            err.to_string().contains("more than one system-service IP pool"),
            "unexpected error: {err}",
        );
    }

    // IPv6 underlay validation tests

    #[test]
    fn test_validate_multicast_rejects_underlay_ipv6_range() {
        // ff04::/64 is reserved for underlay multicast
        let underlay_range = IpRange::V6(
            Ipv6Range::new(
                Ipv6Addr::new(0xff04, 0, 0, 0, 0, 0, 0, 1),
                Ipv6Addr::new(0xff04, 0, 0, 0, 0, 0, 0, 0xff),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&underlay_range);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("underlay"),
            "Error should mention underlay: {err}"
        );
    }

    #[test]
    fn test_validate_multicast_rejects_full_underlay_subnet() {
        // The entire ff04::/64 should be rejected
        let full_underlay = IpRange::V6(
            Ipv6Range::new(
                Ipv6Addr::new(0xff04, 0, 0, 0, 0, 0, 0, 0),
                Ipv6Addr::new(0xff04, 0, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&full_underlay);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_multicast_allows_other_admin_scoped_subnets() {
        // ff04:0:0:1::/64 is within ff04::/16 but not in the underlay /64
        let other_admin_subnet = IpRange::V6(
            Ipv6Range::new(
                Ipv6Addr::new(0xff04, 0, 0, 1, 0, 0, 0, 1),
                Ipv6Addr::new(0xff04, 0, 0, 1, 0, 0, 0, 0xff),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&other_admin_subnet);
        assert!(
            result.is_ok(),
            "ff04:0:0:1::/64 should be allowed: {result:?}"
        );
    }

    #[test]
    fn test_validate_multicast_rejects_range_overlapping_underlay() {
        // Range that spans from outside underlay into it
        let overlapping = IpRange::V6(
            Ipv6Range::new(
                Ipv6Addr::new(0xff04, 0, 0, 0, 0, 0, 0, 0x100),
                Ipv6Addr::new(0xff04, 0, 0, 1, 0, 0, 0, 0x100),
            )
            .unwrap(),
        );
        // This starts inside ff04::/64 and extends beyond it
        let result = validate_multicast_range(&overlapping);
        assert!(
            result.is_err(),
            "Range overlapping underlay should be rejected"
        );
    }

    #[test]
    fn test_validate_multicast_allows_site_local_ipv6() {
        // ff05::/16 (site-local scope) should be allowed
        let site_local = IpRange::V6(
            Ipv6Range::new(
                Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 1),
                Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 0xff),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&site_local);
        assert!(
            result.is_ok(),
            "Site-local ff05:: should be allowed: {result:?}"
        );
    }

    #[test]
    fn test_validate_multicast_rejects_unusable_ipv6_scopes() {
        // Reserved (0, f), interface-local (1), link-local (2), realm-local
        // (3, no Ethernet realm definition), and the RFC 7346 unassigned
        // scopes (6, 7, 9 through d) are rejected regardless of the flags
        // nibble.
        for scoped in [
            "ff00", "ff01", "ff02", "ff03", "ff06", "ff0f", "ff13", "ff17",
            "ff19", "ff1d",
        ] {
            let range = IpRange::V6(
                Ipv6Range::new(
                    format!("{scoped}::1").parse().unwrap(),
                    format!("{scoped}::ff").parse().unwrap(),
                )
                .unwrap(),
            );
            assert!(
                validate_multicast_range(&range).is_err(),
                "{scoped}::/16 should be rejected for its scope"
            );
        }
    }

    #[test]
    fn test_validate_multicast_ipv6_ssm_boundaries() {
        let ssm = IpRange::V6(
            Ipv6Range::new(
                "ff3e::8000:0".parse().unwrap(),
                "ff3e::8000:ff".parse().unwrap(),
            )
            .unwrap(),
        );
        assert!(validate_multicast_range(&ssm).is_ok());

        // Group IDs below 0x40000000 are invalid (RFC 4607 section 1).
        let low_group_ids = IpRange::V6(
            Ipv6Range::new(
                "ff3e::1".parse().unwrap(),
                "ff3e::ff".parse().unwrap(),
            )
            .unwrap(),
        );
        assert!(validate_multicast_range(&low_group_ids).is_err());

        // Group IDs 0x40000000 through 0x7fffffff are reserved or held
        // for IANA allocation (RFC 4607 section 4.3), not locally
        // allocatable.
        let iana_space = IpRange::V6(
            Ipv6Range::new(
                "ff3e::4000:0".parse().unwrap(),
                "ff3e::7fff:ffff".parse().unwrap(),
            )
            .unwrap(),
        );
        assert!(validate_multicast_range(&iana_space).is_err());

        // SSM blocks with unusable or unassigned scope nibbles are rejected
        // even with allocatable group IDs.
        for scoped in
            ["ff30", "ff31", "ff32", "ff33", "ff36", "ff39", "ff3d", "ff3f"]
        {
            let range = IpRange::V6(
                Ipv6Range::new(
                    format!("{scoped}::8000:1").parse().unwrap(),
                    format!("{scoped}::8000:ff").parse().unwrap(),
                )
                .unwrap(),
            );
            assert!(
                validate_multicast_range(&range).is_err(),
                "{scoped}::/32 should be rejected for its scope"
            );
        }

        // Inside the ff3e::/32 SSM block but outside ff3e::/96, so not a
        // valid 32-bit group ID.
        let outside_group_id_space = IpRange::V6(
            Ipv6Range::new(
                "ff3e:0:1234::1".parse().unwrap(),
                "ff3e:0:1234::ff".parse().unwrap(),
            )
            .unwrap(),
        );
        assert!(validate_multicast_range(&outside_group_id_space).is_err());

        // This is unicast-prefix-based ASM inside the broad ff30::/12 prefix,
        // but outside every RFC 4607 ff3x::/32 SSM block.
        let asm = IpRange::V6(
            Ipv6Range::new(
                "ff3e:20:1234::1".parse().unwrap(),
                "ff3e:20:1234::ff".parse().unwrap(),
            )
            .unwrap(),
        );
        assert!(validate_multicast_range(&asm).is_ok());

        let crosses_block_end = IpRange::V6(
            Ipv6Range::new(
                "ff3e::ffff:ffff:ffff:fffe".parse().unwrap(),
                "ff3e:1::1".parse().unwrap(),
            )
            .unwrap(),
        );
        assert!(validate_multicast_range(&crosses_block_end).is_err());

        // Both endpoints are SSM blocks with usable scopes, but the space
        // between scope-specific /32s is ASM.
        let crosses_scopes = IpRange::V6(
            Ipv6Range::new(
                "ff34::8000:1".parse().unwrap(),
                "ff35::8000:1".parse().unwrap(),
            )
            .unwrap(),
        );
        assert!(validate_multicast_range(&crosses_scopes).is_err());
    }

    // IPv4 validation tests

    #[test]
    fn test_validate_multicast_rejects_link_local_ipv4() {
        // 224.0.0.0/24 is reserved (link-local)
        let link_local = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 0, 0, 1),
                Ipv4Addr::new(224, 0, 0, 10),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&link_local);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("link-local"));
    }

    #[test]
    fn test_validate_multicast_allows_valid_ipv4_asm() {
        // 224.1.0.0/24 is valid ASM
        let valid_asm = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 1, 0, 1),
                Ipv4Addr::new(224, 1, 0, 255),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&valid_asm);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_multicast_rejects_asm_ssm_spanning_range() {
        // Range spanning from ASM (224.x-231.x) to SSM (232.x)
        // SSM subnet is 232.0.0.0/8
        let spanning = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(231, 255, 255, 1),
                Ipv4Addr::new(232, 0, 0, 10),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&spanning);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ASM and SSM"));

        // Both endpoints classify ASM but the range strictly contains all
        // of 232/8.
        let contains_ssm = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(231, 0, 0, 1),
                Ipv4Addr::new(233, 0, 0, 1),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&contains_ssm);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ASM and SSM"));
    }

    #[test]
    fn test_validate_multicast_rejects_reserved_ipv4_ssm() {
        // 232.0.0.0/24 is reserved (RFC 4607 section 4.3)
        let reserved = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(232, 0, 0, 1),
                Ipv4Addr::new(232, 0, 0, 10),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&reserved);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("reserved IPv4 SSM"));

        // The remainder of 232.0.0.0/8 is allocatable.
        let valid_ssm = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(232, 0, 1, 0),
                Ipv4Addr::new(232, 0, 1, 255),
            )
            .unwrap(),
        );
        assert!(validate_multicast_range(&valid_ssm).is_ok());
    }
}
