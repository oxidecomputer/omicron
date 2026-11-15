// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP Pools, collections of external IP addresses for guest instances

use dpd_types::mcast::{
    ExternalMulticastIpError, IPV4_SSM_SUBNET, is_ssm_address,
    validate_external_multicast_ipv4, validate_external_multicast_ipv6,
};
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
use omicron_uuid_kinds::GenericUuid;
use ref_cast::RefCast;
use std::matches;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU32;
use uuid::Uuid;

/// Validate a range before adding it to a multicast pool.
///
/// Addresses within the range are checked against Dendrite's external
/// multicast ruleset ([IPv4], [IPv6]), and the range has to be either all ASM
/// or all SSM.
///
/// Note: Dendrite re-checks each group address on group creation, while
/// member source lists are (re-)validated on join.
///
/// [IPv4]: validate_external_multicast_ipv4
/// [IPv6]: validate_external_multicast_ipv6
fn validate_multicast_range(range: &ip_pool::IpRange) -> Result<(), Error> {
    match range {
        ip_pool::IpRange::V4(range) => multicast_ipv4_range_kind(
            range.first_address(),
            range.last_address(),
        ),
        ip_pool::IpRange::V6(range) => multicast_ipv6_range_kind(
            range.first_address(),
            range.last_address(),
        ),
    }
    .map(|_| ())
    .map_err(|err| {
        Error::invalid_request(format!(
            "Cannot add multicast range to IP pool: {err}"
        ))
    })
}

/// The address kind, ASM or SSM, shared by all addresses within a multicast
/// range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MulticastRangeKind {
    Asm,
    Ssm,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
enum MulticastRangeError {
    #[error(transparent)]
    Address(#[from] ExternalMulticastIpError),
    #[error("IP range cannot span ASM and SSM address spaces")]
    Mixed,
}

impl MulticastRangeKind {
    fn of(address: IpAddr) -> Self {
        if is_ssm_address(address) { Self::Ssm } else { Self::Asm }
    }

    fn combine(self, other: Self) -> Result<Self, MulticastRangeError> {
        (self == other).then_some(self).ok_or(MulticastRangeError::Mixed)
    }
}

/// Check addresses within an inclusive IPv4 multicast range against
/// Dendrite's ruleset and report the address kinds it contains.
fn multicast_ipv4_range_kind(
    first: Ipv4Addr,
    last: Ipv4Addr,
) -> Result<MulticastRangeKind, MulticastRangeError> {
    validate_external_multicast_ipv4(first)?;
    validate_external_multicast_ipv4(last)?;
    let ssm_null = IPV4_SSM_SUBNET.first_addr();
    if first <= ssm_null && ssm_null <= last {
        validate_external_multicast_ipv4(ssm_null)?;
    }
    MulticastRangeKind::of(IpAddr::V4(first))
        .combine(MulticastRangeKind::of(IpAddr::V4(last)))
}

/// Check addresses within an inclusive IPv6 multicast range against
/// Dendrite's ruleset and report the address kinds it contains.
fn multicast_ipv6_range_kind(
    first: Ipv6Addr,
    last: Ipv6Addr,
) -> Result<MulticastRangeKind, MulticastRangeError> {
    validate_external_multicast_ipv6(first)?;
    validate_external_multicast_ipv6(last)?;
    let kind = MulticastRangeKind::of(IpAddr::V6(first));
    let last = u128::from(last);
    // Non-RP chunks keep flags and scope fixed. Embedded-RP chunks also keep
    // plen and RIID fixed, but the RP prefix may vary.
    let chunk = |first: u128| {
        let flags_and_scope = Ipv6Addr::from(first).segments()[0];
        let embedded_rp =
            flags_and_scope & (IPV6_FLAG_EMBEDDED_RP << IPV6_FLAGS_SHIFT) != 0;
        let suffix_mask =
            if embedded_rp { u128::MAX >> 32 } else { u128::MAX >> 16 };
        (first, (first | suffix_mask).min(last))
    };
    std::iter::successors(Some(chunk(u128::from(first))), |&(_, end)| {
        (end < last).then(|| chunk(end + 1))
    })
    .try_fold(kind, |kind, (first, last)| {
        multicast_ipv6_chunk_kind(first, last)
            .and_then(|next| kind.combine(next))
    })
}

fn multicast_ipv6_embedded_rp_chunk_kind(
    first: u128,
    last: u128,
) -> Result<MulticastRangeKind, MulticastRangeError> {
    // The chunk's first address detects a zero RP prefix. The remaining
    // invalid RP regions begin at fe80 and ff00, while scalar validation
    // applies the plen mask.
    [IPV6_RP_LINK_LOCAL_PREFIX, IPV6_RP_MULTICAST_PREFIX]
        .into_iter()
        .map(|prefix| (first & (u128::MAX << 96)) | (u128::from(prefix) << 80))
        .filter(|candidate| first <= *candidate && *candidate <= last)
        .map(Ipv6Addr::from)
        .try_for_each(validate_external_multicast_ipv6)?;
    Ok(MulticastRangeKind::Asm)
}

fn multicast_ipv6_ssm_prefix_chunk_kind(
    first: u128,
    last: u128,
) -> Result<MulticastRangeKind, MulticastRangeError> {
    // SSM requires a zero plen octet.
    //
    // We check each possible second segment with this specific low octet.
    // Nonzero reserved bits do not change this requirement however.
    let first_segment = u32::from(Ipv6Addr::from(first).segments()[1]);
    let last_segment = u32::from(Ipv6Addr::from(last).segments()[1]);
    let plen_mask = u32::from(IPV6_PREFIX_BASED_PLEN_MASK);
    let first_ssm_segment = (first_segment + plen_mask) & !plen_mask;
    (first_ssm_segment..=last_segment)
        .step_by(usize::from(IPV6_PREFIX_BASED_PLEN_MASK) + 1)
        .map(|segment| {
            (first & (u128::MAX << 112))
                | (u128::from(segment) << 96)
                | u128::from(IPV6_SSM_NULL_GROUP_ID)
        })
        .filter(|candidate| first <= *candidate && *candidate <= last)
        .map(Ipv6Addr::from)
        .try_for_each(validate_external_multicast_ipv6)?;
    let has_ssm = first_ssm_segment <= last_segment;
    let has_asm =
        first_segment != last_segment || first_segment & plen_mask != 0;
    match (has_asm, has_ssm) {
        (true, true) => Err(MulticastRangeError::Mixed),
        (false, true) => Ok(MulticastRangeKind::Ssm),
        _ => Ok(MulticastRangeKind::Asm),
    }
}

fn multicast_ipv6_chunk_kind(
    first: u128,
    last: u128,
) -> Result<MulticastRangeKind, MulticastRangeError> {
    let address = Ipv6Addr::from(first);
    validate_external_multicast_ipv6(address)?;
    let segs = address.segments();
    if segs[0] & (IPV6_FLAG_EMBEDDED_RP << IPV6_FLAGS_SHIFT) != 0 {
        multicast_ipv6_embedded_rp_chunk_kind(first, last)
    } else if segs[0] & IPV6_SSM_PREFIX_MASK == IPV6_SSM_PREFIX {
        multicast_ipv6_ssm_prefix_chunk_kind(first, last)
    } else {
        Ok(MulticastRangeKind::Asm)
    }
}

// Flags field: the nibble above scope, where R marks an embedded RP
// (RFC 4291 §2.7, RFC 3956 §3).
const IPV6_FLAGS_SHIFT: u16 = 4;
const IPV6_FLAG_EMBEDDED_RP: u16 = 0x4;

// Prefix length in the low octet of the second segment (RFC 3306 §4).
const IPV6_PREFIX_BASED_PLEN_MASK: u16 = 0xff;

// SSM flag pattern: R=0, P=T=1, with X and scope outside the mask
// (RFC 7371 §4.1.2). The null group ID ff3x::4000:0 is reserved
// (RFC 4607 §4.3).
const IPV6_SSM_PREFIX: u16 = 0xff30;
const IPV6_SSM_PREFIX_MASK: u16 = 0xff70;
const IPV6_SSM_NULL_GROUP_ID: u32 = 0x4000_0000;

// First segments of fe80::/10 and ff00::/8, where a derived RP becomes
// unusable (RFC 3956 §4).
const IPV6_RP_LINK_LOCAL_PREFIX: u16 = 0xfe80;
const IPV6_RP_MULTICAST_PREFIX: u16 = 0xff00;

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
                    resource_id: authz_silo.id().into_untyped_uuid(),
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
    use dpd_types::mcast::ExternalMulticastIp;
    use nexus_db_queries::db::pub_test_utils::helpers::create_service_ip_pool;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::address::IpRange;
    use omicron_common::address::Ipv4Range;
    use omicron_common::address::Ipv6Range;
    use proptest::prelude::*;
    use slog::o;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use test_strategy::proptest;

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
        // SSM subnet is 232.0.0.0/8
        let spanning = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(232, 255, 255, 254),
                Ipv4Addr::new(233, 0, 0, 1),
            )
            .unwrap(),
        );
        let result = validate_multicast_range(&spanning);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ASM and SSM"));
    }

    #[test]
    fn test_validate_multicast_rejects_ipv6_asm_ssm_spanning_range() {
        let range = IpRange::V6(
            Ipv6Range::new(
                "ff3e::ffff:ffff:ffff:ffff:ffff:fffe".parse().unwrap(),
                "ff3e:1::1".parse().unwrap(),
            )
            .unwrap(),
        );
        let err = validate_multicast_range(&range).unwrap_err();
        assert!(err.to_string().contains("ASM and SSM"), "Received: {err}");
    }

    fn range_kind(
        first: &str,
        last: &str,
    ) -> Result<MulticastRangeKind, MulticastRangeError> {
        let parse = |addr: &str| -> IpAddr {
            addr.parse()
                .unwrap_or_else(|err| panic!("failed to parse {addr}: {err}"))
        };
        match (parse(first), parse(last)) {
            (IpAddr::V4(first), IpAddr::V4(last)) => {
                multicast_ipv4_range_kind(first, last)
            }
            (IpAddr::V6(first), IpAddr::V6(last)) => {
                multicast_ipv6_range_kind(first, last)
            }
            _ => panic!("{first} and {last} differ in address family"),
        }
    }

    #[test]
    fn test_multicast_range_kind() {
        // Each of the first three ranges holds four addresses. Changing scope,
        // plen, or RP does not necessarily invalidate an address.
        for (first, last, expected) in [
            (
                "ff15:ffff:ffff:ffff:ffff:ffff:ffff:fffe",
                "ff16::1",
                MulticastRangeKind::Asm,
            ),
            (
                "ff3e:1:ffff:ffff:ffff:ffff:ffff:fffe",
                "ff3e:2::1",
                MulticastRangeKind::Asm,
            ),
            (
                "ff7e:140:2001:db8:ffff:ffff:ffff:fffe",
                "ff7e:140:2001:db9::1",
                MulticastRangeKind::Asm,
            ),
            // With plen 7, both boundary candidates produce the usable RP
            // fe00::1.
            (
                "ff7e:107:fe7f:ffff:ffff:ffff:ffff:ffff",
                "ff7e:107:ff00::1",
                MulticastRangeKind::Asm,
            ),
            // The second segment is ffff, and rounding up to the next zero
            // plen octet must not overflow.
            (
                "ff3e:ffff::1",
                "ff3e:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                MulticastRangeKind::Asm,
            ),
            // The address kind depends on the group address.
            ("224.1.0.1", "224.1.0.255", MulticastRangeKind::Asm),
            ("232.1.0.0", "232.1.0.255", MulticastRangeKind::Ssm),
            ("ff3e::f000:0", "ff3e::fcff:ffff", MulticastRangeKind::Ssm),
            ("ffbe:1000::1", "ffbe:1000::ffff", MulticastRangeKind::Ssm),
        ] {
            let kind = range_kind(first, last)
                .unwrap_or_else(|err| panic!("{first} through {last}: {err}"));
            assert_eq!(kind, expected, "{first} through {last}");
        }
    }

    #[test]
    fn test_multicast_range_rejects_asm_ssm_overlap() {
        for (first, last) in [
            ("232.255.255.254", "233.0.0.1"),
            ("ff3e::ffff:ffff:ffff:ffff:ffff:fffe", "ff3e:1::1"),
            // The plen octet wraps from ff to 00, and the range crosses from
            // ASM to SSM.
            ("ff3e:ff:ffff:ffff:ffff:ffff:ffff:fffe", "ff3e:100::1"),
        ] {
            assert_eq!(
                range_kind(first, last),
                Err(MulticastRangeError::Mixed),
                "{first} through {last}",
            );
        }
    }

    #[test]
    fn test_multicast_range_rejects_invalid_interior() {
        type ExpectedError = fn(&ExternalMulticastIpError) -> bool;
        let cases: [(&str, &str, ExpectedError); 7] = [
            // Both endpoints are ASM, but the interval contains the SSM null
            // address 232.0.0.0.
            ("231.255.255.255", "233.0.0.1", |err| {
                matches!(err, ExternalMulticastIpError::ReservedSsmNull(_))
            }),
            ("ff3e::3fff:ffff", "ff3e::4000:1", |err| {
                matches!(err, ExternalMulticastIpError::ReservedSsmNull(_))
            }),
            // The ffbe case covers X=1 and nonzero reserved bits.
            ("ffbe:1000::3fff:ffff", "ffbe:1000::4000:1", |err| {
                matches!(err, ExternalMulticastIpError::ReservedSsmNull(_))
            }),
            // ff06:: is a reserved base address, while ff20:: begins reserved
            // scope 0.
            ("ff05::1", "ff06::1", |err| {
                matches!(err, ExternalMulticastIpError::ReservedBaseAddress(_))
            }),
            ("ff1f::1", "ff33::1", |err| {
                matches!(err, ExternalMulticastIpError::ReservedIpv6Scope(_))
            }),
            // The derived RP crosses fe80::/10 between usable endpoints.
            (
                "ff7e:140:fe7f:ffff:ffff:ffff:ffff:ffff",
                "ff7e:140:fec0::1",
                |err| {
                    matches!(
                        err,
                        ExternalMulticastIpError::MalformedEmbeddedRp { .. }
                    )
                },
            ),
            // The next /32 includes ff7e:140::, whose RP is ::1.
            ("ff7e:13f:2001:db8::1", "ff7e:140:2001:db8::1", |err| {
                matches!(
                    err,
                    ExternalMulticastIpError::MalformedEmbeddedRp { .. }
                )
            }),
        ];
        for (first, last, expected) in cases {
            // Both ends must remain valid, so the rejection comes from an
            // interior address.
            for endpoint in [first, last] {
                let addr = endpoint.parse().unwrap_or_else(|err| {
                    panic!("failed to parse {endpoint}: {err}")
                });
                ExternalMulticastIp::new(addr).unwrap_or_else(|err| {
                    panic!("{endpoint} should be valid: {err}")
                });
            }
            let result = range_kind(first, last);
            assert!(
                matches!(
                    &result,
                    Err(MulticastRangeError::Address(err)) if expected(err)
                ),
                "{first} through {last}: {result:?}",
            );
        }
    }

    /// Check a range one address at a time.
    ///
    /// Returns the range kind when every address passes
    /// `ExternalMulticastIp::new` or `None`.
    fn scalar_range_kind(
        mut addrs: impl Iterator<Item = IpAddr>,
    ) -> Option<MulticastRangeKind> {
        let first = addrs.next()?;
        let kind = |addr| {
            ExternalMulticastIp::new(addr)
                .ok()
                .map(|_| MulticastRangeKind::of(addr))
        };
        let first_kind = kind(first)?;
        addrs.all(|addr| kind(addr) == Some(first_kind)).then_some(first_kind)
    }

    fn range_error_has_scalar_witness(
        actual: &Result<MulticastRangeKind, MulticastRangeError>,
        mut addrs: impl Iterator<Item = IpAddr>,
    ) -> bool {
        match actual {
            Ok(_) => true,
            Err(MulticastRangeError::Address(expected)) => addrs.any(|addr| {
                ExternalMulticastIp::new(addr).err().as_ref() == Some(expected)
            }),
            Err(MulticastRangeError::Mixed) => {
                let mut kinds = addrs
                    .filter_map(|addr| ExternalMulticastIp::new(addr).ok())
                    .map(|group| group.is_ssm());
                let Some(first_kind) = kinds.next() else {
                    return false;
                };
                kinds.any(|kind| kind != first_kind)
            }
        }
    }

    // Bias strategy toward policy boundaries. Random addresses rarely
    // trigger a "useful" one within the 256-address limit used by the
    // scalar check.
    fn range_v4_start_strategy() -> impl Strategy<Value = u32> {
        prop_oneof![
            any::<u32>(),
            0xe000_0000u32..=0xefff_ffff,
            (
                proptest::sample::select(vec![
                    0xe000_0000u32,
                    0xe000_0100,
                    0xe800_0000,
                    0xe900_0000,
                    0xf000_0000,
                    u32::MAX,
                ]),
                0u8..=8,
            )
                .prop_map(|(boundary, offset)| {
                    boundary.saturating_sub(u32::from(offset))
                }),
        ]
    }

    fn range_v6_start_strategy() -> impl Strategy<Value = u128> {
        // Reserved base addresses, scope edges, SSM null groups, and
        // embedded-RP prefix edges.
        let boundaries =
            [
                "ff04::",
                "ff04:0:0:1::",
                "ff06::",
                "ff16::",
                "ff3e::4000:0",
                "ffbe:1000::4000:0",
                "ff3e:1::",
                "ff3e:2::",
                "ff3e:100::",
                "ff7e:140:2001:db9::",
                "ff7e:140:fe80::",
                "ff7e:140:ff00::",
                "ff7e:107:ff00::",
                "ff7e:108:fe80::",
                "ff7e:10a:fe80::",
                "ff7e:109:fe80::",
                "ff7e:104:1000::",
                "ff7e:140::",
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            ]
            .into_iter()
            .map(|addr| {
                u128::from(addr.parse::<Ipv6Addr>().unwrap_or_else(|err| {
                    panic!("failed to parse {addr}: {err}")
                }))
            })
            .collect::<Vec<_>>();
        // Embedded-RP groups (RFC 3956) at or just before an all-zero RP
        // prefix, fe80::, or ff00::. Subtracting the offset from an
        // all-zero RP prefix borrows into plen and RIID, or into scope and
        // flags when both equal zero. The plen value spans 0 through 65, one
        // past each end of the valid 1 to 64 range.
        let embedded_rp = (
            0u16..=0xf,
            0u16..=0xf,
            0u16..=65,
            proptest::sample::select(vec![0u16, 0xfe80, 0xff00]),
            0u8..=8,
        )
            .prop_map(|(scope, riid, plen, rp, offset)| {
                let addr = Ipv6Addr::new(
                    0xff70 | scope,
                    (riid << 8) | plen,
                    rp,
                    0,
                    0,
                    0,
                    0,
                    0,
                );
                u128::from(addr).saturating_sub(u128::from(offset))
            });

        // SSM groups (ff3x and ffbx) at or just before group ID 0 or the
        // null group ID, across every value of the reserved byte.
        let ssm = (
            proptest::sample::select(vec![0xff30u16, 0xffb0]),
            0u16..=0xf,
            any::<u8>(),
            any::<bool>(),
            0u8..=8,
        )
            .prop_map(|(flags, scope, segment, null, offset)| {
                let group_high = if null { 0x4000 } else { 0 };
                let addr = Ipv6Addr::new(
                    flags | scope,
                    u16::from(segment) << 8,
                    0,
                    0,
                    0,
                    0,
                    group_high,
                    0,
                );
                u128::from(addr).saturating_sub(u128::from(offset))
            });
        prop_oneof![
            any::<u128>().prop_map(|bits| (0xff_u128 << 120) | (bits >> 8)),
            (proptest::sample::select(boundaries), 0u8..=8).prop_map(
                |(boundary, offset)| {
                    boundary.saturating_sub(u128::from(offset))
                }
            ),
            embedded_rp,
            ssm,
        ]
    }

    fn range_width_strategy() -> impl Strategy<Value = u128> {
        (0u32..=127, any::<u128>()).prop_map(|(shift, width)| width >> shift)
    }

    fn valid_wide_v6_range_strategy()
    -> impl Strategy<Value = (u128, u128, MulticastRangeKind)> {
        let parse =
            |addr: &str| {
                u128::from(addr.parse::<Ipv6Addr>().unwrap_or_else(|err| {
                    panic!("failed to parse {addr}: {err}")
                }))
            };
        let windows = [
            (
                "ff0e::1",
                "ff0e:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                MulticastRangeKind::Asm,
            ),
            (
                "ff3e::4000:1",
                "ff3e::ffff:ffff:ffff:ffff:ffff:ffff",
                MulticastRangeKind::Ssm,
            ),
            (
                "ffbe:1000::4000:1",
                "ffbe:1000:ffff:ffff:ffff:ffff:ffff:ffff",
                MulticastRangeKind::Ssm,
            ),
            (
                "ff7e:140:2001:db8::",
                "ff7e:140:2001:db9:ffff:ffff:ffff:ffff",
                MulticastRangeKind::Asm,
            ),
        ]
        .into_iter()
        .map(|(first, last, kind)| (parse(first), parse(last), kind))
        .collect::<Vec<_>>();
        proptest::sample::select(windows).prop_flat_map(
            |(first, last, kind)| {
                (first..=last - 256).prop_flat_map(move |first| {
                    (Just(first), first + 256..=last, Just(kind))
                })
            },
        )
    }

    fn wide_v6_range_with_invalid_interior_strategy()
    -> impl Strategy<Value = (u128, u128, u128)> {
        let ssm_null = (
            proptest::sample::select(vec![0xff30u16, 0xffb0]),
            3u16..=0xf,
            any::<u8>(),
        )
            .prop_map(|(flags, scope, reserved)| {
                u128::from(Ipv6Addr::new(
                    flags | scope,
                    u16::from(reserved) << 8,
                    0,
                    0,
                    0,
                    0,
                    0x4000,
                    0,
                ))
            });
        let reserved_base = (5u16..=0xf).prop_map(|scope| {
            u128::from(Ipv6Addr::new(0xff00 | scope, 0, 0, 0, 0, 0, 0, 0))
        });
        (
            prop_oneof![ssm_null, reserved_base],
            256u16..=u16::MAX,
            256u64..=u64::MAX,
        )
            .prop_map(|(invalid, before, after)| {
                (
                    invalid - u128::from(before),
                    invalid,
                    invalid + u128::from(after),
                )
            })
    }

    fn v6_range_kind(first: u128, last: u128) -> Option<MulticastRangeKind> {
        multicast_ipv6_range_kind(Ipv6Addr::from(first), Ipv6Addr::from(last))
            .ok()
    }

    // The range walk returns the same (range) kind as checking each IPv4
    // address with Dendrite's validator separately.
    #[proptest]
    fn prop_range_v4_kind_matches_scalar(
        #[strategy(range_v4_start_strategy())] first: u32,
        width: u8,
    ) {
        let last = first.saturating_add(u32::from(width));
        let addrs = (first..=last).map(|bits| IpAddr::V4(Ipv4Addr::from(bits)));
        let expected = scalar_range_kind(addrs.clone());
        let actual = multicast_ipv4_range_kind(
            Ipv4Addr::from(first),
            Ipv4Addr::from(last),
        );
        prop_assert_eq!(actual.clone().ok(), expected, "{:?}", actual);
        prop_assert!(
            range_error_has_scalar_witness(&actual, addrs),
            "{actual:?} has no matching scalar witness",
        );
    }

    // The same check for IPv6 ranges of up to 256 addresses.
    #[proptest]
    fn prop_range_v6_kind_matches_scalar(
        #[strategy(range_v6_start_strategy())] first: u128,
        width: u8,
    ) {
        let last = first.saturating_add(u128::from(width));
        let addrs = (first..=last).map(|bits| IpAddr::V6(Ipv6Addr::from(bits)));
        let expected = scalar_range_kind(addrs.clone());
        let actual = multicast_ipv6_range_kind(
            Ipv6Addr::from(first),
            Ipv6Addr::from(last),
        );
        prop_assert_eq!(actual.clone().ok(), expected, "{:?}", actual);
        prop_assert!(
            range_error_has_scalar_witness(&actual, addrs),
            "{actual:?} has no matching scalar witness",
        );
    }

    // Splitting/slicing a range at any point and combining both halves
    // should produce same result as the whole range. This covers ranges
    // too wide to check address by address.
    #[proptest]
    fn prop_range_v6_kind_splits_consistently(
        #[strategy(range_v6_start_strategy())] first: u128,
        #[strategy(range_width_strategy())] width: u128,
        split: u128,
    ) {
        let last = first.saturating_add(width);
        if first < last {
            let middle = first + split % (last - first);
            let expected = v6_range_kind(first, middle)
                .zip(v6_range_kind(middle + 1, last))
                .and_then(|(head, tail)| head.combine(tail).ok());
            prop_assert_eq!(
                v6_range_kind(first, last),
                expected,
                "{} through {} split after {}",
                Ipv6Addr::from(first),
                Ipv6Addr::from(last),
                Ipv6Addr::from(middle),
            );
        }
    }

    // Addresses within an accepted range pass Dendrite's validator
    // and have the range's kind.
    #[proptest]
    fn prop_range_v6_kind_holds_for_interior_addresses(
        #[strategy(range_v6_start_strategy())] first: u128,
        #[strategy(range_width_strategy())] width: u128,
        offset: u128,
    ) {
        let last = first.saturating_add(width);
        if let Some(kind) = v6_range_kind(first, last) {
            let span = last - first;
            let bits =
                first + span.checked_add(1).map_or(offset, |n| offset % n);
            let addr = IpAddr::V6(Ipv6Addr::from(bits));
            prop_assert!(
                ExternalMulticastIp::new(addr).is_ok(),
                "{addr} in {} through {}",
                Ipv6Addr::from(first),
                Ipv6Addr::from(last),
            );
            prop_assert_eq!(MulticastRangeKind::of(addr), kind);
        }
    }

    #[proptest]
    fn prop_valid_wide_v6_ranges_are_accepted(
        #[strategy(valid_wide_v6_range_strategy())] range: (
            u128,
            u128,
            MulticastRangeKind,
        ),
        offset: u128,
    ) {
        let (first, last, kind) = range;
        prop_assert_eq!(
            multicast_ipv6_range_kind(
                Ipv6Addr::from(first),
                Ipv6Addr::from(last),
            ),
            Ok(kind),
            "{} through {}",
            Ipv6Addr::from(first),
            Ipv6Addr::from(last),
        );
        let sample = first + offset % (last - first + 1);
        for bits in [first, sample, last] {
            let addr = IpAddr::V6(Ipv6Addr::from(bits));
            prop_assert_eq!(
                ExternalMulticastIp::new(addr).map(|group| group.is_ssm()),
                Ok(kind == MulticastRangeKind::Ssm),
                "{}",
                addr,
            );
        }
    }

    #[proptest]
    fn prop_wide_v6_ranges_reject_invalid_interiors(
        #[strategy(wide_v6_range_with_invalid_interior_strategy())] range: (
            u128,
            u128,
            u128,
        ),
    ) {
        let (first, invalid, last) = range;
        for bits in [first, last] {
            let addr = IpAddr::V6(Ipv6Addr::from(bits));
            prop_assert!(
                ExternalMulticastIp::new(addr).is_ok(),
                "{addr} should be valid",
            );
        }
        let invalid = IpAddr::V6(Ipv6Addr::from(invalid));
        let expected = ExternalMulticastIp::new(invalid)
            .expect_err("generated interior address should be rejected");
        prop_assert_eq!(
            multicast_ipv6_range_kind(
                Ipv6Addr::from(first),
                Ipv6Addr::from(last),
            ),
            Err(MulticastRangeError::Address(expected)),
            "{} through {} contains {}",
            Ipv6Addr::from(first),
            Ipv6Addr::from(last),
            invalid,
        );
    }
}
