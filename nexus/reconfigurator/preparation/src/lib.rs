// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common facilities for assembling inputs to the planner

use nexus_types::deployment::Policy;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolName;
use nexus_types::identity::Asset;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Error;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;

/// Given various pieces of database state that go into the blueprint planning
/// process, produce a `Policy` object encapsulating what the planner needs to
/// generate a blueprint
pub fn policy_from_db(
    sled_rows: &[nexus_db_model::Sled],
    zpool_rows: &[nexus_db_model::Zpool],
    ip_pool_range_rows: &[nexus_db_model::IpPoolRange],
    target_nexus_zone_count: usize,
) -> Result<Policy, Error> {
    let mut zpools_by_sled_id = {
        let mut zpools = BTreeMap::new();
        for z in zpool_rows {
            let sled_zpool_names =
                zpools.entry(z.sled_id).or_insert_with(BTreeSet::new);
            // It's unfortunate that Nexus knows how Sled Agent
            // constructs zpool names, but there's not currently an
            // alternative.
            let zpool_name_generated =
                illumos_utils::zpool::ZpoolName::new_external(z.id())
                    .to_string();
            let zpool_name = ZpoolName::from_str(&zpool_name_generated)
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "unexpectedly failed to parse generated \
                                zpool name: {}: {}",
                        zpool_name_generated, e
                    ))
                })?;
            sled_zpool_names.insert(zpool_name);
        }
        zpools
    };

    let sleds = sled_rows
        .into_iter()
        .map(|sled_row| {
            let sled_id = sled_row.id();
            let subnet = Ipv6Subnet::<SLED_PREFIX>::new(sled_row.ip());
            let zpools = zpools_by_sled_id
                .remove(&sled_id)
                .unwrap_or_else(BTreeSet::new);
            let sled_info = SledResources {
                policy: sled_row.policy(),
                state: sled_row.state().into(),
                subnet,
                zpools,
            };
            (sled_id, sled_info)
        })
        .collect();

    let service_ip_pool_ranges =
        ip_pool_range_rows.iter().map(IpRange::from).collect();

    Ok(Policy { sleds, service_ip_pool_ranges, target_nexus_zone_count })
}
