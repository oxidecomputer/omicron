// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Per-migration data validation checks.
//!
//! Each file in this module is named after its migration directory in
//! `schema/crdb/` (with hyphens replaced by underscores). The version
//! number is looked up from `KNOWN_VERSIONS` at runtime, so the test
//! code never hard-codes version numbers.
//!
//! Each migration module exports a `pub(crate) fn checks() ->
//! DataMigrationFns` that configures the before/after hooks.
//!
//! When advancing the schema baseline (the oldest-supported version of
//! the schema in the database -- see schema/crdb/README.adoc for additional
//! instructions), delete the files whose migration names are older than the new
//! baseline, and remove the corresponding `mod` and `register!` lines below.

use super::schema::DataMigrationFns;

use nexus_db_model::KNOWN_VERSIONS;
use semver::Version;
use std::collections::BTreeMap;
use std::collections::HashMap;

mod add_rendezvous_sled_bp_availability;
mod add_sled_update_disposition;
mod bgp_unnumbered_peer_cleanup;
mod blueprint_zone_multiple_external_ips;
mod delete_nexus_default_allow_firewall_rule;
mod drop_uninitialized_svc_enabled_not_online_state;
mod ereport_everyone_gets_a_slot;
mod ereport_trim_serial_trailing_nulls;
mod ereporter_restart_latest_ereport;
mod ereporter_restart_order_v2;
mod ereporter_restart_rack_id;
mod inventory_zone_multiple_external_ips;
mod normalize_service_external_ips;
mod prune_service_nat_entries;
mod rename_default_igw_ip_pool;
mod sled_resource_vmm_state;
mod tufaceous_v2;

pub(crate) fn get_migration_checks() -> BTreeMap<Version, DataMigrationFns> {
    let versions: HashMap<&str, Version> = KNOWN_VERSIONS
        .iter()
        .map(|v| (v.relative_path(), v.semver().clone()))
        .collect();
    let mut map = BTreeMap::new();

    // Registers a migration module in the checks map. The module name is
    // converted to the migration directory name by replacing underscores
    // with hyphens, then looked up in `versions` to get the version.
    macro_rules! register {
        ($mod:ident) => {
            let name = stringify!($mod).replace('_', "-");
            let version = versions
                .get(name.as_str())
                .unwrap_or_else(|| {
                    panic!("migration {name:?} not found in KNOWN_VERSIONS")
                })
                .clone();
            map.insert(version, $mod::checks());
        };
    }

    register!(ereport_everyone_gets_a_slot);
    register!(rename_default_igw_ip_pool);
    register!(delete_nexus_default_allow_firewall_rule);
    register!(drop_uninitialized_svc_enabled_not_online_state);
    register!(bgp_unnumbered_peer_cleanup);
    register!(ereport_trim_serial_trailing_nulls);
    register!(sled_resource_vmm_state);
    register!(ereporter_restart_order_v2);
    register!(ereporter_restart_rack_id);
    register!(ereporter_restart_latest_ereport);
    register!(tufaceous_v2);
    register!(prune_service_nat_entries);
    register!(add_sled_update_disposition);
    register!(normalize_service_external_ips);
    register!(add_rendezvous_sled_bp_availability);
    register!(inventory_zone_multiple_external_ips);
    register!(blueprint_zone_multiple_external_ips);

    map
}
