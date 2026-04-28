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

mod audit_log_credential_id;
mod bgp_config_max_paths_not_null;
mod bgp_unnumbered_peers;
mod blueprint_sled_config_subnet;
mod blueprint_sled_last_used_ip;
mod boot_partitions_inventory;
mod delete_nexus_default_allow_firewall_rule;
mod disk_types;
mod ereport_everyone_gets_a_slot;
mod fix_leaked_bp_oximeter_read_policy_rows;
mod fix_session_token_column_order;
mod inv_clear_mupdate_override;
mod one_big_ereport_table;
mod populate_db_metadata_nexus;
mod positive_quotas;
mod rename_default_igw_ip_pool;
mod route_config_rib_priority;
mod vpc_firewall_icmp;
mod zone_image_resolver_inventory;

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

    register!(zone_image_resolver_inventory);
    register!(vpc_firewall_icmp);
    register!(boot_partitions_inventory);
    register!(fix_leaked_bp_oximeter_read_policy_rows);
    register!(route_config_rib_priority);
    register!(inv_clear_mupdate_override);
    register!(populate_db_metadata_nexus);
    register!(positive_quotas);
    register!(disk_types);
    register!(one_big_ereport_table);
    register!(blueprint_sled_config_subnet);
    register!(blueprint_sled_last_used_ip);
    register!(audit_log_credential_id);
    register!(fix_session_token_column_order);
    register!(bgp_unnumbered_peers);
    register!(bgp_config_max_paths_not_null);
    register!(ereport_everyone_gets_a_slot);
    register!(rename_default_igw_ip_pool);
    register!(delete_nexus_default_allow_firewall_rule);

    map
}
