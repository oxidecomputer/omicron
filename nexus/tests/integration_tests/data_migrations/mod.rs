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
//! When advancing the schema baseline, delete the files whose migration
//! names are older than the new baseline, and remove the corresponding
//! `mod` and `register!` lines below.

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
mod disk_types;
mod ereport_everyone_gets_a_slot;
mod fix_session_token_column_order;
mod one_big_ereport_table;
mod populate_db_metadata_nexus;
mod positive_quotas;
mod rename_default_igw_ip_pool;

/// Registers a migration module in the checks map. The module name is
/// converted to the migration directory name by replacing underscores
/// with hyphens, then looked up in `versions` to get the version.
macro_rules! register {
    ($map:ident, $versions:ident, $mod:ident) => {
        let name = stringify!($mod).replace('_', "-");
        let version = $versions
            .get(name.as_str())
            .unwrap_or_else(|| {
                panic!("migration {name:?} not found in KNOWN_VERSIONS")
            })
            .clone();
        $map.insert(version, $mod::checks());
    };
}

pub(crate) fn get_migration_checks() -> BTreeMap<Version, DataMigrationFns> {
    let versions: HashMap<&str, Version> = KNOWN_VERSIONS
        .iter()
        .map(|v| (v.relative_path(), v.semver().clone()))
        .collect();
    let mut map = BTreeMap::new();

    register!(map, versions, populate_db_metadata_nexus);
    register!(map, versions, positive_quotas);
    register!(map, versions, disk_types);
    register!(map, versions, one_big_ereport_table);
    register!(map, versions, blueprint_sled_config_subnet);
    register!(map, versions, blueprint_sled_last_used_ip);
    register!(map, versions, audit_log_credential_id);
    register!(map, versions, fix_session_token_column_order);
    register!(map, versions, bgp_unnumbered_peers);
    register!(map, versions, bgp_config_max_paths_not_null);
    register!(map, versions, ereport_everyone_gets_a_slot);
    register!(map, versions, rename_default_igw_ip_pool);

    map
}
