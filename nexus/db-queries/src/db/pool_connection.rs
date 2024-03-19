// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Customization that happens on each connection as they're acquired.

use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use async_bb8_diesel::Connection;
use async_bb8_diesel::ConnectionError;
use async_trait::async_trait;
use bb8::CustomizeConnection;
use diesel::pg::GetPgMetadataCache;
use diesel::pg::PgMetadataCacheKey;
use diesel::prelude::*;
use diesel::PgConnection;
use diesel_dtrace::DTraceConnection;
use std::collections::HashMap;
use tokio::sync::Mutex;

pub type DbConnection = DTraceConnection<PgConnection>;

// This is a list of all user-defined types (ENUMS) in the current DB schema.
//
// Diesel looks up user-defined types as they are encountered, and loads
// them into a metadata cache. Although this cost is amortized over the lifetime
// of a connection, this can be slower than desired:
// - Diesel issues a round-trip database call on each user-defined type
// - The cache of OIDs for user-defined types is "per-connection", so when
// using a connection pool, we redo all these calls for new connections.
//
// To mitigate: We look up a list of user-defined types here on first access
// to the connection, and pre-populate the cache. Furthermore, we save this
// information and use it to populate other connections too, without incurring
// another database lookup.
//
// See https://github.com/oxidecomputer/omicron/issues/4733 for more context.
static CUSTOM_TYPE_KEYS: &'static [&'static str] = &[
    "address_lot_kind",
    "authentication_mode",
    "bfd_mode",
    "block_size",
    "caboose_which",
    "dataset_kind",
    "dns_group",
    "downstairs_client_stop_request_reason_type",
    "downstairs_client_stopped_reason_type",
    "hw_power_state",
    "hw_rot_slot",
    "identity_type",
    "instance_state",
    "ip_attach_state",
    "ip_kind",
    "ip_pool_resource_type",
    "network_interface_kind",
    "physical_disk_kind",
    "producer_kind",
    "provider_type",
    "root_of_trust_page_which",
    "router_route_kind",
    "saga_state",
    "service_kind",
    "sled_policy",
    "sled_resource_kind",
    "sled_role",
    "sled_state",
    "snapshot_state",
    "sp_type",
    "switch_interface_kind",
    "switch_link_fec",
    "switch_link_speed",
    "switch_port_geometry",
    "upstairs_repair_notification_type",
    "upstairs_repair_type",
    "user_provision_type",
    "vpc_firewall_rule_action",
    "vpc_firewall_rule_direction",
    "vpc_firewall_rule_protocol",
    "vpc_firewall_rule_status",
    "vpc_router_kind",
    "zone_type",
];
const CUSTOM_TYPE_SCHEMA: &'static str = "public";

pub const DISALLOW_FULL_TABLE_SCAN_SQL: &str =
    "set disallow_full_table_scans = on; set large_full_scan_rows = 0;";

#[derive(Debug)]
struct OIDCache(HashMap<PgMetadataCacheKey<'static>, (u32, u32)>);

impl OIDCache {
    // Populate a new OID cache by pre-filling values
    async fn new(
        conn: &mut Connection<DbConnection>,
    ) -> Result<Self, ConnectionError> {
        // Lookup all the OIDs for custom types.
        //
        // As a reminder, this is an optimization:
        // - If we supply a value in CUSTOM_TYPE_KEYS that does not
        // exist in the schema, the corresponding row won't be
        // found, so the value will be ignored.
        // - If we don't supply a value in CUSTOM_TYPE_KEYS, even
        // though it DOES exist in the schema, it'll likewise not
        // get pre-populated into the cache. Diesel would observe
        // the cache miss, and perform the lookup later.
        let results: Vec<PgTypeMetadata> = pg_type::table
            .select((pg_type::typname, pg_type::oid, pg_type::typarray))
            .inner_join(
                pg_namespace::table
                    .on(pg_type::typnamespace.eq(pg_namespace::oid)),
            )
            .filter(pg_type::typname.eq_any(CUSTOM_TYPE_KEYS))
            .filter(pg_namespace::nspname.eq(CUSTOM_TYPE_SCHEMA))
            .load_async(&*conn)
            .await?;

        // Convert the OIDs into a ("Cache Key", "OID Tuple") pair,
        // and store the result in a HashMap.
        //
        // We'll iterate over this HashMap to pre-populate the connection-local cache for all
        // future connections, including this one.
        Ok::<_, ConnectionError>(Self(HashMap::from_iter(
            results.into_iter().map(
                |PgTypeMetadata { typname, oid, array_oid }| {
                    (
                        PgMetadataCacheKey::new(
                            Some(CUSTOM_TYPE_SCHEMA.into()),
                            std::borrow::Cow::Owned(typname),
                        ),
                        (oid, array_oid),
                    )
                },
            ),
        )))
    }
}

// String-based representation of the CockroachDB version.
//
// We currently do minimal parsing of this value, but it should
// be distinct between different revisions of CockroachDB.
// This version includes the semver version of the DB, but also
// build and target information.
#[derive(Debug, Eq, PartialEq, Hash)]
struct CockroachVersion(String);

impl CockroachVersion {
    async fn new(
        conn: &Connection<DbConnection>,
    ) -> Result<Self, ConnectionError> {
        diesel::sql_function!(fn version() -> Text);

        let version =
            diesel::select(version()).get_result_async::<String>(conn).await?;
        Ok(Self(version))
    }
}

/// A customizer for all new connections made to CockroachDB, from Diesel.
#[derive(Debug)]
pub(crate) struct ConnectionCustomizer {
    oid_caches: Mutex<HashMap<CockroachVersion, OIDCache>>,
}

impl ConnectionCustomizer {
    pub(crate) fn new() -> Self {
        Self { oid_caches: Mutex::new(HashMap::new()) }
    }

    async fn populate_metadata_cache(
        &self,
        conn: &mut Connection<DbConnection>,
    ) -> Result<(), ConnectionError> {
        // Look up the CockroachDB version for new connections, to ensure
        // that OID caches are distinct between different CRDB versions.
        //
        // This step is performed out of an abundance of caution: OIDs are not
        // necessarily stable across major releases of CRDB, and this ensures
        // that the OID lookups on custom types do not cross this version
        // boundary.
        let version = CockroachVersion::new(conn).await?;

        // Lookup the OID cache, or populate it if we haven't previously
        // established a connection to this database version.
        let mut oid_caches = self.oid_caches.lock().await;
        let entry = oid_caches.entry(version);
        use std::collections::hash_map::Entry::*;
        let oid_cache = match entry {
            Occupied(ref entry) => entry.get(),
            Vacant(entry) => entry.insert(OIDCache::new(conn).await?),
        };

        // Copy the OID cache into this specific connection.
        //
        // NOTE: I don't love that this is blocking (due to "as_sync_conn"), but the
        // "get_metadata_cache" method does not seem implemented for types that could have a
        // non-Postgres backend.
        let mut sync_conn = conn.as_sync_conn();
        let cache = sync_conn.get_metadata_cache();
        for (k, v) in &oid_cache.0 {
            cache.store_type(k.clone(), *v);
        }
        Ok(())
    }

    async fn disallow_full_table_scans(
        &self,
        conn: &mut Connection<DbConnection>,
    ) -> Result<(), ConnectionError> {
        conn.batch_execute_async(DISALLOW_FULL_TABLE_SCAN_SQL).await?;
        Ok(())
    }
}

#[async_trait]
impl CustomizeConnection<Connection<DbConnection>, ConnectionError>
    for ConnectionCustomizer
{
    async fn on_acquire(
        &self,
        conn: &mut Connection<DbConnection>,
    ) -> Result<(), ConnectionError> {
        self.populate_metadata_cache(conn).await?;
        self.disallow_full_table_scans(conn).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Queryable)]
pub struct PgTypeMetadata {
    typname: String,
    oid: u32,
    array_oid: u32,
}

table! {
    pg_type (oid) {
        oid -> Oid,
        typname -> Text,
        typarray -> Oid,
        typnamespace -> Oid,
    }
}

table! {
    pg_namespace (oid) {
        oid -> Oid,
        nspname -> Text,
    }
}

allow_tables_to_appear_in_same_query!(pg_type, pg_namespace);

#[cfg(test)]
mod test {
    use super::*;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;

    // Ensure that the "CUSTOM_TYPE_KEYS" values match the enums
    // we find within the database.
    //
    // If the two are out-of-sync, identify the values causing problems.
    #[tokio::test]
    async fn all_enums_in_prepopulate_list() {
        let logctx = dev::test_setup_log("test_project_creation");
        let mut crdb = test_setup_database(&logctx.log).await;
        let client = crdb.connect().await.expect("Failed to connect to CRDB");

        // https://www.cockroachlabs.com/docs/stable/show-enums
        let rows = client
            .query("SHOW ENUMS FROM omicron.public;", &[])
            .await
            .unwrap_or_else(|_| panic!("failed to list enums"));
        client.cleanup().await.expect("cleaning up after listing enums");

        let mut observed_public_enums = rows
            .into_iter()
            .map(|row| -> String {
                for i in 0..row.len() {
                    if row.columns()[i].name() == "name" {
                        return row.get(i);
                    }
                }
                panic!("Missing 'name' in row: {row:?}");
            })
            .collect::<Vec<_>>();
        observed_public_enums.sort();

        let mut expected_enums: Vec<String> =
            CUSTOM_TYPE_KEYS.into_iter().map(|s| s.to_string()).collect();
        expected_enums.sort();

        pretty_assertions::assert_eq!(
            observed_public_enums,
            expected_enums,
            "Enums did not match.\n\
            If the type is present on the left, but not the right:\n\
            \tThe enum is in the DB, but not in CUSTOM_TYPE_KEYS.\n\
            \tConsider adding it, so we can pre-populate the OID cache.\n\
            If the type is present on the right, but not the left:\n\
            \tThe enum is not the DB, but it is in CUSTOM_TYPE_KEYS.\n\
            \tConsider removing it, because the type no longer exists"
        );

        crdb.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
