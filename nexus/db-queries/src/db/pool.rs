// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database connection pooling
// This whole thing is a placeholder for prototyping.
//
// TODO-robustness TODO-resilience We will want to carefully think about the
// connection pool that we use and its parameters.  It's not clear from the
// survey so far whether an existing module is suitable for our purposes.  See
// the Cueball Internals document for details on the sorts of behaviors we'd
// like here.  Even if by luck we stick with bb8, we definitely want to think
// through the various parameters.
//
// Notes about bb8's behavior:
// * When the database is completely offline, and somebody wants a connection,
//   it still waits for the connection timeout before giving up.  That seems
//   like not what we want.  (To be clear, this is a failure mode where we know
//   the database is offline, not one where it's partitioned and we can't tell.)
// * Although the `build_unchecked()` builder allows the pool to start up with
//   no connections established (good), it also _seems_ to not establish any
//   connections even when it could, resulting in a latency bubble for the first
//   operation after startup.  That's not what we're looking for.
//
// TODO-design Need TLS support (the types below hardcode NoTls).

use super::Config as DbConfig;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use async_bb8_diesel::Connection;
use async_bb8_diesel::ConnectionError;
use async_bb8_diesel::ConnectionManager;
use async_trait::async_trait;
use bb8::CustomizeConnection;
use diesel::PgConnection;
use diesel::prelude::*;
use diesel::pg::GetPgMetadataCache;
use diesel::pg::PgMetadataCacheKey;
use diesel_dtrace::DTraceConnection;
use tokio::sync::OnceCell;

pub type DbConnection = DTraceConnection<PgConnection>;

/// Wrapper around a database connection pool.
///
/// Expected to be used as the primary interface to the database.
pub struct Pool {
    pool: bb8::Pool<ConnectionManager<DbConnection>>,
}

impl Pool {
    pub fn new(log: &slog::Logger, db_config: &DbConfig) -> Self {
        // Make sure diesel-dtrace's USDT probes are enabled.
        usdt::register_probes().expect("Failed to register USDT DTrace probes");
        Self::new_builder(log, db_config, bb8::Builder::new())
    }

    pub fn new_failfast_for_tests(
        log: &slog::Logger,
        db_config: &DbConfig,
    ) -> Self {
        Self::new_builder(
            log,
            db_config,
            bb8::Builder::new()
                .connection_timeout(std::time::Duration::from_millis(1)),
        )
    }

    fn new_builder(
        log: &slog::Logger,
        db_config: &DbConfig,
        builder: bb8::Builder<ConnectionManager<DbConnection>>,
    ) -> Self {
        let url = db_config.url.url();
        let log = log.new(o!(
            "database_url" => url.clone(),
            "component" => "db::Pool"
        ));
        info!(&log, "database connection pool");
        let error_sink = LoggingErrorSink::new(log);
        let manager = ConnectionManager::<DbConnection>::new(url);
        let pool = builder
            .connection_customizer(Box::new(DisallowFullTableScans {}))
            .error_sink(Box::new(error_sink))
            .build_unchecked(manager);
        Pool { pool }
    }

    /// Returns a reference to the underlying pool.
    pub fn pool(&self) -> &bb8::Pool<ConnectionManager<DbConnection>> {
        &self.pool
    }
}

const DISALLOW_FULL_TABLE_SCAN_SQL: &str =
    "set disallow_full_table_scans = on; set large_full_scan_rows = 0;";

#[derive(Debug)]
struct DisallowFullTableScans {}
#[async_trait]
impl CustomizeConnection<Connection<DbConnection>, ConnectionError>
    for DisallowFullTableScans
{
    async fn on_acquire(
        &self,
        conn: &mut Connection<DbConnection>,
    ) -> Result<(), ConnectionError> {
        let keys = vec![
            "sled_provision_state",
            "sled_resource_kind",
            "service_kind",
            "physical_disk_kind",
            "dataset_kind",
            "authentication_mode",
            "user_provision_type",
            "provider_type",
            "instance_state",
            "block_size",
            "snapshot_state",
            "producer_kind",
            "network_interface_kind",
            "vpc_firewall_rule_status",
            "vpc_firewall_rule_direction",
            "vpc_firewall_rule_action",
            "vpc_firewall_rule_protocol",
            "vpc_router_kind",
            "router_route_kind",
            "ip_kind",
            "saga_state",
            "update_artifact_kind",
            "updateable_component_type",
            "update_status",
            "dns_group",
            "identity_type",
            "address_lot_kind",
            "switch_port_geometry",
            "switch_interface_kind",
            "hw_power_state",
            "hw_rot_slot",
            "sp_type",
            "caboose_which",
            "root_of_trust_page_which",
            "switch_link_fec",
            "switch_link_speed",
        ];

        const SCHEMA: &'static str = "public";

        // TODO: Should be stored in a struct, to not be a static variable...
        static OIDS: OnceCell<Vec<InnerPgTypeMetadata>> = OnceCell::const_new();
        let oids = if let Some(oids) = OIDS.get() {
            oids
        } else {
            let oids: Vec<InnerPgTypeMetadata> =
                pg_type::table.select((pg_type::oid, pg_type::typarray))
                    .inner_join(pg_namespace::table)
                    .filter(pg_type::typname.eq_any(keys.clone()))
                    .filter(pg_namespace::nspname.eq(SCHEMA))
                    .load_async(&*conn)
                    .await?;
            let _ = OIDS.set(oids);
            OIDS.get().unwrap()
        };


        // TODO: Don't love that this is blocking? But I'm trying to access the
        // inner type here...
        //
        // TODO: Could I communicate through async-bb8-diesel that this is for
        // PG?
        {
            let mut sync_conn = conn.as_sync_conn();
            let cache = sync_conn.get_metadata_cache();
            let cache_key_for = |name: &'static str| {
                PgMetadataCacheKey::new(Some(SCHEMA.into()), name.into())
            };

            for (key, InnerPgTypeMetadata { oid, array_oid }) in keys.into_iter().zip(oids.into_iter()) {
                cache.store_type(cache_key_for(key), (*oid, *array_oid));
            }
        }

        conn.batch_execute_async(DISALLOW_FULL_TABLE_SCAN_SQL)
            .await
            .map_err(|e| e.into())
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Queryable)]
pub struct InnerPgTypeMetadata {
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

joinable!(pg_type -> pg_namespace(typnamespace));
allow_tables_to_appear_in_same_query!(pg_type, pg_namespace);

#[derive(Clone, Debug)]
struct LoggingErrorSink {
    log: slog::Logger,
}

impl LoggingErrorSink {
    fn new(log: slog::Logger) -> LoggingErrorSink {
        LoggingErrorSink { log }
    }
}

impl bb8::ErrorSink<ConnectionError> for LoggingErrorSink {
    fn sink(&self, error: ConnectionError) {
        error!(
            &self.log,
            "database connection error";
            "error_message" => #%error
        );
    }

    fn boxed_clone(&self) -> Box<dyn bb8::ErrorSink<ConnectionError>> {
        Box::new(self.clone())
    }
}
