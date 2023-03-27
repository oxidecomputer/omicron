// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database testing facilities.

use omicron_test_utils::dev;
use slog::Logger;
use std::path::PathBuf;

/// Path to the "seed" CockroachDB directory.
///
/// Populating CockroachDB unfortunately isn't free - creation of
/// tables, indices, and users takes several seconds to complete.
///
/// By creating a "seed" version of the database, we can cut down
/// on the time spent performing this operation. Instead, we opt
/// to copy the database from this seed location.
fn seed_dir() -> PathBuf {
    PathBuf::from(concat!(env!("OUT_DIR"), "/crdb-base"))
}

/// Wrapper around [`dev::test_setup_database`] which uses a a
/// seed directory provided at build-time.
pub async fn test_setup_database(log: &Logger) -> dev::db::CockroachInstance {
    let dir = seed_dir();
    dev::test_setup_database(log, &dir).await
}

/// SQL used to enable full table scans for the duration of the current
/// transaction.
///
/// We normally disallow table scans in effort to identify scalability issues
/// during development. But it's preferable for some ad hoc test-only queries to
/// do table scans (rather than add indexes that are only used for the test
/// suite).
///
/// This SQL appears to have no effect when used outside of a transaction.
/// That's intentional.  We do not want to use `SET` (rather than `SET LOCAL`)
/// here because that would change the behavior for any code that happens to use
/// the same pooled connection after this SQL gets run.
pub const ALLOW_FULL_TABLE_SCAN_SQL: &str =
    "set local disallow_full_table_scans = off; set local large_full_scan_rows = 1000;";

use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use slog::o;
use std::sync::Arc;
use uuid::Uuid;
/// Constructs a DataStore for use in test suites that has preloaded the
/// built-in users, roles, and role assignments that are needed for basic
/// operation
pub async fn datastore_test(
    logctx: &dropshot::test_util::LogContext,
    db: &omicron_test_utils::dev::db::CockroachInstance,
) -> (OpContext, Arc<DataStore>) {
    let cfg = db::Config { url: db.pg_config().clone() };
    let pool = Arc::new(db::Pool::new(&cfg));
    let datastore = Arc::new(DataStore::new(pool));

    // Create an OpContext with the credentials of "db-init" just for the
    // purpose of loading the built-in users, roles, and assignments.
    let opctx = OpContext::for_background(
        logctx.log.new(o!()),
        Arc::new(authz::Authz::new(&logctx.log)),
        authn::Context::internal_db_init(),
        Arc::clone(&datastore),
    );

    // TODO: Can we just call "Populate" instead of doing this?
    let rack_id = Uuid::parse_str(crate::RACK_UUID).unwrap();
    datastore.load_builtin_users(&opctx).await.unwrap();
    datastore.load_builtin_roles(&opctx).await.unwrap();
    datastore.load_builtin_role_asgns(&opctx).await.unwrap();
    datastore.load_builtin_silos(&opctx).await.unwrap();
    datastore.load_silo_users(&opctx).await.unwrap();
    datastore.load_silo_user_role_assignments(&opctx).await.unwrap();
    datastore
        .load_builtin_fleet_virtual_provisioning_collection(&opctx)
        .await
        .unwrap();
    datastore.load_builtin_rack_data(&opctx, rack_id).await.unwrap();

    // Create an OpContext with the credentials of "test-privileged" for general
    // testing.
    let opctx =
        OpContext::for_tests(logctx.log.new(o!()), Arc::clone(&datastore));

    (opctx, datastore)
}
