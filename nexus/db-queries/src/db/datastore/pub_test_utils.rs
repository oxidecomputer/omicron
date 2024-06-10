// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test support code that can be enabled by dependencies via this crate's
//! `testing` feature.
//!
//! This feature should only be enabled under `dev-dependencies` to avoid this
//! test support code leaking into release binaries.

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::DataStore;
use dropshot::test_util::LogContext;
use omicron_test_utils::dev::db::CockroachInstance;
use std::sync::Arc;
use uuid::Uuid;

/// Constructs a DataStore for use in test suites that has preloaded the
/// built-in users, roles, and role assignments that are needed for basic
/// operation
#[cfg(any(test, feature = "testing"))]
pub async fn datastore_test(
    logctx: &LogContext,
    db: &CockroachInstance,
    rack_id: Uuid,
) -> (OpContext, Arc<DataStore>) {
    use crate::authn;

    let cfg = db::Config { url: db.pg_config().clone() };
    let pool = Arc::new(db::Pool::new_qorb_single_host_blocking(&cfg).await);
    let datastore =
        Arc::new(DataStore::new(&logctx.log, pool, None).await.unwrap());

    // Create an OpContext with the credentials of "db-init" just for the
    // purpose of loading the built-in users, roles, and assignments.
    let opctx = OpContext::for_background(
        logctx.log.new(o!()),
        Arc::new(authz::Authz::new(&logctx.log)),
        authn::Context::internal_db_init(),
        Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
    );

    // TODO: Can we just call "Populate" instead of doing this?
    datastore.load_builtin_users(&opctx).await.unwrap();
    datastore.load_builtin_roles(&opctx).await.unwrap();
    datastore.load_builtin_role_asgns(&opctx).await.unwrap();
    datastore.load_builtin_silos(&opctx).await.unwrap();
    datastore.load_builtin_projects(&opctx).await.unwrap();
    datastore.load_builtin_vpcs(&opctx).await.unwrap();
    datastore.load_silo_users(&opctx).await.unwrap();
    datastore.load_silo_user_role_assignments(&opctx).await.unwrap();
    datastore
        .load_builtin_fleet_virtual_provisioning_collection(&opctx)
        .await
        .unwrap();
    datastore.load_builtin_rack_data(&opctx, rack_id).await.unwrap();

    // Create an OpContext with the credentials of "test-privileged" for general
    // testing.
    let opctx = OpContext::for_tests(
        logctx.log.new(o!()),
        Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
    );

    (opctx, datastore)
}
