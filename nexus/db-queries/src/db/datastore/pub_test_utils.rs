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
use omicron_test_utils::dev::db::CockroachInstance;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

#[cfg(test)]
mod test {
    use super::*;
    use nexus_test_utils::db::test_setup_database;

    enum TestKind {
        Pool { pool: Arc<db::Pool> },
        RawDatastore { datastore: Arc<DataStore> },
        Datastore { opctx: OpContext, datastore: Arc<DataStore> },
    }

    /// A test database with a pool connected to it.
    pub struct TestDatabase {
        db: CockroachInstance,

        kind: TestKind,
    }

    impl TestDatabase {
        /// Creates a new database for test usage, with a pool.
        ///
        /// [`Self::terminate`] should be called before the test finishes,
        /// or dropping the [`TestDatabase`] will panic.
        pub async fn new_with_pool(log: &Logger) -> Self {
            let db = test_setup_database(log).await;
            let cfg = db::Config { url: db.pg_config().clone() };
            let pool = Arc::new(db::Pool::new_single_host(log, &cfg));
            Self { db, kind: TestKind::Pool { pool } }
        }

        /// Creates a new database for test usage, with a pre-loaded datastore.
        ///
        /// [`Self::terminate`] should be called before the test finishes,
        /// or dropping the [`TestDatabase`] will panic.
        pub async fn new_with_datastore(log: &Logger) -> Self {
            let db = test_setup_database(log).await;
            let (opctx, datastore) =
                crate::db::datastore::test_utils::datastore_test(log, &db)
                    .await;

            Self { db, kind: TestKind::Datastore { opctx, datastore } }
        }

        /// Creates a new database for test usage, with a raw datastore.
        ///
        /// [`Self::terminate`] should be called before the test finishes,
        /// or dropping the [`TestDatabase`] will panic.
        pub async fn new_with_raw_datastore(log: &Logger) -> Self {
            let db = test_setup_database(log).await;
            let cfg = db::Config { url: db.pg_config().clone() };
            let pool = Arc::new(db::Pool::new_single_host(log, &cfg));
            let datastore =
                Arc::new(DataStore::new(&log, pool, None).await.unwrap());
            Self { db, kind: TestKind::RawDatastore { datastore } }
        }

        pub fn pool(&self) -> &Arc<db::Pool> {
            match &self.kind {
                TestKind::Pool { pool } => pool,
                TestKind::RawDatastore { .. } | TestKind::Datastore { .. } => {
                    panic!("Wrong test type; try using `TestDatabase::new_with_pool`");
                }
            }
        }

        pub fn opctx(&self) -> &OpContext {
            match &self.kind {
                TestKind::Pool { .. } | TestKind::RawDatastore { .. } => {
                    panic!("Wrong test type; try using `TestDatabase::new_with_datastore`");
                }
                TestKind::Datastore { opctx, .. } => opctx,
            }
        }

        pub fn datastore(&self) -> &Arc<DataStore> {
            match &self.kind {
                TestKind::Pool { .. } => {
                    panic!("Wrong test type; try using `TestDatabase::new_with_datastore`");
                }
                TestKind::RawDatastore { datastore } => datastore,
                TestKind::Datastore { datastore, .. } => datastore,
            }
        }

        /// Shuts down both the database and the pool
        pub async fn terminate(mut self) {
            match self.kind {
                TestKind::Pool { pool } => pool.terminate().await,
                TestKind::RawDatastore { datastore } => {
                    datastore.terminate().await
                }
                TestKind::Datastore { datastore, .. } => {
                    datastore.terminate().await
                }
            }
            self.db.cleanup().await.unwrap();
        }
    }
}

#[cfg(test)]
pub use test::TestDatabase;

/// Constructs a DataStore for use in test suites that has preloaded the
/// built-in users, roles, and role assignments that are needed for basic
/// operation
#[cfg(any(test, feature = "testing"))]
pub async fn datastore_test(
    log: &Logger,
    db: &CockroachInstance,
    rack_id: Uuid,
) -> (OpContext, Arc<DataStore>) {
    use crate::authn;

    let cfg = db::Config { url: db.pg_config().clone() };
    let pool = Arc::new(db::Pool::new_single_host(&log, &cfg));
    let datastore = Arc::new(DataStore::new(&log, pool, None).await.unwrap());

    // Create an OpContext with the credentials of "db-init" just for the
    // purpose of loading the built-in users, roles, and assignments.
    let opctx = OpContext::for_background(
        log.new(o!()),
        Arc::new(authz::Authz::new(&log)),
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
        log.new(o!()),
        Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
    );

    (opctx, datastore)
}
