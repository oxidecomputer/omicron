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
use crate::db::datastore::IdentityCheckPolicy;
use omicron_test_utils::dev::db::CockroachInstance;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

pub mod crdb;
pub mod helpers;
pub mod multicast;

enum Populate {
    Nothing,
    Schema,
    SchemaAndData,
}

enum Interface {
    Nothing,
    Pool,
    Datastore,
}

fn new_pool(log: &Logger, db: &CockroachInstance) -> Arc<db::Pool> {
    let cfg = db::Config { url: db.pg_config().clone() };
    Arc::new(db::Pool::new_single_host(log, &cfg))
}

struct TestDatabaseBuilder {
    populate: Populate,
    interface: Interface,
}

impl TestDatabaseBuilder {
    /// Creates a new database buidler.
    ///
    /// By default, this creates a database with no schema, and with no pools
    /// nor datastores built on top.
    ///
    /// This is equivalent to calling [Self::populate_nothing] and
    /// [Self::no_interface].
    fn new() -> Self {
        Self { populate: Populate::Nothing, interface: Interface::Nothing }
    }

    /// Populates the database without a schema
    fn populate_nothing(self) -> Self {
        self.populate(Populate::Nothing)
    }

    /// Populates the database with a schema
    fn populate_schema(self) -> Self {
        self.populate(Populate::Schema)
    }

    /// Populates the database with a schema and loads it with data
    fn populate_schema_and_builtin_data(self) -> Self {
        self.populate(Populate::SchemaAndData)
    }

    /// Builds no interface on top of the database (neither pool nor datastore)
    fn no_interface(self) -> Self {
        self.interface(Interface::Nothing)
    }

    /// Builds a pool interface on top of the database
    fn interface_pool(self) -> Self {
        self.interface(Interface::Pool)
    }

    /// Builds a datatore interface on top of the database
    fn interface_datastore(self) -> Self {
        self.interface(Interface::Datastore)
    }

    async fn build(self, log: &Logger) -> TestDatabase {
        match (self.populate, self.interface) {
            (Populate::Nothing, interface) => {
                let db = crdb::test_setup_database_empty(log).await;
                match interface {
                    Interface::Nothing => {
                        TestDatabase { db, kind: TestKind::NoPool }
                    }
                    Interface::Pool => {
                        let pool = new_pool(log, &db);
                        TestDatabase { db, kind: TestKind::Pool { pool } }
                    }
                    Interface::Datastore => {
                        panic!("Cannot create datastore without schema")
                    }
                }
            }
            (Populate::Schema, interface) => {
                let db = crdb::test_setup_database(log).await;
                match interface {
                    Interface::Nothing => {
                        TestDatabase { db, kind: TestKind::NoPool }
                    }
                    Interface::Pool => {
                        let pool = new_pool(log, &db);
                        TestDatabase { db, kind: TestKind::Pool { pool } }
                    }
                    Interface::Datastore => {
                        let pool = new_pool(log, &db);
                        let datastore = Arc::new(
                            DataStore::new(
                                &log,
                                pool,
                                None,
                                IdentityCheckPolicy::DontCare,
                            )
                            .await
                            .unwrap(),
                        );
                        TestDatabase {
                            db,
                            kind: TestKind::RawDatastore { datastore },
                        }
                    }
                }
            }
            (Populate::SchemaAndData, Interface::Datastore) => {
                let db = crdb::test_setup_database(log).await;
                let (opctx, datastore) =
                    datastore_test_on_default_rack(log, &db).await;
                TestDatabase {
                    db,
                    kind: TestKind::Datastore { opctx, datastore },
                }
            }
            (Populate::SchemaAndData, Interface::Nothing)
            | (Populate::SchemaAndData, Interface::Pool) => {
                // This configuration isn't wrong, it's just weird - we need to
                // build a datastore to load the built-in data, so it's odd to
                // discard it immediately afterwards.
                panic!(
                    "If you're fully populating a datastore, you probably want a connection to it"
                );
            }
        }
    }

    fn populate(self, populate: Populate) -> Self {
        Self { populate, ..self }
    }

    fn interface(self, interface: Interface) -> Self {
        Self { interface, ..self }
    }
}

enum TestKind {
    NoPool,
    Pool { pool: Arc<db::Pool> },
    RawDatastore { datastore: Arc<DataStore> },
    Datastore { opctx: OpContext, datastore: Arc<DataStore> },
}

/// A test database, possibly with a pool or full datastore on top
pub struct TestDatabase {
    db: CockroachInstance,
    kind: TestKind,
}

impl TestDatabase {
    /// Creates a new database for test usage, without any schema nor interface
    ///
    /// [`Self::terminate`] should be called before the test finishes.
    pub async fn new_populate_nothing(log: &Logger) -> Self {
        TestDatabaseBuilder::new()
            .populate_nothing()
            .no_interface()
            .build(log)
            .await
    }

    /// Creates a new database for test usage, with a schema but no interface
    ///
    /// [`Self::terminate`] should be called before the test finishes.
    pub async fn new_populate_schema_only(log: &Logger) -> Self {
        TestDatabaseBuilder::new()
            .populate_schema()
            .no_interface()
            .build(log)
            .await
    }

    /// Creates a new database for test usage, with a pool.
    ///
    /// [`Self::terminate`] should be called before the test finishes.
    pub async fn new_with_pool(log: &Logger) -> Self {
        TestDatabaseBuilder::new()
            .populate_schema()
            .interface_pool()
            .build(log)
            .await
    }

    /// Creates a new database for test usage, with a pre-loaded datastore.
    ///
    /// [`Self::terminate`] should be called before the test finishes.
    pub async fn new_with_datastore(log: &Logger) -> Self {
        TestDatabaseBuilder::new()
            .populate_schema_and_builtin_data()
            .interface_datastore()
            .build(log)
            .await
    }

    /// Creates a new database for test usage, with a schema but no builtin data
    ///
    /// [`Self::terminate`] should be called before the test finishes.
    pub async fn new_with_raw_datastore(log: &Logger) -> Self {
        TestDatabaseBuilder::new()
            .populate_schema()
            .interface_datastore()
            .build(log)
            .await
    }

    pub fn crdb(&self) -> &CockroachInstance {
        &self.db
    }

    pub fn pool(&self) -> &Arc<db::Pool> {
        match &self.kind {
            TestKind::Pool { pool } => pool,
            TestKind::NoPool
            | TestKind::RawDatastore { .. }
            | TestKind::Datastore { .. } => {
                panic!(
                    "Wrong test type; try using `TestDatabase::new_with_pool`"
                );
            }
        }
    }

    /// Returns a new independent datastore atop a new pool atop the same
    /// database
    ///
    /// This is normally not necessary.  You can clone the `Arc<DataStore>`
    /// returned by `datastore()`.  However, this is important for tests that
    /// need separate datastores to test their separate quiesce behaviors.
    pub async fn extra_datastore(&self, log: &Logger) -> Arc<DataStore> {
        let pool = new_pool(log, &self.db);
        Arc::new(
            DataStore::new(&log, pool, None, IdentityCheckPolicy::DontCare)
                .await
                .unwrap(),
        )
    }

    pub fn opctx(&self) -> &OpContext {
        match &self.kind {
            TestKind::NoPool
            | TestKind::Pool { .. }
            | TestKind::RawDatastore { .. } => {
                panic!(
                    "Wrong test type; try using `TestDatabase::new_with_datastore`"
                );
            }
            TestKind::Datastore { opctx, .. } => opctx,
        }
    }

    pub fn datastore(&self) -> &Arc<DataStore> {
        match &self.kind {
            TestKind::NoPool | TestKind::Pool { .. } => {
                panic!(
                    "Wrong test type; try using `TestDatabase::new_with_datastore`"
                );
            }
            TestKind::RawDatastore { datastore } => datastore,
            TestKind::Datastore { datastore, .. } => datastore,
        }
    }

    /// Shuts down both the database and the pool
    pub async fn terminate(mut self) {
        match self.kind {
            TestKind::NoPool => (),
            TestKind::Pool { pool } => pool.terminate().await,
            TestKind::RawDatastore { datastore } => datastore.terminate().await,
            TestKind::Datastore { datastore, .. } => {
                datastore.terminate().await
            }
        }
        self.db.cleanup().await.unwrap();
    }
}

pub const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";

async fn datastore_test_on_default_rack(
    log: &Logger,
    db: &CockroachInstance,
) -> (OpContext, Arc<DataStore>) {
    let rack_id = Uuid::parse_str(RACK_UUID).unwrap();
    datastore_test(log, db, rack_id).await
}

// Constructs a DataStore for use in test suites that has preloaded the
// built-in users, roles, and role assignments that are needed for basic
// operation
async fn datastore_test(
    log: &Logger,
    db: &CockroachInstance,
    rack_id: Uuid,
) -> (OpContext, Arc<DataStore>) {
    use crate::authn;

    let cfg = db::Config { url: db.pg_config().clone() };
    let pool = Arc::new(db::Pool::new_single_host(&log, &cfg));
    let datastore = Arc::new(
        DataStore::new(&log, pool, None, IdentityCheckPolicy::DontCare)
            .await
            .unwrap(),
    );

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
