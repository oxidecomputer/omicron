// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Primary control plane interface for database read and write operations

// TODO-scalability review all queries for use of indexes (may need
// "time_deleted IS NOT NULL" conditions) Figure out how to automate this.
//
// TODO-design Better support for joins?
// The interfaces here often require that to do anything with an object, a
// caller must first look up the id and then do operations with the id.  For
// example, the caller of project_list_disks() always looks up the project to
// get the project_id, then lists disks having that project_id.  It's possible
// to implement this instead with a JOIN in the database so that we do it with
// one database round-trip.  We could use CTEs similar to what we do with
// conditional updates to distinguish the case where the project didn't exist
// vs. there were no disks in it.  This seems likely to be a fair bit more
// complicated to do safely and generally compared to what we have now.

use super::pool::DbConnection;
use super::Pool;
use crate::authz;
use crate::context::OpContext;
use crate::db::{
    self,
    error::{public_error_from_diesel, ErrorHandler},
};
use ::oximeter::types::ProducerRegistry;
use async_bb8_diesel::{AsyncRunQueryDsl, ConnectionManager};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::{QueryFragment, QueryId};
use diesel::query_dsl::methods::LoadQuery;
use diesel::{ExpressionMethods, QueryDsl};
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::SemverVersion;
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service, BackoffError,
};
use omicron_common::nexus_config::SchemaConfig;
use slog::Logger;
use std::net::Ipv6Addr;
use std::sync::Arc;
use uuid::Uuid;

mod address_lot;
mod certificate;
mod console_session;
mod dataset;
mod db_metadata;
mod device_auth;
mod disk;
mod dns;
mod external_ip;
mod identity_provider;
mod image;
mod instance;
mod inventory;
mod ip_pool;
mod network_interface;
mod oximeter;
mod physical_disk;
mod project;
mod rack;
mod region;
mod region_snapshot;
mod role;
mod saga;
mod service;
mod silo;
mod silo_group;
mod silo_user;
mod sled;
mod sled_instance;
mod snapshot;
mod ssh_key;
mod switch;
mod switch_interface;
mod switch_port;
mod update;
mod virtual_provisioning_collection;
mod vmm;
mod volume;
mod vpc;
mod zpool;

pub use address_lot::AddressLotCreateResult;
pub use db_metadata::{
    all_sql_for_version_migration, EARLIEST_SUPPORTED_VERSION,
};
pub use dns::DnsVersionUpdateBuilder;
pub use instance::InstanceAndActiveVmm;
pub use inventory::DataStoreInventoryTest;
pub use rack::RackInit;
pub use silo::Discoverability;
pub use switch_port::SwitchPortSettingsCombinedResult;
pub use virtual_provisioning_collection::StorageType;
pub use volume::CrucibleResources;
pub use volume::CrucibleTargets;

// Number of unique datasets required to back a region.
// TODO: This should likely turn into a configuration option.
pub(crate) const REGION_REDUNDANCY_THRESHOLD: usize = 3;

/// The name of the built-in IP pool for Oxide services.
pub const SERVICE_IP_POOL_NAME: &str = "oxide-service-pool";

/// The name of the built-in Project and VPC for Oxide services.
pub const SERVICES_DB_NAME: &str = "oxide-services";

// Represents a query that is ready to be executed.
//
// This helper trait lets the statement either be executed or explained.
//
// U: The output type of executing the statement.
pub trait RunnableQueryNoReturn:
    RunQueryDsl<DbConnection> + QueryFragment<Pg> + QueryId
{
}

impl<T> RunnableQueryNoReturn for T where
    T: RunQueryDsl<DbConnection> + QueryFragment<Pg> + QueryId
{
}

pub trait RunnableQuery<U>:
    RunnableQueryNoReturn + LoadQuery<'static, DbConnection, U>
{
}

impl<U, T> RunnableQuery<U> for T where
    T: RunnableQueryNoReturn + LoadQuery<'static, DbConnection, U>
{
}

pub type DataStoreConnection<'a> =
    bb8::PooledConnection<'a, ConnectionManager<DbConnection>>;

pub struct DataStore {
    pool: Arc<Pool>,
    virtual_provisioning_collection_producer: crate::provisioning::Producer,
}

// The majority of `DataStore`'s methods live in our submodules as a concession
// to compilation times; changing a query only requires incremental
// recompilation of that query's module instead of all queries on `DataStore`.
impl DataStore {
    /// Constructs a new Datastore object, without any version validation.
    ///
    /// Ignores the underlying DB version. Should be used with caution, as usage
    /// of this method can construct a Datastore which does not understand
    /// the underlying CockroachDB schema. Data corruption could result.
    pub fn new_unchecked(pool: Arc<Pool>) -> Result<Self, String> {
        let datastore = DataStore {
            pool,
            virtual_provisioning_collection_producer:
                crate::provisioning::Producer::new(),
        };
        Ok(datastore)
    }

    /// Constructs a new Datastore object.
    ///
    /// Only returns if the database schema is compatible with Nexus's known
    /// schema version.
    pub async fn new(
        log: &Logger,
        pool: Arc<Pool>,
        config: Option<&SchemaConfig>,
    ) -> Result<Self, String> {
        let datastore = Self::new_unchecked(pool)?;

        // Keep looping until we find that the schema matches our expectation.
        const EXPECTED_VERSION: SemverVersion =
            nexus_db_model::schema::SCHEMA_VERSION;
        retry_notify(
            retry_policy_internal_service(),
            || async {
                match datastore
                    .ensure_schema(&log, EXPECTED_VERSION, config)
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        warn!(log, "Failed to ensure schema version: {e}");
                    }
                };
                return Err(BackoffError::transient(()));
            },
            |_, _| {},
        )
        .await
        .map_err(|_| "Failed to read valid DB schema".to_string())?;

        Ok(datastore)
    }

    pub fn register_producers(&self, registry: &ProducerRegistry) {
        registry
            .register_producer(
                self.virtual_provisioning_collection_producer.clone(),
            )
            .unwrap();
    }

    /// Returns a connection to a connection from the database connection pool.
    pub(super) async fn pool_connection_authorized(
        &self,
        opctx: &OpContext,
    ) -> Result<DataStoreConnection, Error> {
        opctx.authorize(authz::Action::Query, &authz::DATABASE).await?;
        let pool = self.pool.pool();
        let connection = pool.get().await.map_err(|err| {
            Error::unavail(&format!("Failed to access DB connection: {err}"))
        })?;
        Ok(connection)
    }

    /// Returns an unauthorized connection to a connection from the database
    /// connection pool.
    ///
    /// TODO-security: This should be deprecated in favor of
    /// "pool_connection_authorized".
    pub(super) async fn pool_connection_unauthorized(
        &self,
    ) -> Result<DataStoreConnection, Error> {
        let connection = self.pool.pool().get().await.map_err(|err| {
            Error::unavail(&format!("Failed to access DB connection: {err}"))
        })?;
        Ok(connection)
    }

    /// For testing only. This isn't cfg(test) because nexus needs access to it.
    #[doc(hidden)]
    pub async fn pool_connection_for_tests(
        &self,
    ) -> Result<DataStoreConnection, Error> {
        self.pool_connection_unauthorized().await
    }

    /// Return the next available IPv6 address for an Oxide service running on
    /// the provided sled.
    pub async fn next_ipv6_address(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
    ) -> Result<Ipv6Addr, Error> {
        use db::schema::sled::dsl;
        let net = diesel::update(
            dsl::sled.find(sled_id).filter(dsl::time_deleted.is_null()),
        )
        .set(dsl::last_used_address.eq(dsl::last_used_address + 1))
        .returning(dsl::last_used_address)
        .get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| {
            public_error_from_diesel(
                e,
                ErrorHandler::NotFoundByLookup(
                    ResourceType::Sled,
                    LookupType::ById(sled_id),
                ),
            )
        })?;

        // TODO-correctness: We need to ensure that this address is actually
        // within the sled's underlay prefix, once that's included in the
        // database record.
        match net {
            ipnetwork::IpNetwork::V6(net) => Ok(net.ip()),
            _ => Err(Error::InternalError {
                internal_message: String::from("Sled IP address must be IPv6"),
            }),
        }
    }

    // Test interfaces

    #[cfg(test)]
    async fn test_try_table_scan(&self, opctx: &OpContext) -> Error {
        use db::schema::project::dsl;
        let conn = self.pool_connection_authorized(opctx).await;
        if let Err(error) = conn {
            return error;
        }
        let result = dsl::project
            .select(diesel::dsl::count_star())
            .first_async::<i64>(&*conn.unwrap())
            .await;
        match result {
            Ok(_) => Error::internal_error("table scan unexpectedly succeeded"),
            Err(error) => public_error_from_diesel(error, ErrorHandler::Server),
        }
    }
}

pub enum UpdatePrecondition<T> {
    DontCare,
    Null,
    Value(T),
}

/// Constructs a DataStore for use in test suites that has preloaded the
/// built-in users, roles, and role assignments that are needed for basic
/// operation
#[cfg(test)]
pub async fn datastore_test(
    logctx: &dropshot::test_util::LogContext,
    db: &omicron_test_utils::dev::db::CockroachInstance,
) -> (OpContext, Arc<DataStore>) {
    use crate::authn;

    let cfg = db::Config { url: db.pg_config().clone() };
    let pool = Arc::new(db::Pool::new(&logctx.log, &cfg));
    let datastore =
        Arc::new(DataStore::new(&logctx.log, pool, None).await.unwrap());

    // Create an OpContext with the credentials of "db-init" just for the
    // purpose of loading the built-in users, roles, and assignments.
    let opctx = OpContext::for_background(
        logctx.log.new(o!()),
        Arc::new(authz::Authz::new(&logctx.log)),
        authn::Context::internal_db_init(),
        Arc::clone(&datastore),
    );

    // TODO: Can we just call "Populate" instead of doing this?
    let rack_id = Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap();
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
    let opctx =
        OpContext::for_tests(logctx.log.new(o!()), Arc::clone(&datastore));

    (opctx, datastore)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::authn;
    use crate::authn::SiloAuthnPolicy;
    use crate::authz;
    use crate::db::explain::ExplainableAsync;
    use crate::db::fixed_data::silo::DEFAULT_SILO;
    use crate::db::fixed_data::silo::SILO_ID;
    use crate::db::identity::Asset;
    use crate::db::lookup::LookupPath;
    use crate::db::model::{
        BlockSize, ComponentUpdate, ComponentUpdateIdentity, ConsoleSession,
        Dataset, DatasetKind, ExternalIp, PhysicalDisk, PhysicalDiskKind,
        Project, Rack, Region, Service, ServiceKind, SiloUser, Sled,
        SledBaseboard, SledSystemHardware, SshKey, SystemUpdate,
        UpdateableComponentType, VpcSubnet, Zpool,
    };
    use crate::db::queries::vpc_subnet::FilterConflictingVpcSubnetRangesQuery;
    use assert_matches::assert_matches;
    use chrono::{Duration, Utc};
    use futures::stream;
    use futures::StreamExt;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::external_api::params;
    use omicron_common::api::external::DataPageParams;
    use omicron_common::api::external::{
        self, ByteCount, Error, IdentityMetadataCreateParams, LookupType, Name,
    };
    use omicron_common::nexus_config::RegionAllocationStrategy;
    use omicron_test_utils::dev;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use uuid::Uuid;

    // Creates a "fake" Sled Baseboard.
    pub fn sled_baseboard_for_test() -> SledBaseboard {
        SledBaseboard {
            serial_number: Uuid::new_v4().to_string(),
            part_number: String::from("test-part"),
            revision: 1,
        }
    }

    // Creates "fake" sled hardware accounting
    pub fn sled_system_hardware_for_test() -> SledSystemHardware {
        SledSystemHardware {
            is_scrimlet: false,
            usable_hardware_threads: 4,
            usable_physical_ram: crate::db::model::ByteCount::try_from(1 << 40)
                .unwrap(),
            reservoir_size: crate::db::model::ByteCount::try_from(1 << 39)
                .unwrap(),
        }
    }

    #[tokio::test]
    async fn test_project_creation() {
        let logctx = dev::test_setup_log("test_project_creation");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let authz_silo = opctx.authn.silo_required().unwrap();

        let (.., silo) = LookupPath::new(&opctx, &datastore)
            .silo_id(authz_silo.id())
            .fetch()
            .await
            .unwrap();

        let project = Project::new(
            authz_silo.id(),
            params::ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: "project".parse().unwrap(),
                    description: "desc".to_string(),
                },
            },
        );
        datastore.project_create(&opctx, project).await.unwrap();

        let (.., silo_after_project_create) =
            LookupPath::new(&opctx, &datastore)
                .silo_id(authz_silo.id())
                .fetch()
                .await
                .unwrap();
        assert!(silo_after_project_create.rcgen > silo.rcgen);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_session_methods() {
        let logctx = dev::test_setup_log("test_session_methods");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authn_opctx = OpContext::for_background(
            logctx.log.new(o!("component" => "TestExternalAuthn")),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::external_authn(),
            Arc::clone(&datastore),
        );

        let token = "a_token".to_string();
        let silo_user_id = Uuid::new_v4();

        let session = ConsoleSession {
            token: token.clone(),
            time_created: Utc::now() - Duration::minutes(5),
            time_last_used: Utc::now() - Duration::minutes(5),
            silo_user_id,
        };

        let _ = datastore
            .session_create(&authn_opctx, session.clone())
            .await
            .unwrap();

        // Associate silo with user
        let authz_silo = authz::Silo::new(
            authz::FLEET,
            *SILO_ID,
            LookupType::ById(*SILO_ID),
        );
        datastore
            .silo_user_create(
                &authz_silo,
                SiloUser::new(
                    authz_silo.id(),
                    silo_user_id,
                    "external_id".into(),
                ),
            )
            .await
            .unwrap();

        let (.., db_silo_user) = LookupPath::new(&opctx, &datastore)
            .silo_user_id(session.silo_user_id)
            .fetch()
            .await
            .unwrap();
        assert_eq!(*SILO_ID, db_silo_user.silo_id);

        // fetch the one we just created
        let (.., fetched) = LookupPath::new(&opctx, &datastore)
            .console_session_token(&token)
            .fetch()
            .await
            .unwrap();
        assert_eq!(session.silo_user_id, fetched.silo_user_id);

        // trying to insert the same one again fails
        let duplicate =
            datastore.session_create(&authn_opctx, session.clone()).await;
        assert!(matches!(
            duplicate,
            Err(Error::InternalError { internal_message: _ })
        ));

        // update last used (i.e., renew token)
        let authz_session = authz::ConsoleSession::new(
            authz::FLEET,
            token.clone(),
            LookupType::ByCompositeId(token.clone()),
        );
        let renewed = datastore
            .session_update_last_used(&opctx, &authz_session)
            .await
            .unwrap();
        assert!(
            renewed.console_session.time_last_used > session.time_last_used
        );

        // time_last_used change persists in DB
        let (.., fetched) = LookupPath::new(&opctx, &datastore)
            .console_session_token(&token)
            .fetch()
            .await
            .unwrap();
        assert!(fetched.time_last_used > session.time_last_used);

        // deleting it using `opctx` (which represents the test-privileged user)
        // should succeed but not do anything -- you can't delete someone else's
        // session
        let delete =
            datastore.session_hard_delete(&opctx, &authz_session).await;
        assert_eq!(delete, Ok(()));
        let fetched = LookupPath::new(&opctx, &datastore)
            .console_session_token(&token)
            .fetch()
            .await;
        assert!(fetched.is_ok());

        // delete it and fetch should come back with nothing
        let silo_user_opctx = OpContext::for_background(
            logctx.log.new(o!()),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::for_test_user(
                silo_user_id,
                *SILO_ID,
                SiloAuthnPolicy::try_from(&*DEFAULT_SILO).unwrap(),
            ),
            Arc::clone(&datastore),
        );
        let delete = datastore
            .session_hard_delete(&silo_user_opctx, &authz_session)
            .await;
        assert_eq!(delete, Ok(()));
        let fetched = LookupPath::new(&opctx, &datastore)
            .console_session_token(&token)
            .fetch()
            .await;
        assert!(matches!(
            fetched,
            Err(Error::ObjectNotFound { type_name: _, lookup_type: _ })
        ));

        // deleting an already nonexistent is considered a success
        let delete_again =
            datastore.session_hard_delete(&opctx, &authz_session).await;
        assert_eq!(delete_again, Ok(()));

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Creates a test sled, returns its UUID.
    async fn create_test_sled(datastore: &DataStore) -> Uuid {
        let bogus_addr = SocketAddrV6::new(
            Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1),
            8080,
            0,
            0,
        );
        let rack_id = Uuid::new_v4();
        let sled_id = Uuid::new_v4();

        let sled = Sled::new(
            sled_id,
            bogus_addr,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id,
        );
        datastore.sled_upsert(sled).await.unwrap();
        sled_id
    }

    fn test_zpool_size() -> ByteCount {
        ByteCount::from_gibibytes_u32(100)
    }

    const TEST_VENDOR: &str = "test-vendor";
    const TEST_SERIAL: &str = "test-serial";
    const TEST_MODEL: &str = "test-model";

    async fn create_test_physical_disk(
        datastore: &DataStore,
        opctx: &OpContext,
        sled_id: Uuid,
        kind: PhysicalDiskKind,
    ) -> Uuid {
        let physical_disk = PhysicalDisk::new(
            TEST_VENDOR.into(),
            TEST_SERIAL.into(),
            TEST_MODEL.into(),
            kind,
            sled_id,
        );
        datastore
            .physical_disk_upsert(opctx, physical_disk.clone())
            .await
            .expect("Failed to upsert physical disk");
        physical_disk.uuid()
    }

    // Creates a test zpool, returns its UUID.
    async fn create_test_zpool(
        datastore: &DataStore,
        sled_id: Uuid,
        physical_disk_id: Uuid,
    ) -> Uuid {
        let zpool_id = Uuid::new_v4();
        let zpool = Zpool::new(
            zpool_id,
            sled_id,
            physical_disk_id,
            test_zpool_size().into(),
        );
        datastore.zpool_upsert(zpool).await.unwrap();
        zpool_id
    }

    fn create_test_disk_create_params(
        name: &str,
        size: ByteCount,
    ) -> params::DiskCreate {
        params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from(name.to_string()).unwrap(),
                description: name.to_string(),
            },
            disk_source: params::DiskSource::Blank {
                block_size: params::BlockSize::try_from(4096).unwrap(),
            },
            size,
        }
    }

    struct TestDataset {
        sled_id: Uuid,
        dataset_id: Uuid,
    }

    async fn create_test_datasets_for_region_allocation(
        opctx: &OpContext,
        datastore: Arc<DataStore>,
        number_of_sleds: usize,
    ) -> Vec<TestDataset> {
        // Create sleds...
        let sled_ids: Vec<Uuid> = stream::iter(0..number_of_sleds)
            .then(|_| create_test_sled(&datastore))
            .collect()
            .await;

        struct PhysicalDisk {
            sled_id: Uuid,
            disk_id: Uuid,
        }

        // create 9 disks on each sled
        let physical_disks: Vec<PhysicalDisk> = stream::iter(sled_ids)
            .map(|sled_id| {
                let sled_id_iter: Vec<Uuid> = (0..9).map(|_| sled_id).collect();
                stream::iter(sled_id_iter).then(|sled_id| {
                    let disk_id_future = create_test_physical_disk(
                        &datastore,
                        opctx,
                        sled_id,
                        PhysicalDiskKind::U2,
                    );
                    async move {
                        let disk_id = disk_id_future.await;
                        PhysicalDisk { sled_id, disk_id }
                    }
                })
            })
            .flatten()
            .collect()
            .await;

        #[derive(Copy, Clone)]
        struct Zpool {
            sled_id: Uuid,
            pool_id: Uuid,
        }

        // 1 pool per disk
        let zpools: Vec<Zpool> = stream::iter(physical_disks)
            .then(|disk| {
                let pool_id_future =
                    create_test_zpool(&datastore, disk.sled_id, disk.disk_id);
                async move {
                    let pool_id = pool_id_future.await;
                    Zpool { sled_id: disk.sled_id, pool_id }
                }
            })
            .collect()
            .await;

        let bogus_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);

        let datasets: Vec<TestDataset> = stream::iter(zpools)
            .map(|zpool| {
                // 3 datasets per zpool, to test that pools are distinct
                let zpool_iter: Vec<Zpool> = (0..3).map(|_| zpool).collect();
                stream::iter(zpool_iter).then(|zpool| {
                    let id = Uuid::new_v4();
                    let dataset = Dataset::new(
                        id,
                        zpool.pool_id,
                        bogus_addr,
                        DatasetKind::Crucible,
                    );

                    let datastore = datastore.clone();
                    async move {
                        datastore.dataset_upsert(dataset).await.unwrap();

                        TestDataset { sled_id: zpool.sled_id, dataset_id: id }
                    }
                })
            })
            .flatten()
            .collect()
            .await;

        datasets
    }

    #[tokio::test]
    /// Note that this test is currently non-deterministic. It can be made
    /// deterministic by generating deterministic *dataset* Uuids. The sled and
    /// pool IDs should not matter.
    async fn test_region_allocation_strat_random() {
        let logctx = dev::test_setup_log("test_region_allocation_strat_random");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        create_test_datasets_for_region_allocation(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        // Allocate regions from the datasets for this disk. Do it a few times
        // for good measure.
        for alloc_seed in 0..10 {
            let params = create_test_disk_create_params(
                &format!("disk{}", alloc_seed),
                ByteCount::from_mebibytes_u32(1),
            );
            let volume_id = Uuid::new_v4();

            let expected_region_count = REGION_REDUNDANCY_THRESHOLD;
            let dataset_and_regions = datastore
                .region_allocate(
                    &opctx,
                    volume_id,
                    &params.disk_source,
                    params.size,
                    &RegionAllocationStrategy::Random {
                        seed: Some(alloc_seed),
                    },
                )
                .await
                .unwrap();

            // Verify the allocation.
            assert_eq!(expected_region_count, dataset_and_regions.len());
            let mut disk_datasets = HashSet::new();
            let mut disk_zpools = HashSet::new();

            for (dataset, region) in dataset_and_regions {
                // Must be 3 unique datasets
                assert!(disk_datasets.insert(dataset.id()));

                // Must be 3 unique zpools
                assert!(disk_zpools.insert(dataset.pool_id));

                assert_eq!(volume_id, region.volume_id());
                assert_eq!(ByteCount::from(4096), region.block_size());
                let (_, extent_count) = DataStore::get_crucible_allocation(
                    &BlockSize::AdvancedFormat,
                    params.size,
                );
                assert_eq!(extent_count, region.extent_count());
            }
        }

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    /// Test the [`RegionAllocationStrategy::RandomWithDistinctSleds`] strategy.
    /// It should always pick datasets where no two datasets are on the same
    /// zpool and no two zpools are on the same sled.
    async fn test_region_allocation_strat_random_with_distinct_sleds() {
        let logctx = dev::test_setup_log(
            "test_region_allocation_strat_random_with_distinct_sleds",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a rack without enough sleds for a successful allocation when
        // we require 3 distinct sleds.
        let test_datasets = create_test_datasets_for_region_allocation(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        // We need to check that our datasets end up on 3 distinct sleds, but the query doesn't return the sled ID, so we need to reverse map from dataset ID to sled ID
        let sled_id_map: HashMap<Uuid, Uuid> = test_datasets
            .into_iter()
            .map(|test_dataset| (test_dataset.dataset_id, test_dataset.sled_id))
            .collect();

        // Allocate regions from the datasets for this disk. Do it a few times
        // for good measure.
        for alloc_seed in 0..10 {
            let params = create_test_disk_create_params(
                &format!("disk{}", alloc_seed),
                ByteCount::from_mebibytes_u32(1),
            );
            let volume_id = Uuid::new_v4();

            let expected_region_count = REGION_REDUNDANCY_THRESHOLD;
            let dataset_and_regions = datastore
                .region_allocate(
                    &opctx,
                    volume_id,
                    &params.disk_source,
                    params.size,
                    &&RegionAllocationStrategy::RandomWithDistinctSleds {
                        seed: Some(alloc_seed),
                    },
                )
                .await
                .unwrap();

            // Verify the allocation.
            assert_eq!(expected_region_count, dataset_and_regions.len());
            let mut disk_datasets = HashSet::new();
            let mut disk_zpools = HashSet::new();
            let mut disk_sleds = HashSet::new();
            for (dataset, region) in dataset_and_regions {
                // Must be 3 unique datasets
                assert!(disk_datasets.insert(dataset.id()));

                // Must be 3 unique zpools
                assert!(disk_zpools.insert(dataset.pool_id));

                // Must be 3 unique sleds
                let sled_id = sled_id_map.get(&dataset.id()).unwrap();
                assert!(disk_sleds.insert(*sled_id));

                assert_eq!(volume_id, region.volume_id());
                assert_eq!(ByteCount::from(4096), region.block_size());
                let (_, extent_count) = DataStore::get_crucible_allocation(
                    &BlockSize::AdvancedFormat,
                    params.size,
                );
                assert_eq!(extent_count, region.extent_count());
            }
        }

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    /// Ensure the [`RegionAllocationStrategy::RandomWithDistinctSleds`]
    /// strategy fails when there aren't enough distinct sleds.
    async fn test_region_allocation_strat_random_with_distinct_sleds_fails() {
        let logctx = dev::test_setup_log(
            "test_region_allocation_strat_random_with_distinct_sleds_fails",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a rack without enough sleds for a successful allocation when
        // we require 3 distinct sleds.
        create_test_datasets_for_region_allocation(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD - 1,
        )
        .await;

        // Allocate regions from the datasets for this disk. Do it a few times
        // for good measure.
        for alloc_seed in 0..10 {
            let params = create_test_disk_create_params(
                &format!("disk{}", alloc_seed),
                ByteCount::from_mebibytes_u32(1),
            );
            let volume_id = Uuid::new_v4();

            let err = datastore
                .region_allocate(
                    &opctx,
                    volume_id,
                    &params.disk_source,
                    params.size,
                    &&RegionAllocationStrategy::RandomWithDistinctSleds {
                        seed: Some(alloc_seed),
                    },
                )
                .await
                .unwrap_err();

            let expected = "Not enough zpool space to allocate disks";
            assert!(
                err.to_string().contains(expected),
                "Saw error: \'{err}\', but expected \'{expected}\'"
            );

            assert!(matches!(err, Error::ServiceUnavailable { .. }));
        }

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_is_idempotent() {
        let logctx =
            dev::test_setup_log("test_region_allocation_is_idempotent");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        create_test_datasets_for_region_allocation(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        // Allocate regions from the datasets for this volume.
        let params = create_test_disk_create_params(
            "disk",
            ByteCount::from_mebibytes_u32(500),
        );
        let volume_id = Uuid::new_v4();
        let mut dataset_and_regions1 = datastore
            .region_allocate(
                &opctx,
                volume_id,
                &params.disk_source,
                params.size,
                &RegionAllocationStrategy::Random { seed: Some(0) },
            )
            .await
            .unwrap();

        // Use a different allocation ordering to ensure we're idempotent even
        // if the shuffle changes.
        let mut dataset_and_regions2 = datastore
            .region_allocate(
                &opctx,
                volume_id,
                &params.disk_source,
                params.size,
                &RegionAllocationStrategy::Random { seed: Some(1) },
            )
            .await
            .unwrap();

        // Give them a consistent order so we can easily compare them.
        let sort_vec = |v: &mut Vec<(Dataset, Region)>| {
            v.sort_by(|(d1, r1), (d2, r2)| {
                let order = d1.id().cmp(&d2.id());
                match order {
                    std::cmp::Ordering::Equal => r1.id().cmp(&r2.id()),
                    _ => order,
                }
            });
        };
        sort_vec(&mut dataset_and_regions1);
        sort_vec(&mut dataset_and_regions2);

        // Validate that the two calls to allocate return the same data.
        assert_eq!(dataset_and_regions1.len(), dataset_and_regions2.len());
        for i in 0..dataset_and_regions1.len() {
            assert_eq!(dataset_and_regions1[i], dataset_and_regions2[i],);
        }

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_not_enough_zpools() {
        let logctx =
            dev::test_setup_log("test_region_allocation_not_enough_zpools");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a sled...
        let sled_id = create_test_sled(&datastore).await;

        // ... and a disk on that sled...
        let physical_disk_id = create_test_physical_disk(
            &datastore,
            &opctx,
            sled_id,
            PhysicalDiskKind::U2,
        )
        .await;

        // 1 less than REDUNDANCY level of zpools
        let zpool_ids: Vec<Uuid> =
            stream::iter(0..REGION_REDUNDANCY_THRESHOLD - 1)
                .then(|_| {
                    create_test_zpool(&datastore, sled_id, physical_disk_id)
                })
                .collect()
                .await;

        let bogus_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);

        // 1 dataset per zpool
        stream::iter(zpool_ids)
            .then(|zpool_id| {
                let id = Uuid::new_v4();
                let dataset = Dataset::new(
                    id,
                    zpool_id,
                    bogus_addr,
                    DatasetKind::Crucible,
                );
                let datastore = datastore.clone();
                async move {
                    datastore.dataset_upsert(dataset).await.unwrap();
                    id
                }
            })
            .collect::<Vec<_>>()
            .await;

        // Allocate regions from the datasets for this volume.
        let params = create_test_disk_create_params(
            "disk1",
            ByteCount::from_mebibytes_u32(500),
        );
        let volume1_id = Uuid::new_v4();
        let err = datastore
            .region_allocate(
                &opctx,
                volume1_id,
                &params.disk_source,
                params.size,
                &RegionAllocationStrategy::Random { seed: Some(0) },
            )
            .await
            .unwrap_err();

        let expected = "Not enough zpool space to allocate disks";
        assert!(
            err.to_string().contains(expected),
            "Saw error: \'{err}\', but expected \'{expected}\'"
        );

        assert!(matches!(err, Error::ServiceUnavailable { .. }));

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_out_of_space_fails() {
        let logctx =
            dev::test_setup_log("test_region_allocation_out_of_space_fails");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        create_test_datasets_for_region_allocation(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let disk_size = test_zpool_size();
        let alloc_size = ByteCount::try_from(disk_size.to_bytes() * 2).unwrap();
        let params = create_test_disk_create_params("disk1", alloc_size);
        let volume1_id = Uuid::new_v4();

        assert!(datastore
            .region_allocate(
                &opctx,
                volume1_id,
                &params.disk_source,
                params.size,
                &RegionAllocationStrategy::Random { seed: Some(0) },
            )
            .await
            .is_err());

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    // Validate that queries which should be executable without a full table
    // scan are, in fact, runnable without a FULL SCAN.
    #[tokio::test]
    async fn test_queries_do_not_require_full_table_scan() {
        use omicron_common::api::external;
        let logctx =
            dev::test_setup_log("test_queries_do_not_require_full_table_scan");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&logctx.log, &cfg);
        let datastore =
            DataStore::new(&logctx.log, Arc::new(pool), None).await.unwrap();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let explanation = DataStore::get_allocated_regions_query(Uuid::nil())
            .explain_async(&conn)
            .await
            .unwrap();
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        let subnet = db::model::VpcSubnet::new(
            Uuid::nil(),
            Uuid::nil(),
            external::IdentityMetadataCreateParams {
                name: external::Name::try_from(String::from("name")).unwrap(),
                description: String::from("description"),
            },
            external::Ipv4Net("172.30.0.0/22".parse().unwrap()),
            external::Ipv6Net("fd00::/64".parse().unwrap()),
        );
        let values = FilterConflictingVpcSubnetRangesQuery::new(subnet);
        let query =
            diesel::insert_into(db::schema::vpc_subnet::dsl::vpc_subnet)
                .values(values)
                .returning(VpcSubnet::as_returning());
        println!("{}", diesel::debug_query(&query));
        let explanation = query.explain_async(&conn).await.unwrap();
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation,
        );

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    // Test sled-specific IPv6 address allocation
    #[tokio::test]
    async fn test_sled_ipv6_address_allocation() {
        use omicron_common::address::RSS_RESERVED_ADDRESSES as STATIC_IPV6_ADDRESS_OFFSET;
        use std::net::Ipv6Addr;

        let logctx = dev::test_setup_log("test_sled_ipv6_address_allocation");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = Arc::new(db::Pool::new(&logctx.log, &cfg));
        let datastore =
            Arc::new(DataStore::new(&logctx.log, pool, None).await.unwrap());
        let opctx =
            OpContext::for_tests(logctx.log.new(o!()), datastore.clone());

        let rack_id = Uuid::new_v4();
        let addr1 = "[fd00:1de::1]:12345".parse().unwrap();
        let sled1_id = "0de4b299-e0b4-46f0-d528-85de81a7095f".parse().unwrap();
        let sled1 = db::model::Sled::new(
            sled1_id,
            addr1,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id,
        );
        datastore.sled_upsert(sled1).await.unwrap();

        let addr2 = "[fd00:1df::1]:12345".parse().unwrap();
        let sled2_id = "66285c18-0c79-43e0-e54f-95271f271314".parse().unwrap();
        let sled2 = db::model::Sled::new(
            sled2_id,
            addr2,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id,
        );
        datastore.sled_upsert(sled2).await.unwrap();

        let ip = datastore.next_ipv6_address(&opctx, sled1_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(
            0xfd00,
            0x1de,
            0,
            0,
            0,
            0,
            0,
            2 + STATIC_IPV6_ADDRESS_OFFSET,
        );
        assert_eq!(ip, expected_ip);
        let ip = datastore.next_ipv6_address(&opctx, sled1_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(
            0xfd00,
            0x1de,
            0,
            0,
            0,
            0,
            0,
            3 + STATIC_IPV6_ADDRESS_OFFSET,
        );
        assert_eq!(ip, expected_ip);

        let ip = datastore.next_ipv6_address(&opctx, sled2_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(
            0xfd00,
            0x1df,
            0,
            0,
            0,
            0,
            0,
            2 + STATIC_IPV6_ADDRESS_OFFSET,
        );
        assert_eq!(ip, expected_ip);

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ssh_keys() {
        let logctx = dev::test_setup_log("test_ssh_keys");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a new Silo user so that we can lookup their keys.
        let authz_silo = authz::Silo::new(
            authz::FLEET,
            *SILO_ID,
            LookupType::ById(*SILO_ID),
        );
        let silo_user_id = Uuid::new_v4();
        datastore
            .silo_user_create(
                &authz_silo,
                SiloUser::new(
                    authz_silo.id(),
                    silo_user_id,
                    "external@id".into(),
                ),
            )
            .await
            .unwrap();

        let (.., authz_user) = LookupPath::new(&opctx, &datastore)
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::CreateChild)
            .await
            .unwrap();
        assert_eq!(authz_user.id(), silo_user_id);

        // Create a new SSH public key for the new user.
        let key_name = Name::try_from(String::from("sshkey")).unwrap();
        let public_key = "ssh-test AAAAAAAAKEY".to_string();
        let ssh_key = SshKey::new(
            silo_user_id,
            params::SshKeyCreate {
                identity: IdentityMetadataCreateParams {
                    name: key_name.clone(),
                    description: "my SSH public key".to_string(),
                },
                public_key,
            },
        );
        let created = datastore
            .ssh_key_create(&opctx, &authz_user, ssh_key.clone())
            .await
            .unwrap();
        assert_eq!(created.silo_user_id, ssh_key.silo_user_id);
        assert_eq!(created.public_key, ssh_key.public_key);

        // Lookup the key we just created.
        let (authz_silo, authz_silo_user, authz_ssh_key, found) =
            LookupPath::new(&opctx, &datastore)
                .silo_user_id(silo_user_id)
                .ssh_key_name(&key_name.into())
                .fetch()
                .await
                .unwrap();
        assert_eq!(authz_silo.id(), *SILO_ID);
        assert_eq!(authz_silo_user.id(), silo_user_id);
        assert_eq!(found.silo_user_id, ssh_key.silo_user_id);
        assert_eq!(found.public_key, ssh_key.public_key);

        // Trying to insert the same one again fails.
        let duplicate = datastore
            .ssh_key_create(&opctx, &authz_user, ssh_key.clone())
            .await;
        assert!(matches!(
            duplicate,
            Err(Error::ObjectAlreadyExists { type_name, object_name })
                if type_name == ResourceType::SshKey
                    && object_name == "sshkey"
        ));

        // Delete the key we just created.
        datastore.ssh_key_delete(&opctx, &authz_ssh_key).await.unwrap();

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_service_upsert_and_list() {
        let logctx = dev::test_setup_log("test_service_upsert_and_list");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a sled on which the service should exist.
        let sled_id = create_test_sled(&datastore).await;

        // Create a few new service to exist on this sled.
        let service1_id =
            "ab7bd7fd-7c37-48ab-a84a-9c09a90c4c7f".parse().unwrap();
        let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 123, 0, 0);
        let kind = ServiceKind::Nexus;

        let service1 =
            Service::new(service1_id, sled_id, Some(service1_id), addr, kind);
        let result =
            datastore.service_upsert(&opctx, service1.clone()).await.unwrap();
        assert_eq!(service1.id(), result.id());
        assert_eq!(service1.ip, result.ip);
        assert_eq!(service1.kind, result.kind);

        let service2_id =
            "fe5b6e3d-dfee-47b4-8719-c54f78912c0b".parse().unwrap();
        let service2 = Service::new(service2_id, sled_id, None, addr, kind);
        let result =
            datastore.service_upsert(&opctx, service2.clone()).await.unwrap();
        assert_eq!(service2.id(), result.id());
        assert_eq!(service2.ip, result.ip);
        assert_eq!(service2.kind, result.kind);

        let service3_id = Uuid::new_v4();
        let kind = ServiceKind::Oximeter;
        let service3 = Service::new(
            service3_id,
            sled_id,
            Some(Uuid::new_v4()),
            addr,
            kind,
        );
        let result =
            datastore.service_upsert(&opctx, service3.clone()).await.unwrap();
        assert_eq!(service3.id(), result.id());
        assert_eq!(service3.ip, result.ip);
        assert_eq!(service3.kind, result.kind);

        // Try listing services of one kind.
        let services = datastore
            .services_list_kind(
                &opctx,
                ServiceKind::Nexus,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: NonZeroU32::new(3).unwrap(),
                },
            )
            .await
            .unwrap();
        assert_eq!(services[0].id(), service1.id());
        assert_eq!(services[0].sled_id, service1.sled_id);
        assert_eq!(services[0].zone_id, service1.zone_id);
        assert_eq!(services[0].kind, service1.kind);
        assert_eq!(services[1].id(), service2.id());
        assert_eq!(services[1].sled_id, service2.sled_id);
        assert_eq!(services[1].zone_id, service2.zone_id);
        assert_eq!(services[1].kind, service2.kind);
        assert_eq!(services.len(), 2);

        // Try listing services of a different kind.
        let services = datastore
            .services_list_kind(
                &opctx,
                ServiceKind::Oximeter,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: NonZeroU32::new(3).unwrap(),
                },
            )
            .await
            .unwrap();
        assert_eq!(services[0].id(), service3.id());
        assert_eq!(services[0].sled_id, service3.sled_id);
        assert_eq!(services[0].zone_id, service3.zone_id);
        assert_eq!(services[0].kind, service3.kind);
        assert_eq!(services.len(), 1);

        // Try listing services of a kind for which there are no services.
        let services = datastore
            .services_list_kind(
                &opctx,
                ServiceKind::Dendrite,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: NonZeroU32::new(3).unwrap(),
                },
            )
            .await
            .unwrap();
        assert!(services.is_empty());

        // As a quick check, try supplying a marker.
        let services = datastore
            .services_list_kind(
                &opctx,
                ServiceKind::Nexus,
                &DataPageParams {
                    marker: Some(&service1_id),
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: NonZeroU32::new(3).unwrap(),
                },
            )
            .await
            .unwrap();
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].id(), service2.id());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_rack_initialize_is_idempotent() {
        let logctx = dev::test_setup_log("test_rack_initialize_is_idempotent");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a Rack, insert it into the DB.
        let rack = Rack::new(Uuid::new_v4());
        let result = datastore.rack_insert(&opctx, &rack).await.unwrap();
        assert_eq!(result.id(), rack.id());
        assert_eq!(result.initialized, false);

        // Re-insert the Rack (check for idempotency).
        let result = datastore.rack_insert(&opctx, &rack).await.unwrap();
        assert_eq!(result.id(), rack.id());
        assert_eq!(result.initialized, false);

        // Initialize the Rack.
        let result = datastore
            .rack_set_initialized(
                &opctx,
                RackInit { rack_id: rack.id(), ..Default::default() },
            )
            .await
            .unwrap();
        assert!(result.initialized);

        // Re-initialize the rack (check for idempotency)
        let result = datastore
            .rack_set_initialized(
                &opctx,
                RackInit { rack_id: rack.id(), ..Default::default() },
            )
            .await
            .unwrap();
        assert!(result.initialized);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_table_scan() {
        let logctx = dev::test_setup_log("test_table_scan");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let error = datastore.test_try_table_scan(&opctx).await;
        println!("error from attempted table scan: {:#}", error);
        match error {
            Error::InternalError { internal_message } => {
                assert!(internal_message.contains(
                    "contains a full table/index scan which is \
                    explicitly disallowed"
                ));
            }
            error => panic!(
                "expected internal error with specific message, found {:?}",
                error
            ),
        }

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_deallocate_external_ip_by_instance_id_is_idempotent() {
        use crate::db::model::IpKind;
        use crate::db::schema::external_ip::dsl;

        let logctx = dev::test_setup_log(
            "test_deallocate_external_ip_by_instance_id_is_idempotent",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Create a few records.
        let now = Utc::now();
        let instance_id = Uuid::new_v4();
        let ips = (0..4)
            .map(|i| ExternalIp {
                id: Uuid::new_v4(),
                name: None,
                description: None,
                time_created: now,
                time_modified: now,
                time_deleted: None,
                ip_pool_id: Uuid::new_v4(),
                ip_pool_range_id: Uuid::new_v4(),
                is_service: false,
                parent_id: Some(instance_id),
                kind: IpKind::Ephemeral,
                ip: ipnetwork::IpNetwork::from(IpAddr::from(Ipv4Addr::new(
                    10, 0, 0, i,
                ))),
                first_port: crate::db::model::SqlU16(0),
                last_port: crate::db::model::SqlU16(10),
            })
            .collect::<Vec<_>>();
        diesel::insert_into(dsl::external_ip)
            .values(ips.clone())
            .execute_async(&*conn)
            .await
            .unwrap();

        // Delete everything, make sure we delete all records we made above
        let count = datastore
            .deallocate_external_ip_by_instance_id(&opctx, instance_id)
            .await
            .expect("Failed to delete instance external IPs");
        assert_eq!(
            count,
            ips.len(),
            "Expected to delete all IPs for the instance"
        );

        // Do it again, we should get zero records
        let count = datastore
            .deallocate_external_ip_by_instance_id(&opctx, instance_id)
            .await
            .expect("Failed to delete instance external IPs");
        assert_eq!(count, 0, "Expected to delete zero IPs for the instance");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_deallocate_external_ip_is_idempotent() {
        use crate::db::model::IpKind;
        use crate::db::schema::external_ip::dsl;

        let logctx =
            dev::test_setup_log("test_deallocate_external_ip_is_idempotent");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Create a record.
        let now = Utc::now();
        let ip = ExternalIp {
            id: Uuid::new_v4(),
            name: None,
            description: None,
            time_created: now,
            time_modified: now,
            time_deleted: None,
            ip_pool_id: Uuid::new_v4(),
            ip_pool_range_id: Uuid::new_v4(),
            is_service: false,
            parent_id: Some(Uuid::new_v4()),
            kind: IpKind::SNat,
            ip: ipnetwork::IpNetwork::from(IpAddr::from(Ipv4Addr::new(
                10, 0, 0, 1,
            ))),
            first_port: crate::db::model::SqlU16(0),
            last_port: crate::db::model::SqlU16(10),
        };
        diesel::insert_into(dsl::external_ip)
            .values(ip.clone())
            .execute_async(&*conn)
            .await
            .unwrap();

        // Delete it twice, make sure we get the right sentinel return values.
        let deleted =
            datastore.deallocate_external_ip(&opctx, ip.id).await.unwrap();
        assert!(
            deleted,
            "Got unexpected sentinel value back when \
            deleting external IP the first time"
        );
        let deleted =
            datastore.deallocate_external_ip(&opctx, ip.id).await.unwrap();
        assert!(
            !deleted,
            "Got unexpected sentinel value back when \
            deleting external IP the second time"
        );

        // Deleting a non-existing record fails
        assert!(datastore
            .deallocate_external_ip(&opctx, Uuid::nil())
            .await
            .is_err());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_external_ip_check_constraints() {
        use crate::db::model::IpKind;
        use crate::db::schema::external_ip::dsl;
        use diesel::result::DatabaseErrorKind::CheckViolation;
        use diesel::result::Error::DatabaseError;

        let logctx = dev::test_setup_log("test_external_ip_check_constraints");
        let mut db = test_setup_database(&logctx.log).await;
        let (_opctx, datastore) = datastore_test(&logctx, &db).await;
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let now = Utc::now();

        // Create a mostly-populated record, for a floating IP
        let subnet = ipnetwork::IpNetwork::new(
            IpAddr::from(Ipv4Addr::new(10, 0, 0, 0)),
            8,
        )
        .unwrap();
        let mut addresses = subnet.iter();
        let ip = ExternalIp {
            id: Uuid::new_v4(),
            name: None,
            description: None,
            time_created: now,
            time_modified: now,
            time_deleted: None,
            ip_pool_id: Uuid::new_v4(),
            ip_pool_range_id: Uuid::new_v4(),
            is_service: false,
            parent_id: Some(Uuid::new_v4()),
            kind: IpKind::Floating,
            ip: addresses.next().unwrap().into(),
            first_port: crate::db::model::SqlU16(0),
            last_port: crate::db::model::SqlU16(10),
        };

        // Combinations of NULL and non-NULL for:
        // - name
        // - description
        // - parent (instance / service) UUID
        let names = [
            None,
            Some(db::model::Name(Name::try_from("foo".to_string()).unwrap())),
        ];
        let descriptions = [None, Some("foo".to_string())];
        let parent_ids = [None, Some(Uuid::new_v4())];

        // For Floating IPs, both name and description must be non-NULL
        for name in names.iter() {
            for description in descriptions.iter() {
                for parent_id in parent_ids.iter() {
                    for is_service in [false, true] {
                        let new_ip = ExternalIp {
                            id: Uuid::new_v4(),
                            name: name.clone(),
                            description: description.clone(),
                            ip: addresses.next().unwrap().into(),
                            is_service,
                            parent_id: *parent_id,
                            ..ip
                        };
                        let res = diesel::insert_into(dsl::external_ip)
                            .values(new_ip)
                            .execute_async(&*conn)
                            .await;
                        if name.is_some() && description.is_some() {
                            // Name/description must be non-NULL, instance ID can be
                            // either
                            res.unwrap_or_else(|_| {
                                panic!(
                                    "Failed to insert Floating IP with valid \
                                     name, description, and {} ID",
                                    if is_service {
                                        "Service"
                                    } else {
                                        "Instance"
                                    }
                                )
                            });
                        } else {
                            // At least one is not valid, we expect a check violation
                            let err = res.expect_err(
                                "Expected a CHECK violation when inserting a \
                                 Floating IP record with NULL name and/or description",
                            );
                            assert!(
                                matches!(
                                    err,
                                    DatabaseError(
                                        CheckViolation,
                                        _
                                    )
                                ),
                                "Expected a CHECK violation when inserting a \
                                 Floating IP record with NULL name and/or description",
                            );
                        }
                    }
                }
            }
        }

        // For other IP types, both name and description must be NULL
        for kind in [IpKind::SNat, IpKind::Ephemeral].into_iter() {
            for name in names.iter() {
                for description in descriptions.iter() {
                    for parent_id in parent_ids.iter() {
                        for is_service in [false, true] {
                            let new_ip = ExternalIp {
                                id: Uuid::new_v4(),
                                name: name.clone(),
                                description: description.clone(),
                                kind,
                                ip: addresses.next().unwrap().into(),
                                is_service,
                                parent_id: *parent_id,
                                ..ip
                            };
                            let res = diesel::insert_into(dsl::external_ip)
                                .values(new_ip.clone())
                                .execute_async(&*conn)
                                .await;
                            let ip_type =
                                if is_service { "Service" } else { "Instance" };
                            if name.is_none()
                                && description.is_none()
                                && parent_id.is_some()
                            {
                                // Name/description must be NULL, instance ID cannot
                                // be NULL.

                                if kind == IpKind::Ephemeral && is_service {
                                    // Ephemeral Service IPs aren't supported.
                                    let err = res.unwrap_err();
                                    assert!(
                                        matches!(
                                            err,
                                            DatabaseError(
                                                CheckViolation,
                                                _
                                            )
                                        ),
                                        "Expected a CHECK violation when inserting an \
                                         Ephemeral Service IP",
                                    );
                                } else {
                                    assert!(
                                        res.is_ok(),
                                        "Failed to insert {:?} IP with valid \
                                         name, description, and {} ID",
                                        kind,
                                        ip_type,
                                    );
                                }
                            } else {
                                // One is not valid, we expect a check violation
                                assert!(
                                    res.is_err(),
                                    "Expected a CHECK violation when inserting a \
                                     {:?} IP record with non-NULL name, description, \
                                     and/or {} ID",
                                    kind,
                                    ip_type,
                                );
                                let err = res.unwrap_err();
                                assert!(
                                    matches!(
                                        err,
                                        DatabaseError(
                                            CheckViolation,
                                            _
                                        )
                                    ),
                                    "Expected a CHECK violation when inserting a \
                                     {:?} IP record with non-NULL name, description, \
                                     and/or {} ID",
                                    kind,
                                    ip_type,
                                );
                            }
                        }
                    }
                }
            }
        }
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    /// Expect DB error if we try to insert a system update with an id that
    /// already exists. If version matches, update the existing row (currently
    /// only time_modified)
    #[tokio::test]
    async fn test_system_update_conflict() {
        let logctx = dev::test_setup_log("test_system_update_conflict");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let v1 = external::SemverVersion::new(1, 0, 0);
        let update1 = SystemUpdate::new(v1.clone()).unwrap();
        datastore
            .upsert_system_update(&opctx, update1.clone())
            .await
            .expect("Failed to create system update");

        // same version, but different ID (generated by constructor). should
        // conflict and therefore update time_modified, keeping the old ID
        let update2 = SystemUpdate::new(v1).unwrap();
        let updated_update = datastore
            .upsert_system_update(&opctx, update2.clone())
            .await
            .unwrap();
        assert!(updated_update.identity.id == update1.identity.id);
        assert!(
            updated_update.identity.time_modified
                != update1.identity.time_modified
        );

        // now let's do same ID, but different version. should conflict on the
        // ID because it's the PK, but since the version doesn't match an
        // existing row, it errors out instead of updating one
        let update3 =
            SystemUpdate::new(external::SemverVersion::new(2, 0, 0)).unwrap();
        let update3 = SystemUpdate { identity: update1.identity, ..update3 };
        let conflict =
            datastore.upsert_system_update(&opctx, update3).await.unwrap_err();
        assert_matches!(conflict, Error::ObjectAlreadyExists { .. });

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    /// Expect DB error if we try to insert a component update with a (version,
    /// component_type) that already exists
    #[tokio::test]
    async fn test_component_update_conflict() {
        let logctx = dev::test_setup_log("test_component_update_conflict");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // we need a system update for the component updates to hang off of
        let v1 = external::SemverVersion::new(1, 0, 0);
        let system_update = SystemUpdate::new(v1.clone()).unwrap();
        datastore
            .upsert_system_update(&opctx, system_update.clone())
            .await
            .expect("Failed to create system update");

        // create a component update, that's fine
        let cu1 = ComponentUpdate {
            identity: ComponentUpdateIdentity::new(Uuid::new_v4()),
            component_type: UpdateableComponentType::HubrisForSidecarRot,
            version: db::model::SemverVersion::new(1, 0, 0),
        };
        datastore
            .create_component_update(
                &opctx,
                system_update.identity.id,
                cu1.clone(),
            )
            .await
            .expect("Failed to create component update");

        // create a second component update with same version but different
        // type, also fine
        let cu2 = ComponentUpdate {
            identity: ComponentUpdateIdentity::new(Uuid::new_v4()),
            component_type: UpdateableComponentType::HubrisForSidecarSp,
            version: db::model::SemverVersion::new(1, 0, 0),
        };
        datastore
            .create_component_update(
                &opctx,
                system_update.identity.id,
                cu2.clone(),
            )
            .await
            .expect("Failed to create component update");

        // but same type and version should fail
        let cu3 = ComponentUpdate {
            identity: ComponentUpdateIdentity::new(Uuid::new_v4()),
            ..cu1
        };
        let conflict = datastore
            .create_component_update(&opctx, system_update.identity.id, cu3)
            .await
            .unwrap_err();
        assert_matches!(conflict, Error::ObjectAlreadyExists { .. });

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
