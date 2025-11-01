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

use super::Pool;
use crate::authz;
use crate::context::OpContext;
use ::oximeter::types::ProducerRegistry;
use anyhow::{Context, bail};
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::{QueryFragment, QueryId};
use diesel::query_dsl::methods::LoadQuery;
use diesel::{ExpressionMethods, QueryDsl};
use iddqd::IdOrdMap;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_lookup::{DataStoreConnection, DbConnection};
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::backoff::{
    BackoffError, retry_notify, retry_policy_internal_service,
};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::net::Ipv6Addr;
use std::num::NonZeroU32;
use std::sync::Arc;

mod address_lot;
mod affinity;
mod alert;
mod alert_rx;
mod allow_list;
mod audit_log;
mod auth;
mod bfd;
mod bgp;
mod bootstore;
mod certificate;
mod clickhouse_policy;
mod cockroachdb_node_id;
mod cockroachdb_settings;
mod console_session;
mod crucible_dataset;
mod db_metadata;
mod deployment;
mod device_auth;
mod disk;
mod dns;
mod ereport;
mod external_ip;
mod identity_provider;
mod image;
pub mod instance;
mod inventory;
mod ip_pool;
mod lldp;
mod lookup_interface;
mod migration;
mod nat_entry;
mod network_interface;
mod oximeter;
mod oximeter_read_policy;
mod physical_disk;
mod probe;
mod project;
mod quota;
mod rack;
mod reconfigurator_config;
mod region;
mod region_replacement;
mod region_snapshot;
pub mod region_snapshot_replacement;
mod rendezvous_debug_dataset;
mod role;
mod saga;
mod scim;
mod scim_provider_store;
mod silo;
mod silo_auth_settings;
mod silo_group;
mod silo_user;
mod sled;
mod sled_instance;
mod snapshot;
mod ssh_key;
mod support_bundle;
mod switch;
mod switch_interface;
mod switch_port;
mod target_release;
#[cfg(test)]
pub(crate) mod test_utils;
pub mod update;
mod user_data_export;
mod utilization;
mod v2p_mapping;
mod virtual_provisioning_collection;
mod vmm;
mod volume;
mod volume_repair;
mod vpc;
pub mod webhook_delivery;
mod zpool;

pub use address_lot::AddressLotCreateResult;
pub use db_metadata::DatastoreSetupAction;
pub use db_metadata::ValidatedDatastoreSetupAction;
pub use deployment::BlueprintLimitReachedOutput;
pub use dns::DataStoreDnsTest;
pub use dns::DnsVersionUpdateBuilder;
pub use ereport::EreportFilters;
pub use instance::{
    InstanceAndActiveVmm, InstanceGestalt, InstanceStateComputer,
};
pub use inventory::DataStoreInventoryTest;
pub use ip_pool::IpPoolListFilters;
use nexus_db_model::AllSchemaVersions;
use nexus_types::internal_api::views::HeldDbClaimInfo;
pub use oximeter::CollectorReassignment;
pub use rack::RackInit;
pub use rack::SledUnderlayAllocationResult;
pub use region::RegionAllocationFor;
pub use region::RegionAllocationParameters;
pub use region_snapshot_replacement::NewRegionVolumeId;
pub use region_snapshot_replacement::OldSnapshotVolumeId;
pub use scim_provider_store::CrdbScimProviderStore;
pub use silo::Discoverability;
pub use silo_group::SiloGroup;
pub use silo_group::SiloGroupApiOnly;
pub use silo_group::SiloGroupJit;
pub use silo_group::SiloGroupLookup;
pub use silo_user::SiloUser;
pub use silo_user::SiloUserApiOnly;
pub use silo_user::SiloUserJit;
pub use silo_user::SiloUserLookup;
pub use silo_user::SiloUserScim;
pub use sled::SledTransition;
pub use sled::TransitionError;
pub use support_bundle::SupportBundleExpungementReport;
pub use switch_port::SwitchPortSettingsCombinedResult;
pub use user_data_export::*;
pub use virtual_provisioning_collection::StorageType;
pub use vmm::VmmStateUpdateResult;
pub use volume::*;

// Number of unique datasets required to back a region.
// TODO: This should likely turn into a configuration option.
pub const REGION_REDUNDANCY_THRESHOLD: usize = 3;

/// The name of the built-in IPv4 IP pool for Oxide services.
pub const SERVICE_IPV4_POOL_NAME: &str = "oxide-service-pool-v4";

/// The name of the built-in IPv6 IP pool for Oxide services.
pub const SERVICE_IPV6_POOL_NAME: &str = "oxide-service-pool-v6";

/// "limit" to be used in SQL queries that paginate through large result sets
///
/// This value is chosen to be small enough to avoid any queries being too
/// expensive.
// unsafe: `new_unchecked` is only unsound if the argument is 0.
pub const SQL_BATCH_SIZE: NonZeroU32 = NonZeroU32::new(1000).unwrap();

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

/// Specifies whether the consumer wants to check whether they're allowed to
/// access the database based on the `db_metadata_nexus` table.
#[derive(Debug, Clone, Copy)]
pub enum IdentityCheckPolicy {
    /// The consumer wants full access to the database regardless of the current
    /// upgrade / handoff state.  This would be used by almost all tools and
    /// tests.
    DontCare,

    /// The consumer only wants to access the database if it's in the current
    /// set of Nexus instances that's supposed to be able to access it.  If
    /// possible and legal, take over access from the existing set.
    CheckAndTakeover { nexus_id: OmicronZoneUuid },
}

pub struct DataStore {
    log: Logger,
    pool: Arc<Pool>,
    virtual_provisioning_collection_producer: crate::provisioning::Producer,
    transaction_retry_producer: crate::transaction_retry::Producer,
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
    pub fn new_unchecked(log: Logger, pool: Arc<Pool>) -> Self {
        DataStore {
            log,
            pool,
            virtual_provisioning_collection_producer:
                crate::provisioning::Producer::new(),
            transaction_retry_producer: crate::transaction_retry::Producer::new(
            ),
        }
    }

    /// Constructs a new Datastore object.
    ///
    /// Only returns when the database schema is compatible with Nexus's known
    /// schema version.
    pub async fn new(
        log: &Logger,
        pool: Arc<Pool>,
        config: Option<&AllSchemaVersions>,
        identity_check: IdentityCheckPolicy,
    ) -> Result<Self, String> {
        Self::new_with_timeout(log, pool, config, None, identity_check).await
    }

    pub async fn new_with_timeout(
        log: &Logger,
        pool: Arc<Pool>,
        config: Option<&AllSchemaVersions>,
        try_for: Option<std::time::Duration>,
        identity_check: IdentityCheckPolicy,
    ) -> Result<Self, String> {
        use db_metadata::DatastoreSetupAction;
        use nexus_db_model::SCHEMA_VERSION as EXPECTED_VERSION;

        let datastore =
            Self::new_unchecked(log.new(o!("component" => "datastore")), pool);

        let start = std::time::Instant::now();

        // Keep looping until we find that the schema matches our expectation.
        retry_notify(
            retry_policy_internal_service(),
            || async {
                if let Some(try_for) = try_for {
                    if std::time::Instant::now() > start + try_for {
                        return Err(BackoffError::permanent(
                            "Timeout waiting for DataStore::new_with_timeout",
                        ));
                    }
                }

                loop {
                    let checked_action = datastore
                        .check_schema_and_access(
                            identity_check,
                            EXPECTED_VERSION,
                        )
                        .await
                        .map_err(|err| {
                            warn!(
                                log,
                                "Cannot check schema version / Nexus access";
                                InlineErrorChain::new(err.as_ref()),
                            );
                            BackoffError::transient(
                                "Cannot check schema version / Nexus access",
                            )
                        })?;

                    match checked_action.action() {
                        DatastoreSetupAction::Ready => {
                            info!(log, "Datastore is ready for usage");
                            return Ok(());
                        }
                        DatastoreSetupAction::NeedsHandoff { nexus_id } => {
                            info!(log, "Datastore is awaiting handoff");

                            datastore
                                .attempt_handoff(*nexus_id)
                                .await
                                .map_err(|err| {
                                    warn!(
                                        log,
                                        "Could not handoff to new nexus";
                                        err
                                    );
                                    BackoffError::transient(
                                        "Could not handoff to new nexus",
                                    )
                                })?;

                            // If the handoff was successful, immediately
                            // re-evaluate the schema and access policies to see
                            // if we should update or not.
                            continue;
                        }
                        DatastoreSetupAction::TryLater => {
                            error!(log, "Waiting for metadata; trying later");
                            return Err(BackoffError::permanent(
                                "Waiting for metadata; trying later",
                            ));
                        }
                        DatastoreSetupAction::Update => {
                            info!(
                                log,
                                "Datastore should be updated before usage"
                            );
                            datastore
                                .update_schema(checked_action, config)
                                .await
                                .map_err(|err| {
                                    warn!(
                                        log,
                                        "Failed to update schema version";
                                        InlineErrorChain::new(err.as_ref())
                                    );
                                    BackoffError::transient(
                                        "Failed to update schema version",
                                    )
                                })?;
                            return Ok(());
                        }
                        DatastoreSetupAction::Refuse => {
                            error!(log, "Datastore should not be used");
                            return Err(BackoffError::permanent(
                                "Datastore should not be used",
                            ));
                        }
                    }
                }
            },
            |_, _| {},
        )
        .await
        .map_err(|err| err.to_string())?;

        Ok(datastore)
    }

    /// Constructs a new Datastore, failing if the schema version does not match
    /// this program's expected version
    pub async fn new_failfast(
        log: &Logger,
        pool: Arc<Pool>,
    ) -> Result<Self, anyhow::Error> {
        use nexus_db_model::SCHEMA_VERSION as EXPECTED_VERSION;

        let datastore =
            Self::new_unchecked(log.new(o!("component" => "datastore")), pool);
        let (found_version, found_target) = datastore
            .database_schema_version()
            .await
            .context("loading database schema version")?;

        if let Some(found_target) = found_target {
            bail!(
                "database schema check failed: apparently mid-upgrade \
                 (found_target = {found_target})"
            );
        }

        if found_version != EXPECTED_VERSION {
            bail!(
                "database schema check failed: \
                 expected {EXPECTED_VERSION}, found {found_version}",
            );
        }

        Ok(datastore)
    }

    /// Disables creation of all new database claims
    ///
    /// This is currently a one-way trip.  The DataStore cannot be un-quiesced.
    pub fn quiesce(&self) {
        self.pool.quiesce();
    }

    /// Wait for all outstanding claims to be released
    pub async fn wait_for_quiesced(&self) {
        self.pool.wait_for_quiesced().await;
    }

    /// Returns information about held db claims
    pub fn claims_held(&self) -> IdOrdMap<HeldDbClaimInfo> {
        self.pool.claims_held()
    }

    /// Terminates the underlying pool, stopping it from connecting to backends.
    pub async fn terminate(&self) {
        self.pool.terminate().await
    }

    pub fn register_producers(&self, registry: &ProducerRegistry) {
        registry
            .register_producer(
                self.virtual_provisioning_collection_producer.clone(),
            )
            .unwrap();
        registry
            .register_producer(self.transaction_retry_producer.clone())
            .unwrap();
    }

    /// Constructs a transaction retry helper
    ///
    /// Automatically wraps the underlying producer
    pub fn transaction_retry_wrapper(
        &self,
        name: &'static str,
    ) -> crate::transaction_retry::RetryHelper {
        crate::transaction_retry::RetryHelper::new(
            &self.log,
            &self.transaction_retry_producer,
            name,
        )
    }

    /// Constructs a non-retryable transaction helper
    pub fn transaction_non_retry_wrapper(
        &self,
        name: &'static str,
    ) -> crate::transaction_retry::NonRetryHelper {
        crate::transaction_retry::NonRetryHelper::new(&self.log, name)
    }

    #[cfg(test)]
    pub(crate) fn transaction_retry_producer(
        &self,
    ) -> &crate::transaction_retry::Producer {
        &self.transaction_retry_producer
    }

    /// Returns a connection to a connection from the database connection pool.
    pub(super) async fn pool_connection_authorized(
        &self,
        opctx: &OpContext,
    ) -> Result<DataStoreConnection, Error> {
        opctx.authorize(authz::Action::Query, &authz::DATABASE).await?;
        self.pool_connection_unauthorized().await
    }

    /// Returns an unauthorized connection to a connection from the database
    /// connection pool.
    ///
    /// TODO-security: This should be deprecated in favor of
    /// "pool_connection_authorized".
    pub(super) async fn pool_connection_unauthorized(
        &self,
    ) -> Result<DataStoreConnection, Error> {
        self.pool.claim().await
    }

    /// For testing only. This isn't cfg(test) because nexus needs access to it.
    #[doc(hidden)]
    pub async fn pool_connection_for_tests(
        &self,
    ) -> Result<DataStoreConnection, Error> {
        self.pool_connection_unauthorized().await
    }

    /// Return the next available IPv6 address for a propolis instance running
    /// on the provided sled.
    pub async fn next_ipv6_address(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<Ipv6Addr, Error> {
        use nexus_db_schema::schema::sled::dsl;
        let net = diesel::update(
            dsl::sled
                .find(sled_id.into_untyped_uuid())
                .filter(dsl::time_deleted.is_null()),
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
                    LookupType::by_id(sled_id),
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
        use nexus_db_schema::schema::project::dsl;
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

#[derive(Clone, Copy, Debug)]
pub enum UpdatePrecondition<T> {
    DontCare,
    Null,
    Value(T),
}

/// Whether state transitions should be validated. "No" is only accessible in
/// test-only code.
///
/// Intended only for testing around illegal states.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[must_use]
enum ValidateTransition {
    Yes,
    #[cfg(test)]
    No,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::authn;
    use crate::authn::SiloAuthnPolicy;
    use crate::authz;
    use crate::db::datastore::test_utils::{
        IneligibleSledKind, IneligibleSleds,
    };
    use crate::db::explain::ExplainableAsync;
    use crate::db::identity::Asset;
    use crate::db::model::{
        BlockSize, ConsoleSession, CrucibleDataset, ExternalIp, PhysicalDisk,
        PhysicalDiskKind, PhysicalDiskPolicy, PhysicalDiskState, Project, Rack,
        Region, SshKey, Zpool,
    };
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::SledUpdateBuilder;
    use crate::db::queries::vpc_subnet::InsertVpcSubnetQuery;
    use chrono::{Duration, Utc};
    use futures::StreamExt;
    use futures::stream;
    use nexus_config::RegionAllocationStrategy;
    use nexus_db_fixed_data::silo::DEFAULT_SILO;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::IpAttachState;
    use nexus_db_model::to_db_typed_uuid;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::external_api::params;
    use nexus_types::silo::DEFAULT_SILO_ID;
    use omicron_common::address::REPO_DEPOT_PORT;
    use omicron_common::api::external::{
        ByteCount, Error, IdentityMetadataCreateParams, LookupType, Name,
    };
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SiloUserUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};
    use std::sync::Arc;
    use strum::EnumCount;
    use uuid::Uuid;

    /// Inserts a blueprint in the DB and forcibly makes it the target
    ///
    /// WARNING: This makes no attempts to validate the blueprint relative to
    /// parents -- this is just a test-only helper to make testing
    /// blueprint-specific checks easier.
    pub async fn bp_insert_and_make_target(
        opctx: &OpContext,
        datastore: &DataStore,
        bp: &Blueprint,
    ) {
        datastore
            .blueprint_insert(opctx, bp)
            .await
            .expect("inserted blueprint");
        datastore
            .blueprint_target_set_current(
                opctx,
                BlueprintTarget {
                    target_id: bp.id,
                    enabled: true,
                    time_made_target: Utc::now(),
                },
            )
            .await
            .expect("made blueprint the target");
    }

    #[tokio::test]
    async fn test_project_creation() {
        let logctx = dev::test_setup_log("test_project_creation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let authz_silo = opctx.authn.silo_required().unwrap();

        let (.., silo) = LookupPath::new(&opctx, datastore)
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
            LookupPath::new(&opctx, datastore)
                .silo_id(authz_silo.id())
                .fetch()
                .await
                .unwrap();
        assert!(silo_after_project_create.rcgen > silo.rcgen);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_session_methods() {
        let logctx = dev::test_setup_log("test_session_methods");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let authn_opctx = OpContext::for_background(
            logctx.log.new(o!("component" => "TestExternalAuthn")),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::external_authn(),
            Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        let token = "a_token".to_string();
        let silo_user_id = SiloUserUuid::new_v4();

        let both_times = Utc::now() - Duration::minutes(5);

        let session = ConsoleSession::new_with_times(
            token.clone(),
            silo_user_id,
            both_times,
            both_times,
        );

        let _ = datastore
            .session_create(&authn_opctx, session.clone())
            .await
            .unwrap();

        // Associate silo with user
        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );
        datastore
            .silo_user_create(
                &authz_silo,
                SiloUserApiOnly::new(
                    authz_silo.id(),
                    silo_user_id,
                    "external_id".into(),
                )
                .into(),
            )
            .await
            .unwrap();

        let (.., db_silo_user) = LookupPath::new(&opctx, datastore)
            .silo_user_id(session.silo_user_id())
            .fetch()
            .await
            .unwrap();
        assert_eq!(DEFAULT_SILO_ID, db_silo_user.silo_id);

        // fetch the one we just created
        let (.., fetched) = datastore
            .session_lookup_by_token(&authn_opctx, token.clone())
            .await
            .unwrap();
        assert_eq!(session.silo_user_id(), fetched.silo_user_id());
        assert_eq!(session.id, fetched.id);

        // also try looking it up by ID
        let (.., fetched) = LookupPath::new(&opctx, datastore)
            .console_session_id(session.id.into())
            .fetch()
            .await
            .unwrap();
        assert_eq!(session.silo_user_id(), fetched.silo_user_id());
        assert_eq!(session.token, fetched.token);

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
            session.id.into(),
            LookupType::ById(session.id.into_untyped_uuid()),
        );
        let renewed = datastore
            .session_update_last_used(&opctx, &authz_session)
            .await
            .unwrap();
        assert!(
            renewed.console_session.time_last_used > session.time_last_used
        );

        // time_last_used change persists in DB
        let (.., fetched) = datastore
            .session_lookup_by_token(&opctx, token.clone())
            .await
            .unwrap();
        assert!(fetched.time_last_used > session.time_last_used);

        // delete it and fetch should come back with nothing
        let silo_user_opctx = OpContext::for_background(
            logctx.log.new(o!()),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::for_test_user(
                silo_user_id,
                DEFAULT_SILO_ID,
                SiloAuthnPolicy::try_from(&*DEFAULT_SILO).unwrap(),
            ),
            Arc::clone(&datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );
        let delete = datastore
            .session_hard_delete_by_token(&silo_user_opctx, token.clone())
            .await;
        assert_eq!(delete, Ok(()));
        let fetched = datastore
            .session_lookup_by_token(&authn_opctx, token.clone())
            .await;
        assert!(matches!(
            fetched,
            Err(Error::ObjectNotFound { type_name: _, lookup_type: _ })
        ));

        // deleting an already nonexistent is considered a success
        let delete_again =
            datastore.session_hard_delete_by_token(&opctx, token.clone()).await;
        assert_eq!(delete_again, Ok(()));

        let delete_again =
            datastore.session_hard_delete_by_token(&opctx, token.clone()).await;
        assert_eq!(delete_again, Ok(()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Creates a test sled, returns its UUID.
    async fn create_test_sled(datastore: &DataStore) -> SledUuid {
        let sled_id = SledUuid::new_v4();
        let sled_update = SledUpdateBuilder::new().sled_id(sled_id).build();
        datastore.sled_upsert(sled_update).await.unwrap();
        sled_id
    }

    fn test_zpool_size() -> ByteCount {
        ByteCount::from_gibibytes_u32(100)
    }

    const TEST_VENDOR: &str = "test-vendor";
    const TEST_MODEL: &str = "test-model";

    /// Creates a disk on a sled of a particular kind.
    ///
    /// The "serial" value of the disk is supplied by the
    /// caller, and is arbitrary, but should be unique.
    async fn create_test_physical_disk(
        datastore: &DataStore,
        opctx: &OpContext,
        sled_id: SledUuid,
        kind: PhysicalDiskKind,
        serial: String,
    ) -> PhysicalDiskUuid {
        let physical_disk = PhysicalDisk::new(
            PhysicalDiskUuid::new_v4(),
            TEST_VENDOR.into(),
            serial,
            TEST_MODEL.into(),
            kind,
            sled_id,
        );
        datastore
            .physical_disk_insert(opctx, physical_disk.clone())
            .await
            .expect("Failed to upsert physical disk");
        physical_disk.id()
    }

    // Creates a test zpool, returns its UUID.
    async fn create_test_zpool(
        datastore: &DataStore,
        opctx: &OpContext,
        sled_id: SledUuid,
        physical_disk_id: PhysicalDiskUuid,
    ) -> ZpoolUuid {
        let zpool_id = create_test_zpool_not_in_inventory(
            datastore,
            opctx,
            sled_id,
            physical_disk_id,
        )
        .await;

        add_test_zpool_to_inventory(datastore, zpool_id, sled_id).await;

        zpool_id
    }

    // Creates a test zpool, returns its UUID.
    //
    // However, this helper doesn't add the zpool to the inventory just yet.
    async fn create_test_zpool_not_in_inventory(
        datastore: &DataStore,
        opctx: &OpContext,
        sled_id: SledUuid,
        physical_disk_id: PhysicalDiskUuid,
    ) -> ZpoolUuid {
        let zpool_id = ZpoolUuid::new_v4();
        let zpool = Zpool::new(
            zpool_id,
            sled_id,
            physical_disk_id,
            ByteCount::from(0).into(),
        );
        datastore.zpool_insert(opctx, zpool).await.unwrap();
        zpool_id
    }

    // Adds a test zpool into the inventory, with a randomly generated
    // collection UUID.
    async fn add_test_zpool_to_inventory(
        datastore: &DataStore,
        zpool_id: ZpoolUuid,
        sled_id: SledUuid,
    ) {
        use nexus_db_schema::schema::inv_zpool::dsl;

        let inv_collection_id = CollectionUuid::new_v4();
        let time_collected = Utc::now();
        let inv_pool = nexus_db_model::InvZpool {
            inv_collection_id: inv_collection_id.into(),
            time_collected,
            id: zpool_id.into(),
            sled_id: to_db_typed_uuid(sled_id),
            total_size: test_zpool_size().into(),
        };
        diesel::insert_into(dsl::inv_zpool)
            .values(inv_pool)
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
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

    #[derive(Debug)]
    pub(crate) struct TestDatasets {
        // eligible and ineligible aren't currently used, but are probably handy
        // for the future.
        #[allow(dead_code)]
        eligible: SledToDatasetMap,
        #[allow(dead_code)]
        ineligible: SledToDatasetMap,

        // A map from eligible dataset IDs to their corresponding sled IDs.
        eligible_dataset_ids: HashMap<DatasetUuid, SledUuid>,
        ineligible_dataset_ids: HashMap<DatasetUuid, IneligibleSledKind>,
    }

    // Map of sled IDs to dataset IDs.
    type SledToDatasetMap = HashMap<SledUuid, Vec<DatasetUuid>>;

    impl TestDatasets {
        pub(crate) async fn create(
            opctx: &OpContext,
            datastore: Arc<DataStore>,
            num_eligible_sleds: usize,
        ) -> Self {
            let eligible =
                Self::create_impl(opctx, datastore.clone(), num_eligible_sleds)
                    .await;

            let eligible_dataset_ids = eligible
                .iter()
                .flat_map(|(sled_id, dataset_ids)| {
                    dataset_ids
                        .iter()
                        .map(move |dataset_id| (*dataset_id, *sled_id))
                })
                .collect();

            let ineligible = Self::create_impl(
                opctx,
                datastore.clone(),
                IneligibleSledKind::COUNT,
            )
            .await;

            let mut ineligible_sled_ids = ineligible.keys();

            // Set up the ineligible sleds. (We're guaranteed that
            // IneligibleSledKind::COUNT is the same as the number of next()
            // calls below.)
            let ineligible_sleds = IneligibleSleds {
                non_provisionable: *ineligible_sled_ids.next().unwrap(),
                expunged: *ineligible_sled_ids.next().unwrap(),
                decommissioned: *ineligible_sled_ids.next().unwrap(),
                illegal_decommissioned: *ineligible_sled_ids.next().unwrap(),
            };

            eprintln!("Setting up ineligible sleds: {:?}", ineligible_sleds);

            ineligible_sleds
                .setup(opctx, &datastore)
                .await
                .expect("error setting up ineligible sleds");

            // Build a map of dataset IDs to their ineligible kind.
            let mut ineligible_dataset_ids = HashMap::new();
            for (kind, sled_id) in ineligible_sleds.iter() {
                for dataset_id in ineligible.get(&sled_id).unwrap() {
                    ineligible_dataset_ids.insert(*dataset_id, kind);
                }
            }

            Self {
                eligible,
                eligible_dataset_ids,
                ineligible,
                ineligible_dataset_ids,
            }
        }

        // Returns a map of sled ID to dataset IDs.
        async fn create_impl(
            opctx: &OpContext,
            datastore: Arc<DataStore>,
            number_of_sleds: usize,
        ) -> SledToDatasetMap {
            // Create sleds...
            let sled_ids: Vec<SledUuid> = stream::iter(0..number_of_sleds)
                .then(|_| create_test_sled(&datastore))
                .collect()
                .await;

            struct PhysicalDisk {
                sled_id: SledUuid,
                disk_id: PhysicalDiskUuid,
            }

            // create 9 disks on each sled
            let physical_disks: Vec<PhysicalDisk> = stream::iter(sled_ids)
                .map(|sled_id| {
                    let sled_id_iter: Vec<SledUuid> =
                        (0..9).map(|_| sled_id).collect();
                    stream::iter(sled_id_iter).enumerate().then(
                        |(i, sled_id)| {
                            let disk_id_future = create_test_physical_disk(
                                &datastore,
                                opctx,
                                sled_id,
                                PhysicalDiskKind::U2,
                                format!("{sled_id}, disk index {i}"),
                            );
                            async move {
                                let disk_id = disk_id_future.await;
                                PhysicalDisk { sled_id, disk_id }
                            }
                        },
                    )
                })
                .flatten()
                .collect()
                .await;

            #[derive(Copy, Clone)]
            struct Zpool {
                sled_id: SledUuid,
                pool_id: ZpoolUuid,
            }

            // 1 pool per disk
            let zpools: Vec<Zpool> = stream::iter(physical_disks)
                .then(|disk| {
                    let pool_id_future = create_test_zpool(
                        &datastore,
                        &opctx,
                        disk.sled_id,
                        disk.disk_id,
                    );
                    async move {
                        let pool_id = pool_id_future.await;
                        Zpool { sled_id: disk.sled_id, pool_id }
                    }
                })
                .collect()
                .await;

            let bogus_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);

            let datasets = stream::iter(zpools)
                .map(|zpool| {
                    // 3 datasets per zpool, to test that pools are distinct
                    let zpool_iter: Vec<Zpool> =
                        (0..3).map(|_| zpool).collect();
                    stream::iter(zpool_iter).then(|zpool| {
                        let dataset_id = DatasetUuid::new_v4();
                        let dataset = CrucibleDataset::new(
                            dataset_id,
                            zpool.pool_id,
                            bogus_addr,
                        );

                        let datastore = datastore.clone();
                        async move {
                            datastore
                                .crucible_dataset_upsert(dataset)
                                .await
                                .unwrap();

                            (zpool.sled_id, dataset_id)
                        }
                    })
                })
                .flatten()
                .fold(
                    SledToDatasetMap::new(),
                    |mut map, (sled_id, dataset_id)| {
                        // Build a map of sled ID to dataset IDs.
                        map.entry(sled_id)
                            .or_insert_with(Vec::new)
                            .push(dataset_id);
                        async move { map }
                    },
                )
                .await;

            datasets
        }
    }

    #[tokio::test]
    /// Note that this test is currently non-deterministic. It can be made
    /// deterministic by generating deterministic *dataset* Uuids. The sled and
    /// pool IDs should not matter.
    async fn test_region_allocation_strat_random() {
        let logctx = dev::test_setup_log("test_region_allocation_strat_random");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let test_datasets = TestDatasets::create(
            &opctx,
            datastore.clone(),
            // We aren't forcing the datasets to be on distinct sleds, so we
            // just need one eligible sled.
            1,
        )
        .await;

        // Allocate regions from the datasets for this disk. Do it a few times
        // for good measure.
        for alloc_seed in 0..10 {
            let params = create_test_disk_create_params(
                &format!("disk{}", alloc_seed),
                ByteCount::from_mebibytes_u32(1),
            );
            let volume_id = VolumeUuid::new_v4();

            let expected_region_count = REGION_REDUNDANCY_THRESHOLD;
            let dataset_and_regions = datastore
                .disk_region_allocate(
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
            let mut regions = HashSet::new();

            for (dataset, region) in dataset_and_regions {
                // Must be 3 unique datasets
                let dataset_id = dataset.id();
                assert!(disk_datasets.insert(dataset_id.into_untyped_uuid()));
                // All regions should be unique
                assert!(regions.insert(region.id()));

                // Check there's no cross contamination between returned UUIDs
                //
                // This is a little goofy, but it catches a bug that has
                // happened before. The returned columns share names (like
                // "id"), so we need to process them in-order.
                assert!(!regions.contains(dataset_id.as_untyped_uuid()));
                assert!(!disk_datasets.contains(&region.id()));

                // Dataset must not be eligible for provisioning.
                if let Some(kind) =
                    test_datasets.ineligible_dataset_ids.get(&dataset.id())
                {
                    panic!(
                        "Dataset {} was ineligible for provisioning: {:?}",
                        dataset.id(),
                        kind
                    );
                }

                // Must be 3 unique zpools
                assert!(disk_zpools.insert(dataset.pool_id()));

                assert_eq!(volume_id, region.volume_id());
                assert_eq!(ByteCount::from(4096), region.block_size());
                let (_, extent_count) = DataStore::get_crucible_allocation(
                    &BlockSize::AdvancedFormat,
                    params.size,
                );
                assert_eq!(extent_count, region.extent_count());
            }
        }

        db.terminate().await;
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
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a rack with enough sleds for a successful allocation when we
        // require 3 distinct eligible sleds.
        let test_datasets = TestDatasets::create(
            &opctx,
            datastore.clone(),
            // We're forcing the datasets to be on distinct sleds, hence the
            // full REGION_REDUNDANCY_THRESHOLD.
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
            let volume_id = VolumeUuid::new_v4();

            let expected_region_count = REGION_REDUNDANCY_THRESHOLD;
            let dataset_and_regions = datastore
                .disk_region_allocate(
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

                // Dataset must not be eligible for provisioning.
                if let Some(kind) =
                    test_datasets.ineligible_dataset_ids.get(&dataset.id())
                {
                    panic!(
                        "Dataset {} was ineligible for provisioning: {:?}",
                        dataset.id(),
                        kind
                    );
                }

                // Must be 3 unique zpools
                assert!(disk_zpools.insert(dataset.pool_id()));

                // Must be 3 unique sleds
                let sled_id = test_datasets
                    .eligible_dataset_ids
                    .get(&dataset.id())
                    .unwrap();
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    /// Ensure the [`RegionAllocationStrategy::RandomWithDistinctSleds`]
    /// strategy fails when there aren't enough distinct sleds.
    async fn test_region_allocation_strat_random_with_distinct_sleds_fails() {
        let logctx = dev::test_setup_log(
            "test_region_allocation_strat_random_with_distinct_sleds_fails",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a rack without enough sleds for a successful allocation when
        // we require 3 distinct provisionable sleds.
        TestDatasets::create(
            &opctx,
            datastore.clone(),
            // Here, we need to have REGION_REDUNDANCY_THRESHOLD - 1 eligible
            // sleds to test this failure condition.
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
            let volume_id = VolumeUuid::new_v4();

            let err = datastore
                .disk_region_allocate(
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

            assert!(matches!(err, Error::InsufficientCapacity { .. }));
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_is_idempotent() {
        let logctx =
            dev::test_setup_log("test_region_allocation_is_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        TestDatasets::create(
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
        let volume_id = VolumeUuid::new_v4();
        let mut dataset_and_regions1 = datastore
            .disk_region_allocate(
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
            .disk_region_allocate(
                &opctx,
                volume_id,
                &params.disk_source,
                params.size,
                &RegionAllocationStrategy::Random { seed: Some(1) },
            )
            .await
            .unwrap();

        // Give them a consistent order so we can easily compare them.
        let sort_vec = |v: &mut Vec<(CrucibleDataset, Region)>| {
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_only_operates_on_zpools_in_inventory() {
        let logctx = dev::test_setup_log(
            "test_region_allocation_only_operates_on_zpools_in_inventory",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a sled...
        let sled_id = create_test_sled(&datastore).await;

        // ... and a disk on that sled...
        let physical_disk_id = create_test_physical_disk(
            &datastore,
            &opctx,
            sled_id,
            PhysicalDiskKind::U2,
            "fake serial".to_string(),
        )
        .await;

        // Create enough zpools for region allocation to succeed
        let zpool_ids: Vec<ZpoolUuid> =
            stream::iter(0..REGION_REDUNDANCY_THRESHOLD)
                .then(|_| {
                    create_test_zpool_not_in_inventory(
                        &datastore,
                        &opctx,
                        sled_id,
                        physical_disk_id,
                    )
                })
                .collect()
                .await;

        let bogus_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);

        // 1 dataset per zpool
        stream::iter(zpool_ids.clone())
            .then(|zpool_id| {
                let id = DatasetUuid::new_v4();
                let dataset = CrucibleDataset::new(id, zpool_id, bogus_addr);
                let datastore = datastore.clone();
                async move {
                    datastore.crucible_dataset_upsert(dataset).await.unwrap();
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
        let volume1_id = VolumeUuid::new_v4();
        let err = datastore
            .disk_region_allocate(
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
        assert!(matches!(err, Error::InsufficientCapacity { .. }));

        // If we add the zpools to the inventory and try again, the allocation
        // will succeed.
        for zpool_id in zpool_ids {
            add_test_zpool_to_inventory(&datastore, zpool_id, sled_id).await;
        }
        datastore
            .disk_region_allocate(
                &opctx,
                volume1_id,
                &params.disk_source,
                params.size,
                &RegionAllocationStrategy::Random { seed: Some(0) },
            )
            .await
            .expect("Allocation should have worked after adding zpools to inventory");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_not_enough_zpools() {
        let logctx =
            dev::test_setup_log("test_region_allocation_not_enough_zpools");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a sled...
        let sled_id = create_test_sled(&datastore).await;

        // ... and a disk on that sled...
        let physical_disk_id = create_test_physical_disk(
            &datastore,
            &opctx,
            sled_id,
            PhysicalDiskKind::U2,
            "fake serial".to_string(),
        )
        .await;

        // 1 less than REDUNDANCY level of zpools
        let zpool_ids: Vec<ZpoolUuid> =
            stream::iter(0..REGION_REDUNDANCY_THRESHOLD - 1)
                .then(|_| {
                    create_test_zpool(
                        &datastore,
                        &opctx,
                        sled_id,
                        physical_disk_id,
                    )
                })
                .collect()
                .await;

        let bogus_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);

        // 1 dataset per zpool
        stream::iter(zpool_ids)
            .then(|zpool_id| {
                let id = DatasetUuid::new_v4();
                let dataset = CrucibleDataset::new(id, zpool_id, bogus_addr);
                let datastore = datastore.clone();
                async move {
                    datastore.crucible_dataset_upsert(dataset).await.unwrap();
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
        let volume1_id = VolumeUuid::new_v4();
        let err = datastore
            .disk_region_allocate(
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

        assert!(matches!(err, Error::InsufficientCapacity { .. }));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_only_considers_disks_in_service() {
        let logctx = dev::test_setup_log(
            "test_region_allocation_only_considers_disks_in_service",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a sled...
        let sled_id = create_test_sled(&datastore).await;

        // ... and create several disks on that sled, each with a zpool/dataset.
        let mut physical_disk_ids = vec![];
        for i in 0..REGION_REDUNDANCY_THRESHOLD {
            let physical_disk_id = create_test_physical_disk(
                &datastore,
                &opctx,
                sled_id,
                PhysicalDiskKind::U2,
                format!("fake serial #{i}"),
            )
            .await;
            let zpool_id = create_test_zpool(
                &datastore,
                &opctx,
                sled_id,
                physical_disk_id,
            )
            .await;
            let bogus_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);
            let dataset = CrucibleDataset::new(
                DatasetUuid::new_v4(),
                zpool_id,
                bogus_addr,
            );
            datastore.crucible_dataset_upsert(dataset).await.unwrap();
            physical_disk_ids.push(physical_disk_id);
        }

        // Check the following combinations of physical disk policy/state
        // on region allocation. Since we only created
        // REGION_REDUNDANCY_THRESHOLD disks/zpools/datasets, updating the
        // state of a single disk should be sufficient to prevent the
        // allocations from occurring.
        use PhysicalDiskPolicy as Policy;
        use PhysicalDiskState as State;

        // Just a bool with a fancier name -- determines whether or not
        // we expect the policy/state combinations to pass or not.
        enum AllocationShould {
            Fail,
            Succeed,
        }

        let policy_state_combos = [
            (Policy::Expunged, State::Active, AllocationShould::Fail),
            (Policy::Expunged, State::Decommissioned, AllocationShould::Fail),
            (Policy::InService, State::Decommissioned, AllocationShould::Fail),
            // Save this one for last, since it actually leaves an allocation
            // lying around.
            (Policy::InService, State::Active, AllocationShould::Succeed),
        ];

        let volume_id = VolumeUuid::new_v4();
        let params = create_test_disk_create_params(
            "disk",
            ByteCount::from_mebibytes_u32(500),
        );

        for (policy, state, expected) in policy_state_combos {
            // Update policy/state only on a single physical disk.
            //
            // The rest are assumed "in service" + "active".
            datastore
                .physical_disk_update_policy(
                    &opctx,
                    physical_disk_ids[0],
                    policy,
                )
                .await
                .unwrap();
            datastore
                .physical_disk_update_state(&opctx, physical_disk_ids[0], state)
                .await
                .unwrap();

            let result = datastore
                .disk_region_allocate(
                    &opctx,
                    volume_id,
                    &params.disk_source,
                    params.size,
                    &RegionAllocationStrategy::Random { seed: Some(0) },
                )
                .await;

            match expected {
                AllocationShould::Fail => {
                    let err = result.unwrap_err();
                    let expected = "Not enough zpool space to allocate disks";
                    assert!(
                        err.to_string().contains(expected),
                        "Saw error: \'{err}\', but expected \'{expected}\'"
                    );
                    assert!(matches!(err, Error::InsufficientCapacity { .. }));
                }
                AllocationShould::Succeed => {
                    let _ = result.expect("Allocation should have succeeded");
                }
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_region_allocation_out_of_space_fails() {
        let logctx =
            dev::test_setup_log("test_region_allocation_out_of_space_fails");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        TestDatasets::create(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let disk_size = test_zpool_size();
        let alloc_size = ByteCount::try_from(disk_size.to_bytes() * 2).unwrap();
        let params = create_test_disk_create_params("disk1", alloc_size);
        let volume1_id = VolumeUuid::new_v4();

        assert!(
            datastore
                .disk_region_allocate(
                    &opctx,
                    volume1_id,
                    &params.disk_source,
                    params.size,
                    &RegionAllocationStrategy::Random { seed: Some(0) },
                )
                .await
                .is_err()
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Validate that queries which should be executable without a full table
    // scan are, in fact, runnable without a FULL SCAN.
    #[tokio::test]
    async fn test_queries_do_not_require_full_table_scan() {
        use omicron_common::api::external;
        let logctx =
            dev::test_setup_log("test_queries_do_not_require_full_table_scan");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let explanation =
            DataStore::get_allocated_regions_query(VolumeUuid::nil())
                .explain_async(&conn)
                .await
                .unwrap();
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        let subnet = nexus_db_model::VpcSubnet::new(
            Uuid::nil(),
            Uuid::nil(),
            external::IdentityMetadataCreateParams {
                name: external::Name::try_from(String::from("name")).unwrap(),
                description: String::from("description"),
            },
            "172.30.0.0/22".parse().unwrap(),
            "fd00::/64".parse().unwrap(),
        );
        let query = InsertVpcSubnetQuery::new(subnet);
        println!("{}", diesel::debug_query(&query));
        let explanation = query.explain_async(&conn).await.unwrap();
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Test sled-specific IPv6 address allocation
    #[tokio::test]
    async fn test_sled_ipv6_address_allocation() {
        use std::net::Ipv6Addr;

        let logctx = dev::test_setup_log("test_sled_ipv6_address_allocation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let rack_id = Uuid::new_v4();
        let addr1 = "[fd00:1de::1]:12345".parse().unwrap();
        let sled1_id = "0de4b299-e0b4-46f0-d528-85de81a7095f".parse().unwrap();

        let sled1 = SledUpdateBuilder::new()
            .sled_id(sled1_id)
            .addr(addr1)
            .repo_depot_port(REPO_DEPOT_PORT)
            .rack_id(rack_id)
            .build();
        datastore.sled_upsert(sled1).await.unwrap();

        let addr2 = "[fd00:1df::1]:12345".parse().unwrap();
        let sled2_id = "66285c18-0c79-43e0-e54f-95271f271314".parse().unwrap();
        let sled2 = SledUpdateBuilder::new()
            .sled_id(sled2_id)
            .addr(addr2)
            .repo_depot_port(REPO_DEPOT_PORT)
            .rack_id(rack_id)
            .build();
        datastore.sled_upsert(sled2).await.unwrap();

        let ip = datastore.next_ipv6_address(&opctx, sled1_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(0xfd00, 0x1de, 0, 0, 0, 0, 1, 0);
        assert_eq!(ip, expected_ip);
        let ip = datastore.next_ipv6_address(&opctx, sled1_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(0xfd00, 0x1de, 0, 0, 0, 0, 1, 1);
        assert_eq!(ip, expected_ip);

        let ip = datastore.next_ipv6_address(&opctx, sled2_id).await.unwrap();
        let expected_ip = Ipv6Addr::new(0xfd00, 0x1df, 0, 0, 0, 0, 1, 0);
        assert_eq!(ip, expected_ip);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ssh_keys() {
        let logctx = dev::test_setup_log("test_ssh_keys");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a new Silo user so that we can lookup their keys.
        let authz_silo = authz::Silo::new(
            authz::FLEET,
            DEFAULT_SILO_ID,
            LookupType::ById(DEFAULT_SILO_ID),
        );
        let silo_user_id = SiloUserUuid::new_v4();
        datastore
            .silo_user_create(
                &authz_silo,
                SiloUserApiOnly::new(
                    authz_silo.id(),
                    silo_user_id,
                    "external@id".into(),
                )
                .into(),
            )
            .await
            .unwrap();

        let (.., authz_user) = LookupPath::new(&opctx, datastore)
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
        assert_eq!(created.silo_user_id(), ssh_key.silo_user_id());
        assert_eq!(created.public_key, ssh_key.public_key);

        // Lookup the key we just created.
        let (authz_silo, authz_silo_user, authz_ssh_key, found) =
            LookupPath::new(&opctx, datastore)
                .silo_user_id(silo_user_id)
                .ssh_key_name(&key_name.into())
                .fetch()
                .await
                .unwrap();
        assert_eq!(authz_silo.id(), DEFAULT_SILO_ID);
        assert_eq!(authz_silo_user.id(), silo_user_id);
        assert_eq!(found.silo_user_id(), ssh_key.silo_user_id());
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
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_rack_initialize_is_idempotent() {
        let logctx = dev::test_setup_log("test_rack_initialize_is_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_table_scan() {
        let logctx = dev::test_setup_log("test_table_scan");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_deallocate_external_ip_by_instance_id_is_idempotent() {
        use crate::db::model::IpKind;
        use nexus_db_schema::schema::external_ip::dsl;

        let logctx = dev::test_setup_log(
            "test_deallocate_external_ip_by_instance_id_is_idempotent",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Create a few records.
        let now = Utc::now();
        let instance_id = Uuid::new_v4();
        let kinds = [IpKind::SNat, IpKind::Ephemeral];
        let ips = (0..2)
            .map(|i| ExternalIp {
                id: Uuid::new_v4(),
                name: None,
                description: None,
                time_created: now,
                time_modified: now,
                time_deleted: None,
                ip_pool_id: Uuid::new_v4(),
                ip_pool_range_id: Uuid::new_v4(),
                project_id: None,
                is_service: false,
                parent_id: Some(instance_id),
                kind: kinds[i as usize],
                ip: ipnetwork::IpNetwork::from(IpAddr::from(Ipv4Addr::new(
                    10, 0, 0, i,
                ))),
                first_port: crate::db::model::SqlU16(0),
                last_port: crate::db::model::SqlU16(10),
                state: nexus_db_model::IpAttachState::Attached,
                is_probe: false,
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_deallocate_external_ip_is_idempotent() {
        use crate::db::model::IpKind;
        use nexus_db_schema::schema::external_ip::dsl;

        let logctx =
            dev::test_setup_log("test_deallocate_external_ip_is_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
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
            project_id: None,
            is_service: false,
            parent_id: Some(Uuid::new_v4()),
            kind: IpKind::SNat,
            ip: ipnetwork::IpNetwork::from(IpAddr::from(Ipv4Addr::new(
                10, 0, 0, 1,
            ))),
            first_port: crate::db::model::SqlU16(0),
            last_port: crate::db::model::SqlU16(10),
            state: nexus_db_model::IpAttachState::Attached,
            is_probe: false,
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
        assert!(
            datastore
                .deallocate_external_ip(&opctx, Uuid::nil())
                .await
                .is_err()
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_external_ip_check_constraints() {
        use crate::db::model::IpKind;
        use diesel::result::DatabaseErrorKind::CheckViolation;
        use diesel::result::DatabaseErrorKind::UniqueViolation;
        use diesel::result::Error::DatabaseError;
        use nexus_db_schema::schema::external_ip::dsl;

        let logctx = dev::test_setup_log("test_external_ip_check_constraints");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
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
            project_id: None,
            is_service: false,
            parent_id: Some(Uuid::new_v4()),
            kind: IpKind::Floating,
            ip: addresses.next().unwrap().into(),
            first_port: crate::db::model::SqlU16(0),
            last_port: crate::db::model::SqlU16(10),
            state: nexus_db_model::IpAttachState::Attached,
            is_probe: false,
        };

        // Combinations of NULL and non-NULL for:
        // - name
        // - description
        // - parent (instance / service) UUID
        // - project UUID
        // - attach state
        let names = [None, Some("foo")];
        let descriptions = [None, Some("foo".to_string())];
        let parent_ids = [None, Some(Uuid::new_v4())];
        let project_ids = [None, Some(Uuid::new_v4())];

        let mut seen_pairs = HashSet::new();

        // For Floating IPs, both name and description must be non-NULL
        // If they are instance FIPs, they *must* have a project id.
        for (
            name,
            description,
            parent_id,
            is_service,
            project_id,
            modify_name,
        ) in itertools::iproduct!(
            &names,
            &descriptions,
            &parent_ids,
            [false, true],
            &project_ids,
            [false, true]
        ) {
            // Both choices of parent_id are valid, so we need a unique name for each.
            let name_local = name.map(|v| {
                let name = if modify_name {
                    v.to_string()
                } else {
                    format!("{v}-with-parent")
                };
                nexus_db_model::Name(Name::try_from(name).unwrap())
            });

            // We do name duplicate checking on the `Some` branch, don't steal the
            // name intended for another floating IP.
            if parent_id.is_none() && modify_name {
                continue;
            }

            let state = if parent_id.is_some() {
                IpAttachState::Attached
            } else {
                IpAttachState::Detached
            };

            let new_ip = ExternalIp {
                id: Uuid::new_v4(),
                name: name_local.clone(),
                description: description.clone(),
                ip: addresses.next().unwrap().into(),
                is_service,
                parent_id: *parent_id,
                project_id: *project_id,
                state,
                ..ip
            };

            let key = (*project_id, name_local);

            let res = diesel::insert_into(dsl::external_ip)
                .values(new_ip)
                .execute_async(&*conn)
                .await;

            let project_as_expected = (is_service && project_id.is_none())
                || (!is_service && project_id.is_some());

            let valid_expression =
                name.is_some() && description.is_some() && project_as_expected;
            let name_exists = seen_pairs.contains(&key);

            if valid_expression && !name_exists {
                // Name/description must be non-NULL, instance ID can be
                // either
                // Names must be unique at fleet level and at project level.
                // Project must be NULL if service, non-NULL if instance.
                res.unwrap_or_else(|e| {
                    panic!(
                        "Failed to insert Floating IP with valid \
                         name, description, project ID, and {} ID:\
                         {name:?} {description:?} {project_id:?} {:?}\n{e}",
                        if is_service { "Service" } else { "Instance" },
                        &ip.parent_id
                    )
                });

                seen_pairs.insert(key);
            } else if !valid_expression {
                // Several permutations are invalid and we want to detect them all.
                // NOTE: CHECK violation will supersede UNIQUE violation below.
                let err = res.expect_err(
                    "Expected a CHECK violation when inserting a \
                     Floating IP record with NULL name and/or description, \
                     and incorrect project parent relation",
                );
                assert!(
                    matches!(err, DatabaseError(CheckViolation, _)),
                    "Expected a CHECK violation when inserting a \
                     Floating IP record with NULL name and/or description, \
                     and incorrect project parent relation",
                );
            } else {
                let err = res.expect_err(
                    "Expected a UNIQUE violation when inserting a \
                     Floating IP record with existing (name, project_id)",
                );
                assert!(
                    matches!(err, DatabaseError(UniqueViolation, _)),
                    "Expected a UNIQUE violation when inserting a \
                     Floating IP record with existing (name, project_id)",
                );
            }
        }

        // For other IP types: name, description and project must be NULL
        for (kind, name, description, parent_id, is_service, project_id) in itertools::iproduct!(
            [IpKind::SNat, IpKind::Ephemeral],
            &names,
            &descriptions,
            &parent_ids,
            [false, true],
            &project_ids
        ) {
            let name_local = name.map(|v| {
                nexus_db_model::Name(Name::try_from(v.to_string()).unwrap())
            });
            let state = if parent_id.is_some() {
                IpAttachState::Attached
            } else {
                IpAttachState::Detached
            };
            let new_ip = ExternalIp {
                id: Uuid::new_v4(),
                name: name_local,
                description: description.clone(),
                kind,
                ip: addresses.next().unwrap().into(),
                is_service,
                parent_id: *parent_id,
                project_id: *project_id,
                state,
                ..ip
            };
            let res = diesel::insert_into(dsl::external_ip)
                .values(new_ip.clone())
                .execute_async(&*conn)
                .await;
            let ip_type = if is_service { "Service" } else { "Instance" };
            let null_snat_parent = parent_id.is_none() && kind == IpKind::SNat;
            if name.is_none()
                && description.is_none()
                && !null_snat_parent
                && project_id.is_none()
            {
                // Name/description must be NULL, instance ID cannot
                // be NULL.

                if kind == IpKind::Ephemeral && is_service {
                    // Ephemeral Service IPs aren't supported.
                    let err = res.unwrap_err();
                    assert!(
                        matches!(err, DatabaseError(CheckViolation, _)),
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
                    matches!(err, DatabaseError(CheckViolation, _)),
                    "Expected a CHECK violation when inserting a \
                     {:?} IP record with non-NULL name, description, \
                     and/or {} ID",
                    kind,
                    ip_type,
                );
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
