/*!
 * Rust types that describe the control plane database schema
 *
 * This includes the control-plane-specific impls for the generic facilities in
 * ./sql.rs.
 */

use omicron_common::api;
use omicron_common::api::external::Error;
use omicron_common::api::external::Name;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

use super::sql::LookupKey;
use super::sql::ResourceTable;
use super::sql::Table;
use crate::db;

/** Describes the "Project" table */
pub struct Project;
impl Table for Project {
    type ModelType = api::internal::nexus::Project;
    const TABLE_NAME: &'static str = "Project";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_modified",
        "time_deleted",
    ];
}

impl ResourceTable for Project {
    const RESOURCE_TYPE: ResourceType = ResourceType::Project;
}

/** Describes the "Instance" table */
pub struct Instance;
impl Table for Instance {
    type ModelType = api::internal::nexus::Instance;
    const TABLE_NAME: &'static str = "Instance";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_modified",
        "time_deleted",
        "project_id",
        "instance_state",
        "time_state_updated",
        "state_generation",
        "active_server_id",
        "ncpus",
        "memory",
        "hostname",
    ];
}

impl ResourceTable for Instance {
    const RESOURCE_TYPE: ResourceType = ResourceType::Instance;
}

/** Describes the "Disk" table */
pub struct Disk;
impl Table for Disk {
    type ModelType = api::internal::nexus::Disk;
    const TABLE_NAME: &'static str = "Disk";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_modified",
        "time_deleted",
        "project_id",
        "disk_state",
        "time_state_updated",
        "state_generation",
        "attach_instance_id",
        "size_bytes",
        "origin_snapshot",
    ];
}

impl ResourceTable for Disk {
    const RESOURCE_TYPE: ResourceType = ResourceType::Disk;
}

/** Describes the "Saga" table */
pub struct Saga;
impl Table for Saga {
    type ModelType = db::saga_types::Saga;
    const TABLE_NAME: &'static str = "Saga";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "creator",
        "template_name",
        "time_created",
        "saga_params",
        "saga_state",
        "current_sec",
        "adopt_generation",
        "adopt_time",
    ];
    const LIVE_CONDITIONS: &'static str = "TRUE";
}

/** Describes the "SagaNodeEvent" table */
pub struct SagaNodeEvent;
impl Table for SagaNodeEvent {
    type ModelType = db::saga_types::SagaNodeEvent;
    const TABLE_NAME: &'static str = "SagaNodeEvent";
    const ALL_COLUMNS: &'static [&'static str] =
        &["saga_id", "node_id", "event_type", "data", "event_time", "creator"];
    const LIVE_CONDITIONS: &'static str = "TRUE";
}

/** Describes the "Oximeter" table */
pub struct Oximeter;
impl Table for Oximeter {
    type ModelType = api::internal::nexus::OximeterInfo;
    const TABLE_NAME: &'static str = "Oximeter";
    const ALL_COLUMNS: &'static [&'static str] =
        &["id", "time_created", "time_modified", "ip", "port"];
}

/** Describes the "MetricProducer" table */
pub struct MetricProducer;
impl Table for MetricProducer {
    type ModelType = api::internal::nexus::ProducerEndpoint;
    const TABLE_NAME: &'static str = "MetricProducer";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "time_created",
        "time_modified",
        "ip",
        "port",
        "interval",
        "route",
    ];
}

/** Describes the "OximeterAssignment" table */
pub struct OximeterAssignment;
impl Table for OximeterAssignment {
    type ModelType = api::internal::nexus::OximeterAssignment;
    const TABLE_NAME: &'static str = "OximeterAssignment";
    const ALL_COLUMNS: &'static [&'static str] =
        &["oximeter_id", "producer_id", "time_created"];
}

/** Describes the "VPC" table */
pub struct VPC;
impl Table for VPC {
    type ModelType = api::external::VPC;
    const TABLE_NAME: &'static str = "VPC";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_modified",
        "time_deleted",
        "project_id",
    ];
}

/** Describes the "VPCSubnet" table */
pub struct VPCSubnet;
impl Table for VPCSubnet {
    type ModelType = api::external::VPCSubnet;
    const TABLE_NAME: &'static str = "VPCSubnet";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_modified",
        "time_deleted",
        "vpc_id",
        "ipv4_block",
        "ipv6_block",
    ];
}

/** Describes the "NetworkInterface" table */
pub struct NetworkInterface;
impl Table for NetworkInterface {
    type ModelType = api::external::NetworkInterface;
    const TABLE_NAME: &'static str = "NetworkInterface";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_modified",
        "time_deleted",
        "vpc_id",
        "subnet_id",
        "mac",
        "ip",
    ];
}

#[cfg(test)]
mod test {
    use super::Disk;
    use super::Instance;
    use super::Project;
    use super::Saga;
    use super::SagaNodeEvent;
    use super::Table;
    use super::{MetricProducer, Oximeter, OximeterAssignment};
    use super::{NetworkInterface, VPCSubnet, VPC};
    use omicron_common::dev;
    use std::collections::BTreeSet;
    use tokio_postgres::types::ToSql;

    /*
     * Check the Rust descriptions of the database schema against what's in a
     * newly-populated database.  This is simplistic for lots of reasons: it
     * doesn't account for multiple versions of the schema, schema migrations,
     * types, and any number of other complexities.  It still seems like a
     * useful sanity check.
     */
    #[tokio::test]
    async fn test_schemas() {
        let logctx = dev::test_setup_log("test_schemas").await;
        let mut database = dev::test_setup_database(&logctx.log).await;
        let client = database
            .connect()
            .await
            .expect("failed to connect to test database");

        check_table_schema::<Project>(&client).await;
        check_table_schema::<Disk>(&client).await;
        check_table_schema::<Instance>(&client).await;
        check_table_schema::<Saga>(&client).await;
        check_table_schema::<SagaNodeEvent>(&client).await;
        check_table_schema::<Oximeter>(&client).await;
        check_table_schema::<MetricProducer>(&client).await;
        check_table_schema::<OximeterAssignment>(&client).await;
        check_table_schema::<VPC>(&client).await;
        check_table_schema::<VPCSubnet>(&client).await;
        check_table_schema::<NetworkInterface>(&client).await;

        database.cleanup().await.expect("failed to clean up database");
        logctx.cleanup_successful();
    }

    async fn check_table_schema<T: Table>(c: &tokio_postgres::Client) {
        let sql = "SELECT column_name FROM information_schema.columns \
            WHERE table_catalog = $1 AND lower(table_name) = lower($2)";
        let sql_params: Vec<&(dyn ToSql + Sync)> =
            vec![&"omicron", &T::TABLE_NAME];
        let rows = c
            .query(sql, &sql_params)
            .await
            .expect("failed to query information_schema");

        if rows.is_empty() {
            panic!(
                "querying information_schema: found no rows \
                (sql = {:?}, $1 = {:?}, $2 = {:?})",
                sql, sql_params[0], sql_params[1],
            );
        }

        let set_expected = T::ALL_COLUMNS
            .iter()
            .cloned()
            .map(str::to_owned)
            .collect::<BTreeSet<String>>();
        let expected =
            set_expected.iter().cloned().collect::<Vec<String>>().join(", ");
        let set_found = rows
            .iter()
            .map(|r| {
                r.try_get::<'_, _, String>("column_name")
                    .expect("missing \"column_name\"")
            })
            .collect::<BTreeSet<String>>();
        let found =
            set_found.iter().cloned().collect::<Vec<String>>().join(", ");
        let list_missing = set_expected
            .difference(&set_found)
            .cloned()
            .collect::<Vec<String>>();
        let list_extra = set_found
            .difference(&set_expected)
            .cloned()
            .collect::<Vec<String>>();

        eprintln!("TABLE: {}", T::TABLE_NAME);
        eprintln!("found in database:        {}", expected);
        eprintln!("found in Rust definition: {}", found);
        eprintln!("missing from Rust:        {}", list_extra.join(", "));
        eprintln!("missing from database:    {}", list_missing.join(", "));

        if !list_missing.is_empty() || !list_extra.is_empty() {
            panic!(
                "mismatch between columns in database schema and those defined \
                in Rust code for table {:?}",
                T::TABLE_NAME
            );
        } else {
            eprintln!("TABLE {} Rust and database fields match", T::TABLE_NAME);
        }
    }
}

/**
 * Implementation of [`LookupKey`] for looking up objects by their universally
 * unique id
 */
pub struct LookupByUniqueId;
impl<'a, R: ResourceTable> LookupKey<'a, R> for LookupByUniqueId {
    type ScopeKey = ();
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] = &[];
    type ItemKey = Uuid;
    const ITEM_KEY_COLUMN_NAME: &'static str = "id";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> Error {
        Error::not_found_by_id(R::RESOURCE_TYPE, item_key)
    }
}

/**
 * Like [`LookupByUniqueId`], but for objects that are not API resources (so
 * that "not found" error is different).
 */
pub struct LookupGenericByUniqueId;
impl<'a, T: Table> LookupKey<'a, T> for LookupGenericByUniqueId {
    type ScopeKey = ();
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] = &[];
    type ItemKey = Uuid;
    const ITEM_KEY_COLUMN_NAME: &'static str = "id";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> Error {
        Error::internal_error(&format!(
            "table {:?}: expected row with id {:?}, but found none",
            T::TABLE_NAME,
            item_key,
        ))
    }
}

/**
 * Implementation of [`LookupKey`] for looking up objects by just their name
 *
 * This generally assumes that the name is unique within an instance of the
 * control plane.  (You could also use this to look up all objects of a certain
 * type having a given name, but it's not clear that would be useful.)
 */
pub struct LookupByUniqueName;
impl<'a, R: ResourceTable> LookupKey<'a, R> for LookupByUniqueName {
    type ScopeKey = ();
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] = &[];
    type ItemKey = Name;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> Error {
        Error::not_found_by_name(R::RESOURCE_TYPE, item_key)
    }
}

/**
 * Implementation of [`LookupKey`] for looking up objects within a project by
 * the project_id and the object's name
 */
pub struct LookupByUniqueNameInProject;
impl<'a, R: ResourceTable> LookupKey<'a, R> for LookupByUniqueNameInProject {
    type ScopeKey = (&'a Uuid,);
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] = &["project_id"];
    type ItemKey = Name;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> Error {
        Error::not_found_by_name(R::RESOURCE_TYPE, item_key)
    }
}

/**
 * Implementation of [`LookupKey`] for looking up objects by name within the
 * scope of an instance (SQL column "attach_instance_id").  This is really just
 * intended for finding attached disks.
 */
pub struct LookupByAttachedInstance;
impl<'a, R: ResourceTable> LookupKey<'a, R> for LookupByAttachedInstance {
    /*
     * TODO-design What if we want an additional filter here (like disk_state in
     * ('attaching', 'attached', 'detaching'))?  This would almost work using
     * the fixed columns except that we cannot change the operator or supply
     * multiple values.
     */
    type ScopeKey = (&'a Uuid,);
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] =
        &["attach_instance_id"];
    type ItemKey = Name;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        _item_key: &Self::ItemKey,
    ) -> Error {
        /*
         * This is not a supported API operation, so we do not have an
         * appropriate NotFound error.
         */
        Error::internal_error("attempted lookup attached instance")
    }
}

/**
 * Implementation of [`LookupKey`] specifically for listing pages from the
 * "SagaNodeEvent" table.  We're always filtering on a specific `saga_id` and
 * paginating by `node_id`.
 */
pub struct LookupSagaNodeEvent;
impl<'a> LookupKey<'a, SagaNodeEvent> for LookupSagaNodeEvent {
    type ScopeKey = (&'a Uuid,);
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] = &["saga_id"];
    type ItemKey = i64;
    const ITEM_KEY_COLUMN_NAME: &'static str = "node_id";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        _item_key: &Self::ItemKey,
    ) -> Error {
        /* There's no reason to ever use this function. */
        Error::internal_error(
            "unexpected lookup for saga node unexpectedly found no rows",
        )
    }
}
