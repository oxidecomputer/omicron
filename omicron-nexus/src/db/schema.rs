/*!
 * Rust types that describe the control plane database schema
 *
 * This includes the control-plane-specific impls for the generic facilities in
 * ./sql.rs.
 */

use crate::sec;
use omicron_common::error::ApiError;
use omicron_common::model::ApiDisk;
use omicron_common::model::ApiInstance;
use omicron_common::model::ApiName;
use omicron_common::model::ApiProject;
use omicron_common::model::ApiResourceType;
use uuid::Uuid;

use super::sql::LookupKey;
use super::sql::ResourceTable;
use super::sql::Table;

/** Describes the "Project" table */
pub struct Project;
impl Table for Project {
    type ModelType = ApiProject;
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
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Project;
}

/** Describes the "Instance" table */
pub struct Instance;
impl Table for Instance {
    type ModelType = ApiInstance;
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
        "reboot_in_progress",
        "time_state_updated",
        "state_generation",
        "active_server_id",
        "ncpus",
        "memory",
        "hostname",
    ];
}

impl ResourceTable for Instance {
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Instance;
}

/** Describes the "Disk" table */
pub struct Disk;
impl Table for Disk {
    type ModelType = ApiDisk;
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
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Disk;
}

/** Describes the "SagaNodeEvent" table */
pub struct Saga;
impl Table for Saga {
    type ModelType = sec::log::Saga;
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

/** Describes the "Saga" table */
pub struct SagaNodeEvent;
impl Table for SagaNodeEvent {
    type ModelType = sec::log::SagaNodeEventDeserializer;
    const TABLE_NAME: &'static str = "SagaNodeEvent";
    const ALL_COLUMNS: &'static [&'static str] =
        &["saga_id", "node_id", "event_type", "data", "event_time", "creator"];
    const LIVE_CONDITIONS: &'static str = "TRUE";
}

#[cfg(test)]
mod test {
    use super::Disk;
    use super::Instance;
    use super::Project;
    use super::Saga;
    use super::SagaNodeEvent;
    use super::Table;
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
    ) -> ApiError {
        ApiError::not_found_by_id(R::RESOURCE_TYPE, item_key)
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
    ) -> ApiError {
        ApiError::internal_error(&format!(
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
    type ItemKey = ApiName;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> ApiError {
        ApiError::not_found_by_name(R::RESOURCE_TYPE, item_key)
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
    type ItemKey = ApiName;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> ApiError {
        ApiError::not_found_by_name(R::RESOURCE_TYPE, item_key)
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
    type ItemKey = ApiName;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error(
        _scope_key: Self::ScopeKey,
        _item_key: &Self::ItemKey,
    ) -> ApiError {
        /*
         * This is not a supported API operation, so we do not have an
         * appropriate NotFound error.
         */
        ApiError::internal_error("attempted lookup attached instance")
    }
}
