/*!
 * Rust types that describe the control plane database schema
 *
 * This includes the control-plane-specific impls for the generic facilities in
 * ./sql.rs.
 */

use crate::api_error::ApiError;
use crate::api_model::ApiDisk;
use crate::api_model::ApiInstance;
use crate::api_model::ApiName;
use crate::api_model::ApiProject;
use crate::api_model::ApiResourceType;
use uuid::Uuid;

use super::sql::LookupKey;
use super::sql::Table;

/** Describes the "Project" table */
pub struct Project;
impl Table for Project {
    type ModelType = ApiProject;
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Project;
    const TABLE_NAME: &'static str = "Project";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_metadata_updated",
        "time_deleted",
    ];
}

/** Describes the "Instance" table */
pub struct Instance;
impl Table for Instance {
    type ModelType = ApiInstance;
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Instance;
    const TABLE_NAME: &'static str = "Instance";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_metadata_updated",
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

/** Describes the "Disk" table */
pub struct Disk;
impl Table for Disk {
    type ModelType = ApiDisk;
    const RESOURCE_TYPE: ApiResourceType = ApiResourceType::Disk;
    const TABLE_NAME: &'static str = "Disk";
    const ALL_COLUMNS: &'static [&'static str] = &[
        "id",
        "name",
        "description",
        "time_created",
        "time_metadata_updated",
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

#[cfg(test)]
mod test {
    use super::Disk;
    use super::Instance;
    use super::Project;
    use super::Table;
    use crate::dev;
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
        eprintln!("missing from database:    {}", list_extra.join(", "));
        eprintln!("missing from Rust:        {}", list_missing.join(", "));

        if !list_missing.is_empty() || !list_extra.is_empty() {
            panic!(
                "mismatch between columns in database schema and those defined \
                in Rust code for table {:?}",
                T::TABLE_NAME
            );
        }
    }
}

/**
 * Implementation of [`LookupKey`] for looking up objects by their universally
 * unique id
 */
pub struct LookupByUniqueId;
impl<'a> LookupKey<'a> for LookupByUniqueId {
    type ScopeKey = ();
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] = &[];
    type ItemKey = Uuid;
    const ITEM_KEY_COLUMN_NAME: &'static str = "id";

    fn where_select_error<T: Table>(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> ApiError {
        ApiError::not_found_by_id(T::RESOURCE_TYPE, item_key)
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
impl<'a> LookupKey<'a> for LookupByUniqueName {
    type ScopeKey = ();
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] = &[];
    type ItemKey = ApiName;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error<T: Table>(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> ApiError {
        ApiError::not_found_by_name(T::RESOURCE_TYPE, item_key)
    }
}

/**
 * Implementation of [`LookupKey`] for looking up objects within a project by
 * the project_id and the object's name
 */
pub struct LookupByUniqueNameInProject;
impl<'a> LookupKey<'a> for LookupByUniqueNameInProject {
    type ScopeKey = (&'a Uuid,);
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str] = &["project_id"];
    type ItemKey = ApiName;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error<T: Table>(
        _scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> ApiError {
        ApiError::not_found_by_name(T::RESOURCE_TYPE, item_key)
    }
}

/**
 * Implementation of [`LookupKey`] for looking up objects by name within the
 * scope of an instance (SQL column "attach_instance_id").  This is really just
 * intended for finding attached disks.
 */
pub struct LookupByAttachedInstance;
impl<'a> LookupKey<'a> for LookupByAttachedInstance {
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

    fn where_select_error<T: Table>(
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
