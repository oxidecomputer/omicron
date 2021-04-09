/*!
 * Rust types that describe the database schema
 */

use crate::api_error::ApiError;
use crate::api_model::ApiDisk;
use crate::api_model::ApiInstance;
use crate::api_model::ApiName;
use crate::api_model::ApiProject;
use crate::api_model::ApiResourceType;
use crate::api_model::DataPageParams;
use std::convert::TryFrom;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::operations::SqlString;

/** Describes a table in the control plane database */
/*
 * TODO-design We want to find a better way to abstract this.  Diesel provides a
 * compelling model in terms of using it.  But it also seems fairly heavyweight
 * and seems to tightly couple the application to the current database schema.
 * This pattern of fetch-or-insert all-fields-of-an-object likely _isn't_ our
 * most common use case, even though we do it a lot for basic CRUD.
 * TODO-robustness it would also be great if this were generated from
 * src/sql/dbinit.sql or vice versa or at least if there were some way to keep
 * them in sync.
 */
pub trait Table {
    /** Struct that represents rows of this table when the full row is needed */
    /* TODO-cleanup what does the 'static actually mean here? */
    type ApiModelType: for<'a> TryFrom<&'a tokio_postgres::Row, Error = ApiError>
        + Send
        + 'static;
    /** [`ApiResourceType`] that corresponds to rows of this table */
    const RESOURCE_TYPE: ApiResourceType;
    /** Name of the table */
    const TABLE_NAME: &'static str;
    /** List of names of all columns in the table. */
    const ALL_COLUMNS: &'static [&'static str];

    /**
     * Parts of a WHERE clause that should be included in all queries for live
     * records
     */
    const LIVE_CONDITIONS: &'static str = "time_deleted IS NULL";
}

/** Describes the "Project" table */
pub struct Project;
impl Table for Project {
    type ApiModelType = ApiProject;
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
    type ApiModelType = ApiInstance;
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
    type ApiModelType = ApiDisk;
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
 * Used to generate WHERE clauses for individual row lookups and paginated scans
 */
pub trait LookupKey<'a> {
    /*
     * Items defined by the various impls of this trait
     */

    type ScopeParams: IntoToSqlVec<'a> + 'a + Clone + Copy;
    const SCOPE_PARAMS_COLUMN_NAMES: &'static [&'static str];
    type ItemKey: ToSql + Sync + Clone + 'static;
    const ITEM_KEY_COLUMN_NAME: &'static str;

    /** Returns an error for the case where no item was found */
    fn where_select_error<T: Table>(
        scope_params: Self::ScopeParams,
        item_key: &Self::ItemKey,
    ) -> ApiError;

    /*
     * The rest of this trait provides common implementation for all impls in
     * terms of the above items.
     */

    /** Returns a WHERE clause for selecting specific row(s) */
    fn where_select_rows<'b>(
        scope_params: Self::ScopeParams,
        item_key: &'a Self::ItemKey,
        output: &'b mut SqlString<'a>,
    ) where
        'a: 'b,
    {
        let mut column_names =
            Vec::with_capacity(Self::SCOPE_PARAMS_COLUMN_NAMES.len() + 1);
        column_names.extend_from_slice(&Self::SCOPE_PARAMS_COLUMN_NAMES);
        column_names.push(Self::ITEM_KEY_COLUMN_NAME);
        let mut param_values = scope_params.to_sql_vec();
        param_values.push(item_key);
        where_cond(&column_names, param_values, "=", output);
    }

    /**
     * Returns a WHERE clause for selecting a page of rows
     *
     * A "page" here is a sequence of rows according to some sort order, as in
     * API pagination.  Callers of this function specify the page by specifying
     * values from the last row they saw, not knowing exactly which row(s) are
     * next.  By contrast, [`where_select_rows`] (besides usually being used
     * to select a only single row) does not assume anything about the ordering
     * and callers specify rows by specific values.
     */
    fn where_select_page<'b, 'c, 'd>(
        scope_params: Self::ScopeParams,
        pagparams: &'c DataPageParams<'c, Self::ItemKey>,
        output: &'b mut SqlString<'d>,
    ) where
        'a: 'b + 'd,
        'c: 'a,
    {
        let (operator, order) = match pagparams.direction {
            dropshot::PaginationOrder::Ascending => (">", "ASC"),
            dropshot::PaginationOrder::Descending => ("<", "DESC"),
        };

        /*
         * First, generate the conditions that are true for every page.  For
         * example, when listing Instances in a Project, this would specify the
         * project_id.
         */
        let fixed_column_names = Self::SCOPE_PARAMS_COLUMN_NAMES;
        if fixed_column_names.len() > 0 {
            let fixed_param_values = scope_params.to_sql_vec();
            output.push_str(" AND (");
            where_cond(fixed_column_names, fixed_param_values, "=", output);
            output.push_str(") ");
        }

        /*
         * If a marker was provided, then generate conditions that resume the
         * scan after the marker value.
         */
        if let Some(marker) = pagparams.marker {
            let var_column_names = &[Self::ITEM_KEY_COLUMN_NAME];
            let marker_ref = marker as &(dyn ToSql + Sync);
            let var_param_values = vec![marker_ref];
            output.push_str(" AND (");
            where_cond(var_column_names, var_param_values, operator, output);
            output.push_str(") ");
        }

        /*
         * Generate the ORDER BY clause based on the columns that make up the
         * marker.
         */
        output.push_str(&format!(
            " ORDER BY {} {} ",
            Self::ITEM_KEY_COLUMN_NAME,
            order,
        ));
    }
}

/* TODO-coverage TODO-doc */
fn where_cond<'a, 'b>(
    column_names: &'b [&'static str],
    param_values: Vec<&'a (dyn ToSql + Sync)>,
    operator: &'static str,
    output: &'b mut SqlString<'a>,
) where
    'a: 'b,
{
    assert_eq!(param_values.len(), column_names.len());
    let conditions = column_names
        .iter()
        .zip(param_values)
        .map(|(name, value): (&&'static str, &(dyn ToSql + Sync))| {
            format!("({} {} {})", *name, operator, output.next_param(value))
        })
        .collect::<Vec<String>>()
        .join(" AND ");
    output.push_str(format!("( {} )", conditions).as_str());
}

// XXX document all this

pub struct LookupByUniqueId;
impl<'a> LookupKey<'a> for LookupByUniqueId {
    type ScopeParams = ();
    const SCOPE_PARAMS_COLUMN_NAMES: &'static [&'static str] = &[];
    type ItemKey = Uuid;
    const ITEM_KEY_COLUMN_NAME: &'static str = "id";

    fn where_select_error<T: Table>(
        _scope_params: Self::ScopeParams,
        item_key: &Self::ItemKey,
    ) -> ApiError {
        ApiError::not_found_by_id(T::RESOURCE_TYPE, item_key)
    }
}

pub struct LookupByUniqueName;
impl<'a> LookupKey<'a> for LookupByUniqueName {
    type ScopeParams = ();
    const SCOPE_PARAMS_COLUMN_NAMES: &'static [&'static str] = &[];
    type ItemKey = ApiName;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error<T: Table>(
        _scope_params: Self::ScopeParams,
        item_key: &Self::ItemKey,
    ) -> ApiError {
        ApiError::not_found_by_name(T::RESOURCE_TYPE, item_key)
    }
}

pub struct LookupByUniqueNameInProject;
impl<'a> LookupKey<'a> for LookupByUniqueNameInProject {
    type ScopeParams = (&'a Uuid,);
    const SCOPE_PARAMS_COLUMN_NAMES: &'static [&'static str] = &["project_id"];
    type ItemKey = ApiName;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error<T: Table>(
        _scope_params: Self::ScopeParams,
        item_key: &Self::ItemKey,
    ) -> ApiError {
        ApiError::not_found_by_name(T::RESOURCE_TYPE, item_key)
    }
}

pub struct LookupByAttachedInstance;
impl<'a> LookupKey<'a> for LookupByAttachedInstance {
    /*
     * TODO-design What if we want an additional filter here (like disk_state in
     * ('attaching', 'attached', 'detaching'))?  This would almost work using
     * the fixed columns except that we cannot change the operator or supply
     * multiple values.
     */
    type ScopeParams = (&'a Uuid,);
    const SCOPE_PARAMS_COLUMN_NAMES: &'static [&'static str] =
        &["attach_instance_id"];
    type ItemKey = ApiName;
    const ITEM_KEY_COLUMN_NAME: &'static str = "name";

    fn where_select_error<T: Table>(
        _scope_params: Self::ScopeParams,
        _item_key: &Self::ItemKey,
    ) -> ApiError {
        /*
         * This is not a supported API operation, so we do not have an
         * appropriate NotFound error.
         */
        ApiError::internal_error("attempted lookup attached instance")
    }
}

pub trait IntoToSqlVec<'a> {
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)>;
}

impl<'a> IntoToSqlVec<'a> for () {
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)> {
        Vec::new()
    }
}

impl<'a, 't1, T1> IntoToSqlVec<'a> for (&'t1 T1,)
where
    T1: ToSql + Sync,
    't1: 'a,
{
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)> {
        vec![self.0]
    }
}

impl<'a, 't1, 't2, T1, T2> IntoToSqlVec<'a> for (&'t1 T1, &'t2 T2)
where
    T1: ToSql + Sync,
    't1: 'a,
    T2: ToSql + Sync,
    't2: 'a,
{
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)> {
        vec![self.0, self.1]
    }
}
