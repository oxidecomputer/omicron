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
use std::borrow::Borrow;
use std::convert::TryFrom;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::operations::SqlString;

/**
 * Used to generate WHERE clauses for individual row lookups and paginated scans
 */
pub trait LookupKey<'a> {
    /* Direct lookup */

    type LookupParams: IntoToSqlVec<'a> + Clone + Copy;
    const LOOKUP_COLUMN_NAMES: &'static [&'static str];

    /** Returns a WHERE clause for selecting specific row(s) */
    fn where_select_rows<'b>(
        lookup_params: Self::LookupParams,
        output: &'b mut SqlString<'a>,
    ) where
        'a: 'b,
    {
        let column_names = Self::LOOKUP_COLUMN_NAMES;
        let param_values = lookup_params.to_sql_vec();
        where_cond(column_names, param_values, "=", output);
    }

    /** Returns an error for the case where no item was found */
    fn where_select_error<T: Table>(params: Self::LookupParams) -> ApiError;

    /* Pagination */

    type PageParamsFixed: IntoToSqlVec<'a>;
    const PAGE_FIXED_COLUMN_NAMES: &'static [&'static str];
    type PageParamsMarker: ToSql + Sync + Clone + 'static;
    const PAGE_MARKER_COLUMN_NAMES: &'static [&'static str];

    /** Returns a WHERE clause for selecting a page of rows */
    fn where_select_page<'b, 'c, 'd>(
        page_params_fixed: Self::PageParamsFixed,
        pagparams: &'c DataPageParams<'c, Self::PageParamsMarker>,
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
        let fixed_column_names = Self::PAGE_FIXED_COLUMN_NAMES;
        let fixed_param_values = page_params_fixed.to_sql_vec();
        where_cond(fixed_column_names, fixed_param_values, "=", output);

        /*
         * If a marker was provided, then generate conditions that resume the
         * scan after the marker value.
         */
        if let Some(marker) = pagparams.marker {
            let var_column_names = Self::PAGE_MARKER_COLUMN_NAMES;
            let marker_ref = marker as &(dyn ToSql + Sync);
            let var_param_values = vec![marker_ref];
            output.push_str(" AND ");
            where_cond(var_column_names, var_param_values, operator, output);
        }

        /*
         * Generate the ORDER BY clause based on all of the columns that make up
         * the marker.
         */
        let order_clauses = Self::PAGE_MARKER_COLUMN_NAMES
            .iter()
            .map(|column_name: &&'static str| {
                format!("{} {}", output.next_param(column_name), order)
            })
            .collect::<Vec<String>>()
            .join(", ");
        output.push_str("ORDER BY ");
        output.push_str(&order_clauses);
        output.push_str(" ");
    }
}

/* TODO-coverage TODO-doc */
fn where_cond<'a, 'b>(
    column_names: &'static [&'static str],
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
    type LookupParams = (&'a Uuid,);
    const LOOKUP_COLUMN_NAMES: &'static [&'static str] = &["id"];
    fn where_select_error<T: Table>(
        lookup_params: Self::LookupParams,
    ) -> ApiError {
        ApiError::not_found_by_id(T::RESOURCE_TYPE, lookup_params.0)
    }

    type PageParamsFixed = ();
    const PAGE_FIXED_COLUMN_NAMES: &'static [&'static str] = &[];
    type PageParamsMarker = Uuid;
    const PAGE_MARKER_COLUMN_NAMES: &'static [&'static str] = &["id"];
}

pub struct LookupByUniqueName;
impl<'a> LookupKey<'a> for LookupByUniqueName {
    type LookupParams = (&'a ApiName,);
    const LOOKUP_COLUMN_NAMES: &'static [&'static str] = &["name"];
    fn where_select_error<T: Table>(
        lookup_params: Self::LookupParams,
    ) -> ApiError {
        ApiError::not_found_by_name(T::RESOURCE_TYPE, lookup_params.0)
    }

    type PageParamsFixed = ();
    const PAGE_FIXED_COLUMN_NAMES: &'static [&'static str] = &[];
    type PageParamsMarker = ApiName;
    const PAGE_MARKER_COLUMN_NAMES: &'static [&'static str] = &["name"];
}

pub struct LookupByUniqueNameInProject;
impl<'a> LookupKey<'a> for LookupByUniqueNameInProject {
    type LookupParams = (&'a Uuid, &'a ApiName);
    const LOOKUP_COLUMN_NAMES: &'static [&'static str] =
        &["project_id", "name"];
    fn where_select_error<T: Table>(
        lookup_params: Self::LookupParams,
    ) -> ApiError {
        ApiError::not_found_by_name(T::RESOURCE_TYPE, lookup_params.1)
    }

    type PageParamsFixed = (&'a Uuid,);
    const PAGE_FIXED_COLUMN_NAMES: &'static [&'static str] = &["project_id"];
    type PageParamsMarker = ApiName;
    const PAGE_MARKER_COLUMN_NAMES: &'static [&'static str] = &["name"];
}

pub trait IntoToSqlVec<'a> {
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)>;
}

impl<'a> IntoToSqlVec<'a> for () {
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)> {
        Vec::new()
    }
}

// impl<'a, T: ToSql + Sync> IntoToSqlVec<'a> for &[T] {
//     fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)> {
//         self.to_vec()
//     }
// }

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
    /** Struct describing rows of this type when the full row is needed */
    /* TODO-cleanup what does the 'static actually mean here? */
    type ApiModelType: for<'a> TryFrom<&'a tokio_postgres::Row, Error = ApiError>
        + Send
        + 'static;
    /** [`ApiResourceType`] corresponding to rows of this table */
    const RESOURCE_TYPE: ApiResourceType;
    /** Name of the table */
    const TABLE_NAME: &'static str;
    /** List of names of all columns in the table. */
    const ALL_COLUMNS: &'static [&'static str];

    /** Type of the primary key column. */
    type PrimaryKey: ToSql + Borrow<Uuid> + Sync;
    /** Column name for the primary key */
    const PRIMARY_KEY_COLUMN_NAME: &'static str = "id";
    /**
     * Parts of a WHERE clause that should be included in all queries for live
     * records
     */
    const LIVE_CONDITIONS: &'static str = "time_deleted IS NULL";

    /** Type of the column linking each object to its parent */
    type ParentPrimaryKey: ToSql + Borrow<Uuid> + Sync;
    /** Name of the column linking each object to its parent */
    const PARENT_KEY_COLUMN_NAME: &'static str;
    /** Name of the column containing each object's name */
    const NAME_COLUMN_NAME: &'static str = "name";
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
    type PrimaryKey = Uuid;
    type ParentPrimaryKey = Uuid; // XXX
    const PARENT_KEY_COLUMN_NAME: &'static str = "__nonexistent__"; // XXX
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
    type PrimaryKey = Uuid;
    type ParentPrimaryKey = Uuid;
    const PARENT_KEY_COLUMN_NAME: &'static str = "id";
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
    type PrimaryKey = Uuid;
    type ParentPrimaryKey = Uuid;
    const PARENT_KEY_COLUMN_NAME: &'static str = "id";
}
