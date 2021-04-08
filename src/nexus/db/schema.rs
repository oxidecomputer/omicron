/*!
 * Rust types that describe the database schema
 */

use crate::api_error::ApiError;
use crate::api_model::ApiDisk;
use crate::api_model::ApiInstance;
use crate::api_model::ApiProject;
use crate::api_model::ApiResourceType;
use std::convert::TryFrom;

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
    type ApiModelType: for<'a> TryFrom<
        &'a tokio_postgres::Row,
        Error = ApiError,
    >;
    /** [`ApiResourceType`] corresponding to rows of this table */
    const RESOURCE_TYPE: ApiResourceType;
    /** Name of the table */
    const TABLE_NAME: &'static str;
    /** List of names of all columns in the table. */
    const ALL_COLUMNS: &'static [&'static str];
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
