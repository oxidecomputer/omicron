/*!
 * Facilities for mapping model Rust types to and from database types
 *
 * We'd prefer to put this in omicron-nexus, but the Rust orphan rules prevent
 * that.
 *
 * There are essentially two patterns used for database conversions:
 *
 * (1) For Rust types that map directly to database types, we impl
 *     tokio_postgres's [`ToSql`] and [`FromSql`] traits.  For the most part,
 *     these are newtypes in Rust that wrap a type for which there is already an
 *     impl for these traits and we delegate to those impls where possible.  For
 *     example, [`ApiByteCount`] is a numeric newtype containing a u64, which
 *     maps directly to a CockroachDB (PostgreSQL) `int`, which is essentially
 *     an `i64`.  The `ToSql` and `FromSql` impls for `ApiByteCount` delegate to
 *     the existing impls for `i64`.
 *
 * (2) For Rust types that require multiple database values (e.g., an
 *     [`ApiDisk`], which represents an entire row from the Disk table, we impl
 *     `TryFrom<&tokio_postgres::Row>`.  The impl pulls multiple values out of
 *     the row, generally using the names of columns from the table.  We also
 *     impl `SqlSerialize`, which records key-value pairs that can be safely
 *     inserted into SQL "UPDATE" and "INSERT" statements.
 *
 * These often combine to form a hierarchy.  For example, to load an
 * [`ApiProject`] from a row from the Project table:
 *
 * * We start with `ApiProject::try_from(row: &tokio_postgres::Row)`.
 * * The main field of an `ApiProject` is its `ApiIdentityMetadata`, so
 *   `ApiProject::try_from` invokes `ApiIdentityMetadata::try_from(row)` with
 *   the same `row` argument.
 * * `ApiIdentityMetadata` pulls out the fields that it knows about, including
 *   `id` and `name`.
 * * `name` is an [`ApiName`], which is a newtype that wraps a Rust `String`.
 *   This has a `FromSql` impl.
 *
 * The ToSql/FromSql, and TryFrom impls are in this file because the Rust orphan
 * rules prevent them from going in omicron_nexus.  We're not so restricted on
 * the SqlSerialize interface, and we don't want to have to pull in the other
 * facilities that those impls depend on, so those remain in omicron_nexus.
 */
/*
 * TODO-cleanup We could potentially derive these TryFrom impls.  Diesel and
 * sqlx both have derive macros that do this.  It's not clear how that will
 * compose with our custom types having their own impls of `ToSql` and
 * `FromSql`.
 *
 * TODO-coverage tests for these FromSql and ToSql implementations
 */

use super::db::sql_row_value;
use crate::bail_unless;
use crate::error::ApiError;
use crate::model::ApiByteCount;
use crate::model::ApiDisk;
use crate::model::ApiDiskAttachment;
use crate::model::ApiDiskRuntimeState;
use crate::model::ApiDiskState;
use crate::model::ApiGeneration;
use crate::model::ApiIdentityMetadata;
use crate::model::ApiInstance;
use crate::model::ApiInstanceCpuCount;
use crate::model::ApiInstanceRuntimeState;
use crate::model::ApiInstanceState;
use crate::model::ApiName;
use crate::model::ApiProject;
use chrono::DateTime;
use chrono::Utc;
use std::convert::TryFrom;
use tokio_postgres::types::FromSql;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

/*
 * FromSql/ToSql impls used for simple Rust types
 * (see module-level documentation above)
 */

/**
 * Define impls for ToSql and FromSql for a type T such that &T: Into<D> and
 * T: TryFrom<D>.  These impls delegate to the "D" impls of these traits.  See
 * the module-level documentation for why this is useful.
 */
macro_rules! impl_sql_wrapping {
    ($T:ident, $D:ty) => {
        impl ToSql for $T {
            fn to_sql(
                &self,
                ty: &tokio_postgres::types::Type,
                out: &mut tokio_postgres::types::private::BytesMut,
            ) -> Result<
                tokio_postgres::types::IsNull,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                <$D>::from(self).to_sql(ty, out)
            }

            fn accepts(ty: &tokio_postgres::types::Type) -> bool {
                <$D as ToSql>::accepts(ty)
            }

            tokio_postgres::types::to_sql_checked!();
        }

        impl<'a> FromSql<'a> for $T {
            fn from_sql(
                ty: &tokio_postgres::types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
                let value: $D = <$D as FromSql>::from_sql(ty, raw)?;
                $T::try_from(value).map_err(|e| e.into())
            }

            fn accepts(ty: &tokio_postgres::types::Type) -> bool {
                <$D as FromSql>::accepts(ty)
            }
        }
    };
}

impl_sql_wrapping!(ApiByteCount, i64);
impl_sql_wrapping!(ApiGeneration, i64);
impl_sql_wrapping!(ApiInstanceCpuCount, i64);
impl_sql_wrapping!(ApiName, &str);

/*
 * TryFrom impls used for more complex Rust types
 */

/// Load an [`ApiIdentityMetadata`] from a row of any table that contains the
/// usual identity fields: "id", "name", "description, "time_created", and
/// "time_modified".
impl TryFrom<&tokio_postgres::Row> for ApiIdentityMetadata {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let time_deleted: Option<DateTime<Utc>> =
            sql_row_value(value, "time_deleted")?;
        /*
         * We could support representing deleted objects, but we would want to
         * think about how to do that.  For example, we might want to use
         * separate types so that the control plane can't accidentally do things
         * like attach a disk to a deleted Instance.  We haven't figured any of
         * this out, and there's no need yet.
         */
        bail_unless!(
            time_deleted.is_none(),
            "model does not support objects that have been deleted"
        );
        Ok(ApiIdentityMetadata {
            id: sql_row_value(value, "id")?,
            name: sql_row_value(value, "name")?,
            description: sql_row_value(value, "description")?,
            time_created: sql_row_value(value, "time_created")?,
            time_modified: sql_row_value(value, "time_modified")?,
        })
    }
}

impl TryFrom<&tokio_postgres::Row> for ApiInstanceState {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let variant = sql_row_value(value, "instance_state")?;
        let rebooting = sql_row_value(value, "rebooting")?;
        ApiInstanceState::try_from((variant, rebooting))
            .map_err(|err| ApiError::InternalError { message: err.into() })
    }
}

/// Load an [`ApiProject`] from a whole row of the "Project" table.
impl TryFrom<&tokio_postgres::Row> for ApiProject {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiProject { identity: ApiIdentityMetadata::try_from(value)? })
    }
}

/// Load an [`ApiInstance`] from a whole row of the "Instance" table.
impl TryFrom<&tokio_postgres::Row> for ApiInstance {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiInstance {
            identity: ApiIdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            ncpus: sql_row_value(value, "ncpus")?,
            memory: sql_row_value(value, "memory")?,
            hostname: sql_row_value(value, "hostname")?,
            runtime: ApiInstanceRuntimeState::try_from(value)?,
        })
    }
}

/// Load an [`ApiInstanceRuntimeState`] from a row of the "Instance" table,
/// using the "instance_state", "active_server_id", "state_generation", and
/// "time_state_updated" columns.
impl TryFrom<&tokio_postgres::Row> for ApiInstanceRuntimeState {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiInstanceRuntimeState {
            run_state: ApiInstanceState::try_from(value)?,
            sled_uuid: sql_row_value(value, "active_server_id")?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

/// Load an [`ApiDisk`] from a row of the "Disk" table.
impl TryFrom<&tokio_postgres::Row> for ApiDisk {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiDisk {
            identity: ApiIdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            create_snapshot_id: sql_row_value(value, "origin_snapshot")?,
            size: sql_row_value(value, "size_bytes")?,
            runtime: ApiDiskRuntimeState::try_from(value)?,
        })
    }
}

/// Load an [`ApiDiskAttachment`] from a database row containing those columns
/// of the Disk table that describe the attachment: "id", "name", "disk_state",
/// "attach_instance_id"
impl TryFrom<&tokio_postgres::Row> for ApiDiskAttachment {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiDiskAttachment {
            instance_id: sql_row_value(value, "attach_instance_id")?,
            disk_id: sql_row_value(value, "id")?,
            disk_name: sql_row_value(value, "name")?,
            disk_state: ApiDiskState::try_from(value)?,
        })
    }
}

/// Load an [`ApiDiskRuntimeState`'] from a row from the Disk table, using the
/// columns needed for [`ApiDiskState`], plus "state_generation" and
/// "time_state_updated".
impl TryFrom<&tokio_postgres::Row> for ApiDiskRuntimeState {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiDiskRuntimeState {
            disk_state: ApiDiskState::try_from(value)?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

/// Load an [`ApiDiskState`] from a row from the Disk table, using the columns
/// "disk_state" and "attach_instance_id".
impl TryFrom<&tokio_postgres::Row> for ApiDiskState {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let disk_state_str: &str = sql_row_value(value, "disk_state")?;
        let instance_uuid: Option<Uuid> =
            sql_row_value(value, "attach_instance_id")?;
        ApiDiskState::try_from((disk_state_str, instance_uuid))
            .map_err(|e| ApiError::internal_error(&e))
    }
}
