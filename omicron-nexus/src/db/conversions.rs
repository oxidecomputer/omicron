/*!
 * Facilities for mapping Rust types to and from database types
 *
 * There are essentially two patterns here:
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
 */
/*
 * TODO-cleanup We could potentially derive these TryFrom impls.  Diesel and
 * sqlx both have derive macros that do this.  It's not clear how that will
 * compose with our custom types having their own impls of `ToSql` and
 * `FromSql`.
 *
 * TODO-coverage tests for these FromSql and ToSql implementations
 */

use omicron_common::error::ApiError;
use omicron_common::model::ApiByteCount;
use omicron_common::model::ApiDisk;
use omicron_common::model::ApiDiskAttachment;
use omicron_common::model::ApiDiskCreateParams;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskState;
use omicron_common::model::ApiGeneration;
use omicron_common::model::ApiIdentityMetadata;
use omicron_common::model::ApiIdentityMetadataCreateParams;
use omicron_common::model::ApiInstance;
use omicron_common::model::ApiInstanceCpuCount;
use omicron_common::model::ApiInstanceCreateParams;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiName;
use omicron_common::model::ApiProject;
use omicron_common::model::ApiProjectCreateParams;
use omicron_common::bail_unless;
use chrono::DateTime;
use chrono::Utc;
use std::convert::TryFrom;
use tokio_postgres::types::FromSql;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::operations::sql_row_value;
use super::sql::SqlSerialize;
use super::sql::SqlValueSet;

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
impl_sql_wrapping!(ApiInstanceState, &str);
impl_sql_wrapping!(ApiName, &str);

/*
 * TryFrom and SqlSerialize impls used for more complex Rust types
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

impl SqlSerialize for ApiIdentityMetadataCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("name", &self.name);
        output.set("description", &self.description);
        output.set("time_deleted", &(None as Option<DateTime<Utc>>));
    }
}

/// Load an [`ApiProject`] from a whole row of the "Project" table.
impl TryFrom<&tokio_postgres::Row> for ApiProject {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiProject { identity: ApiIdentityMetadata::try_from(value)? })
    }
}

impl SqlSerialize for ApiProjectCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output)
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

impl SqlSerialize for ApiInstanceCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output);
        output.set("ncpus", &self.ncpus);
        output.set("memory", &self.memory);
        output.set("hostname", &self.hostname);
    }
}

/// Load an [`ApiInstanceRuntimeState`] from a row of the "Instance" table,
/// using the "instance_state", "reboot_in_progress", "active_server_id",
/// "state_generation", and "time_state_updated" columns.
impl TryFrom<&tokio_postgres::Row> for ApiInstanceRuntimeState {
    type Error = ApiError;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(ApiInstanceRuntimeState {
            run_state: sql_row_value(value, "instance_state")?,
            reboot_in_progress: sql_row_value(value, "reboot_in_progress")?,
            sled_uuid: sql_row_value(value, "active_server_id")?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

impl SqlSerialize for ApiInstanceRuntimeState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("instance_state", &self.run_state);
        output.set("active_server_id", &self.sled_uuid);
        output.set("state_generation", &self.gen);
        output.set("time_state_updated", &self.time_updated);
        output.set("reboot_in_progress", &self.reboot_in_progress);
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

impl SqlSerialize for ApiDiskCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output);
        output.set("size_bytes", &self.size);
        output.set("origin_snapshot", &self.snapshot_id);
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

impl SqlSerialize for ApiDiskRuntimeState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.disk_state.sql_serialize(output);
        output.set("state_generation", &self.gen);
        output.set("time_state_updated", &self.time_updated);
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

impl SqlSerialize for ApiDiskState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        let attach_id = &self.attached_instance_id().map(|id| *id);
        output.set("attach_instance_id", attach_id);
        output.set("disk_state", &self.label());
    }
}
