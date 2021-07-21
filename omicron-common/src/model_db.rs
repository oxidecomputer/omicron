/*!
 * Facilities for mapping model Rust types to and from database types
 *
 * We'd prefer to put this in omicron-nexus, but the Rust orphan rules prevent
 * that.
 *
 * There are essentially two patterns used for database conversions:
 *
 * (1) For Rust types that map directly to database types, we impl
 *     tokio_postgres's [`tokio_postgres::types::ToSql`] and
 *     [`tokio_postgres::types::FromSql`] traits.  For the most part, these are
 *     newtypes in Rust that wrap a type for which there is already an impl for
 *     these traits and we delegate to those impls where possible.  For example,
 *     [`ByteCount`] is a numeric newtype containing a u64, which maps
 *     directly to a CockroachDB (PostgreSQL) `int`, which is essentially an
 *     `i64`.  The `ToSql` and `FromSql` impls for `ByteCount` delegate to
 *     the existing impls for `i64`.
 *
 * (2) For Rust types that require multiple database values (e.g., an
 *     [`Disk`], which represents an entire row from the Disk table, we impl
 *     `TryFrom<&tokio_postgres::Row>`.  The impl pulls multiple values out of
 *     the row, generally using the names of columns from the table.  We also
 *     impl `SqlSerialize`, which records key-value pairs that can be safely
 *     inserted into SQL "UPDATE" and "INSERT" statements.
 *
 * These often combine to form a hierarchy.  For example, to load an
 * [`Project`] from a row from the Project table:
 *
 * * We start with `Project::try_from(row: &tokio_postgres::Row)`.
 * * The main field of an `Project` is its `IdentityMetadata`, so
 *   `Project::try_from` invokes `IdentityMetadata::try_from(row)` with
 *   the same `row` argument.
 * * `IdentityMetadata` pulls out the fields that it knows about, including
 *   `id` and `name`.
 * * `name` is an [`Name`], which is a newtype that wraps a Rust `String`.
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

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use super::db::sql_row_value;
use crate::api::external::Error;
use crate::api::external::ByteCount;
use crate::api::internal::nexus::Disk;
use crate::api::external::DiskAttachment;
use crate::api::internal::nexus::DiskRuntimeState;
use crate::api::external::DiskState;
use crate::api::external::Generation;
use crate::api::external::IdentityMetadata;
use crate::api::external::Instance;
use crate::api::external::InstanceCpuCount;
use crate::api::external::InstanceRuntimeState;
use crate::api::external::InstanceState;
use crate::api::external::Name;
use crate::api::external::OximeterAssignment;
use crate::api::external::OximeterInfo;
use crate::api::external::ProducerEndpoint;
use crate::api::external::Project;
use crate::bail_unless;
use chrono::DateTime;
use chrono::Utc;
use std::convert::TryFrom;
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
#[macro_export]
macro_rules! impl_sql_wrapping {
    ($T:ident, $D:ty) => {
        impl tokio_postgres::types::ToSql for $T {
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
                <$D as tokio_postgres::types::ToSql>::accepts(ty)
            }

            tokio_postgres::types::to_sql_checked!();
        }

        impl<'a> tokio_postgres::types::FromSql<'a> for $T {
            fn from_sql(
                ty: &tokio_postgres::types::Type,
                raw: &'a [u8],
            ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
                let value: $D =
                    <$D as tokio_postgres::types::FromSql>::from_sql(ty, raw)?;
                $T::try_from(value).map_err(|e| e.into())
            }

            fn accepts(ty: &tokio_postgres::types::Type) -> bool {
                <$D as tokio_postgres::types::FromSql>::accepts(ty)
            }
        }
    };
}

impl_sql_wrapping!(ByteCount, i64);
impl_sql_wrapping!(Generation, i64);
impl_sql_wrapping!(InstanceCpuCount, i64);
impl_sql_wrapping!(Name, &str);

/*
 * TryFrom impls used for more complex Rust types
 */

/// Load an [`IdentityMetadata`] from a row of any table that contains the
/// usual identity fields: "id", "name", "description, "time_created", and
/// "time_modified".
impl TryFrom<&tokio_postgres::Row> for IdentityMetadata {
    type Error = Error;

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
        Ok(IdentityMetadata {
            id: sql_row_value(value, "id")?,
            name: sql_row_value(value, "name")?,
            description: sql_row_value(value, "description")?,
            time_created: sql_row_value(value, "time_created")?,
            time_modified: sql_row_value(value, "time_modified")?,
        })
    }
}

impl TryFrom<&tokio_postgres::Row> for InstanceState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let variant: &str = sql_row_value(value, "instance_state")?;
        InstanceState::try_from(variant)
            .map_err(|err| Error::InternalError { message: err })
    }
}

/// Load an [`Project`] from a whole row of the "Project" table.
impl TryFrom<&tokio_postgres::Row> for Project {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Project { identity: IdentityMetadata::try_from(value)? })
    }
}

/// Load an [`Instance`] from a whole row of the "Instance" table.
impl TryFrom<&tokio_postgres::Row> for Instance {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Instance {
            identity: IdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            ncpus: sql_row_value(value, "ncpus")?,
            memory: sql_row_value(value, "memory")?,
            hostname: sql_row_value(value, "hostname")?,
            runtime: InstanceRuntimeState::try_from(value)?,
        })
    }
}

/// Load an [`InstanceRuntimeState`] from a row of the "Instance" table,
/// using the "instance_state", "active_server_id", "state_generation", and
/// "time_state_updated" columns.
impl TryFrom<&tokio_postgres::Row> for InstanceRuntimeState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(InstanceRuntimeState {
            run_state: InstanceState::try_from(value)?,
            sled_uuid: sql_row_value(value, "active_server_id")?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

/// Load an [`Disk`] from a row of the "Disk" table.
impl TryFrom<&tokio_postgres::Row> for Disk {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(Disk {
            identity: IdentityMetadata::try_from(value)?,
            project_id: sql_row_value(value, "project_id")?,
            create_snapshot_id: sql_row_value(value, "origin_snapshot")?,
            size: sql_row_value(value, "size_bytes")?,
            runtime: DiskRuntimeState::try_from(value)?,
        })
    }
}

/// Load an [`DiskAttachment`] from a database row containing those columns
/// of the Disk table that describe the attachment: "id", "name", "disk_state",
/// "attach_instance_id"
impl TryFrom<&tokio_postgres::Row> for DiskAttachment {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(DiskAttachment {
            instance_id: sql_row_value(value, "attach_instance_id")?,
            disk_id: sql_row_value(value, "id")?,
            disk_name: sql_row_value(value, "name")?,
            disk_state: DiskState::try_from(value)?,
        })
    }
}

/// Load an [`DiskRuntimeState`'] from a row from the Disk table, using the
/// columns needed for [`DiskState`], plus "state_generation" and
/// "time_state_updated".
impl TryFrom<&tokio_postgres::Row> for DiskRuntimeState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        Ok(DiskRuntimeState {
            disk_state: DiskState::try_from(value)?,
            gen: sql_row_value(value, "state_generation")?,
            time_updated: sql_row_value(value, "time_state_updated")?,
        })
    }
}

/// Load an [`DiskState`] from a row from the Disk table, using the columns
/// "disk_state" and "attach_instance_id".
impl TryFrom<&tokio_postgres::Row> for DiskState {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let disk_state_str: &str = sql_row_value(value, "disk_state")?;
        let instance_uuid: Option<Uuid> =
            sql_row_value(value, "attach_instance_id")?;
        DiskState::try_from((disk_state_str, instance_uuid))
            .map_err(|e| Error::internal_error(&e))
    }
}

/// Load a [`ProducerEndpoint`] from a row in the `MetricProducer` table, using
/// the columns "id", "ip", "port", "interval", and "route"
impl TryFrom<&tokio_postgres::Row> for ProducerEndpoint {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let id: Uuid = sql_row_value(value, "id")?;
        let ip: IpAddr = sql_row_value(value, "ip")?;
        let port: i32 = sql_row_value(value, "port")?;
        let address = SocketAddr::new(ip, port as _);
        let base_route: String = sql_row_value(value, "route")?;
        let interval =
            Duration::from_secs_f64(sql_row_value(value, "interval")?);
        Ok(Self { id, address, base_route, interval })
    }
}

/// Load an [`OximeterInfo`] from a row in the `Oximeter` table, using the
/// columns "id", "ip", and "port".
impl TryFrom<&tokio_postgres::Row> for OximeterInfo {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let collector_id: Uuid = sql_row_value(value, "id")?;
        let ip: IpAddr = sql_row_value(value, "ip")?;
        let port: i32 = sql_row_value(value, "port")?;
        let address = SocketAddr::new(ip, port as _);
        Ok(Self { collector_id, address })
    }
}

/// Load an [`OximeterAssignment`] from a row in the `OximeterAssignment`
/// table, using the columns "oximeter_id" and "producer_id"
impl TryFrom<&tokio_postgres::Row> for OximeterAssignment {
    type Error = Error;

    fn try_from(value: &tokio_postgres::Row) -> Result<Self, Self::Error> {
        let oximeter_id: Uuid = sql_row_value(value, "oximeter_id")?;
        let producer_id: Uuid = sql_row_value(value, "producer_id")?;
        Ok(Self { oximeter_id, producer_id })
    }
}
