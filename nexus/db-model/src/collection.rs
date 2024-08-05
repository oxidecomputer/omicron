// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::pg::Pg;
use diesel::Column;
use diesel::ExpressionMethods;
use diesel::Selectable;
use std::fmt::Debug;

/// Trait to be implemented by any structs representing a collection.
/// For example, since Silos have a one-to-many relationship with
/// Projects, the Silo datatype should implement this trait.
/// ```
/// # use diesel::prelude::*;
/// # use nexus_db_model::DatastoreCollectionConfig;
/// # use nexus_db_model::Generation;
/// #
/// # table! {
/// #     test_schema.silo (id) {
/// #         id -> Uuid,
/// #         time_deleted -> Nullable<Timestamptz>,
/// #         rcgen -> Int8,
/// #     }
/// # }
/// #
/// # table! {
/// #     test_schema.project (id) {
/// #         id -> Uuid,
/// #         time_deleted -> Nullable<Timestamptz>,
/// #         silo_id -> Uuid,
/// #     }
/// # }
///
/// #[derive(Queryable, Insertable, Debug, Selectable)]
/// #[diesel(table_name = project)]
/// struct Project {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
///     pub silo_id: uuid::Uuid,
/// }
///
/// #[derive(Queryable, Insertable, Debug, Selectable)]
/// #[diesel(table_name = silo)]
/// struct Silo {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
///     pub rcgen: Generation,
/// }
///
/// impl DatastoreCollectionConfig<Project> for Silo {
///     // Type of Silo::identity::id and Project::silo_id
///     type CollectionId = uuid::Uuid;
///
///     type GenerationNumberColumn = silo::dsl::rcgen;
///     type CollectionTimeDeletedColumn = silo::dsl::time_deleted;
///
///     type CollectionIdColumn = project::dsl::silo_id;
/// }
/// ```
pub trait DatastoreCollectionConfig<ResourceType> {
    /// The Rust type of the collection id (typically Uuid for us)
    type CollectionId: Copy + Debug;

    /// The column in the CollectionTable that acts as a generation number.
    /// This is the "child-resource-generation-number" in RFD 192.
    type GenerationNumberColumn: Column + Default;

    /// The time deleted column in the CollectionTable
    // We enforce that this column comes from the same table as
    // GenerationNumberColumn when defining insert_resource() below.
    type CollectionTimeDeletedColumn: Column + Default;

    /// The column in the ResourceTable that acts as a foreign key into
    /// the CollectionTable
    type CollectionIdColumn: Column;
}

/// Trait to be implemented by structs representing an attachable collection.
///
/// For example, since Instances have a one-to-many relationship with
/// Disks, the Instance datatype should implement this trait.
/// ```
/// # use diesel::prelude::*;
/// # use nexus_db_model::DatastoreAttachTargetConfig;
/// #
/// # table! {
/// #     test_schema.instance (id) {
/// #         id -> Uuid,
/// #         time_deleted -> Nullable<Timestamptz>,
/// #     }
/// # }
/// #
/// # table! {
/// #     test_schema.disk (id) {
/// #         id -> Uuid,
/// #         time_deleted -> Nullable<Timestamptz>,
/// #         instance_id -> Nullable<Uuid>,
/// #     }
/// # }
///
/// #[derive(Queryable, Debug, Selectable)]
/// #[diesel(table_name = disk)]
/// struct Disk {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
///     pub instance_id: Option<uuid::Uuid>,
/// }
///
/// #[derive(Queryable, Debug, Selectable)]
/// #[diesel(table_name = instance)]
/// struct Instance {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
/// }
///
/// impl DatastoreAttachTargetConfig<Disk> for Instance {
///     // Type of instance::id and disk::id.
///     type Id = uuid::Uuid;
///
///     type CollectionIdColumn = instance::dsl::id;
///     type CollectionTimeDeletedColumn = instance::dsl::time_deleted;
///
///     type ResourceIdColumn = disk::dsl::id;
///     type ResourceCollectionIdColumn = disk::dsl::instance_id;
///     type ResourceTimeDeletedColumn = disk::dsl::time_deleted;
/// }
/// ```
pub trait DatastoreAttachTargetConfig<ResourceType>:
    Selectable<Pg> + Sized
{
    /// The Rust type of the collection and resource ids (typically Uuid).
    type Id: Copy + Debug + PartialEq + Send + 'static;

    /// The primary key column of the collection.
    type CollectionIdColumn: Column;

    /// The time deleted column in the CollectionTable
    type CollectionTimeDeletedColumn: Column<Table = <Self::CollectionIdColumn as Column>::Table>
        + Default
        + ExpressionMethods;

    /// The primary key column of the resource
    type ResourceIdColumn: Column;

    /// The column in the resource acting as a foreign key into the Collection
    type ResourceCollectionIdColumn: Column<Table = <Self::ResourceIdColumn as Column>::Table>
        + Default
        + ExpressionMethods;

    /// The time deleted column in the ResourceTable
    type ResourceTimeDeletedColumn: Column<Table = <Self::ResourceIdColumn as Column>::Table>
        + Default
        + ExpressionMethods;

    /// Controls whether a resource may be attached to a new collection without
    /// first being explicitly detached from the previous one
    const ALLOW_FROM_ATTACHED: bool = false;
}
