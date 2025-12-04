// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper queries for working with disks.

use crate::db::queries::next_item::{DefaultShiftGenerator, NextItem};
use diesel::{
    Column, QueryResult,
    pg::Pg,
    query_builder::{AstPass, QueryFragment, QueryId},
    sql_types,
};
use uuid::Uuid;

/// The maximum number of disks that can be attached to an instance.
//
// This is defined here for layering reasons: the main Nexus crate depends on
// the db-queries crate, so the disk-per-instance limit lives here and Nexus
// proper re-exports it.
pub const MAX_DISKS_PER_INSTANCE: u32 = 8;

/// A wrapper for the query that selects a PCI slot for a newly-attached disk.
///
/// The general idea is to produce a query that left joins a single-column table
/// containing the sequence [0, 1, 2, ..., MAX_DISKS] against the disk table,
/// filtering for disks that are attached to the instance of interest and whose
/// currently-assigned slots match the slots in the sequence. Filtering this
/// query to just those rows where a slot has no disk attached yields the first
/// available slot number. If no slots are available, the returned value is
/// MAX_DISKS, which is not a valid slot number and which will be rejected when
/// attempting to update the row (the "slot" column has a CHECK clause that
/// enforces this at the database level).
///
/// See the `NextItem` documentation for more details.
struct NextDiskSlot {
    inner: NextItem<i16, DefaultShiftGenerator<i16>>,
}

impl NextDiskSlot {
    fn new(instance_id: Uuid) -> Self {
        let generator = DefaultShiftGenerator::new(
            0,
            i64::try_from(MAX_DISKS_PER_INSTANCE).unwrap(),
            0,
        )
        .expect("invalid min/max shift");
        Self {
            inner: NextItem::new_scoped(
                "disk",
                "slot",
                "attach_instance_id",
                instance_id,
                generator,
            ),
        }
    }
}

impl QueryId for NextDiskSlot {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for NextDiskSlot {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        self.inner.walk_ast(out.reborrow())?;
        Ok(())
    }
}

/// Produces a query fragment containing the SET clause to use in the UPDATE
/// statement when attaching a disk to an instance.
///
/// The expected form of the statement is
///
/// ```sql
/// SET attach_instance_id = instance_id,
///     disk_state = 'attached',
///     slot = (SELECT 0 + shift AS slot FROM
///             (SELECT generate_series(0, 8) AS shift
///              UNION ALL
///              SELECT generate_series(0, -1) AS shift)
///             LEFT OUTER JOIN disk
///             ON (attach_instance_id, slot, time_deleted IS NULL) =
///                (instance_id, 0 + shift, TRUE)
///             WHERE slot IS NULL
///             LIMIT 1)
/// ```
///
/// This fragment can be passed to an `attach_resource` operation by supplying
/// it as the argument to a `set`, e.g.
/// `diesel::update(disk::dsl::disk).set(DiskSetClauseForAttach::new(instance_id))`.
pub struct DiskSetClauseForAttach {
    attach_instance_id: Uuid,
    next_slot: NextDiskSlot,
}

impl DiskSetClauseForAttach {
    pub fn new(instance_id: Uuid) -> Self {
        Self {
            attach_instance_id: instance_id,
            next_slot: NextDiskSlot::new(instance_id),
        }
    }
}

impl QueryId for DiskSetClauseForAttach {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for DiskSetClauseForAttach {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        let attached_label =
            omicron_common::api::external::DiskState::Attached(
                self.attach_instance_id,
            )
            .label();

        // Ideally this code would not have to handcraft the entire SET
        // statement. It would be cleaner to write something like
        //
        // diesel::update(disk::dsl::disk).set(
        //  (disk::dsl::disk::attach_instance_id.eq(attach_instance_id),
        //   disk::dsl::disk::disk_state.eq(attached_label),
        //   disk::dsl::disk::slot.eq(next_slot_query))
        //
        // where `next_slot_query` is a wrapper around the `NextItem` query
        // above that yields the slot-choosing SELECT statement. Diesel provides
        // a `single_value` adapter function that's supposed to do this: given
        // a query that returns a single column, it appends a LIMIT 1 to the
        // query and parenthesizes it so that it can be used as the argument to
        // a SET statement. The sticky bit is the trait bounds: SingleValueDsl
        // is implemented for SelectQuery + LimitDsl, and while it's easy enough
        // to impl SelectQuery for NextDiskSlot, implementing LimitDsl is harder
        // (and seems to be discouraged by the trait docs)--it is natively
        // implemented for diesel::Table, but here there's no table type, in
        // part because the left side of the join is synthesized from thin air
        // by the NextItem query fragment.
        //
        // Rather than spar extensively with Diesel over these details, just
        // implement the whole SET by hand.
        out.push_identifier(
            nexus_db_schema::schema::disk::dsl::attach_instance_id::NAME,
        )?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.attach_instance_id)?;
        out.push_sql(", ");
        out.push_identifier(
            nexus_db_schema::schema::disk::dsl::disk_state::NAME,
        )?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Text, str>(attached_label)?;
        out.push_sql(", ");
        out.push_identifier(nexus_db_schema::schema::disk::dsl::slot::NAME)?;
        out.push_sql(" = (");
        self.next_slot.walk_ast(out.reborrow())?;
        out.push_sql(")");

        Ok(())
    }
}

// Required to pass `DiskSetClauseForAttach` to `set`.
impl diesel::query_builder::AsChangeset for DiskSetClauseForAttach {
    type Target = nexus_db_schema::schema::disk::dsl::disk;
    type Changeset = Self;

    fn as_changeset(self) -> Self::Changeset {
        self
    }
}

/// Builds the next disk slot subquery using QueryBuilder.
///
/// This is equivalent to NextDiskSlot but uses the QueryBuilder API.
/// It generates a query that selects the next available disk slot for an instance.
pub(crate) fn build_next_disk_slot_subquery(
    builder: &mut crate::db::raw_query_builder::QueryBuilder,
    instance_id: Uuid,
) {
    // SELECT 0 + shift AS slot FROM
    builder.sql("SELECT 0 + shift AS slot FROM (");

    // (SELECT generate_series(0, 8) AS shift
    builder.sql("SELECT generate_series(0, ");
    builder.param().bind::<sql_types::BigInt, i64>(
        i64::try_from(MAX_DISKS_PER_INSTANCE).unwrap(),
    );
    builder.sql(") AS shift");

    // UNION ALL SELECT generate_series(0, -1) AS shift)
    builder.sql(" UNION ALL SELECT generate_series(0, -1) AS shift");
    builder.sql(") ");

    // LEFT OUTER JOIN disk
    builder.sql("LEFT OUTER JOIN disk ");

    // ON (attach_instance_id, slot, time_deleted IS NULL) =
    //    (instance_id, 0 + shift, TRUE)
    builder.sql("ON (attach_instance_id, slot, time_deleted IS NULL) = (");
    builder.param().bind::<sql_types::Uuid, Uuid>(instance_id);
    builder.sql(", 0 + shift, TRUE) ");

    // WHERE slot IS NULL LIMIT 1
    builder.sql("WHERE slot IS NULL ORDER BY shift LIMIT 1");
}
