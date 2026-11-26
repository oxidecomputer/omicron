// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper queries for working with disks.

use crate::db::queries::next_item::{DefaultShiftGenerator, NextItem};
use diesel::{
    AppearsOnTable, Expression, ExpressionMethods, QueryResult,
    pg::Pg,
    query_builder::{AsChangeset, AstPass, QueryFragment, QueryId},
};
use nexus_db_schema::schema::disk;
use omicron_common::api::external::DiskState;
use uuid::Uuid;

/// The maximum number of disks that can be attached to an instance.
//
// This is defined here for layering reasons: the main Nexus crate depends on
// the db-queries crate, so the disk-per-instance limit lives here and Nexus
// proper re-exports it.
pub const MAX_DISKS_PER_INSTANCE: u32 = 12;

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
#[derive(Debug, Clone, Copy)]
pub struct NextDiskSlot {
    inner:
        NextItem<disk::table, i16, disk::slot, Uuid, disk::attach_instance_id>,
}

impl NextDiskSlot {
    fn new(instance_id: Uuid) -> Self {
        let generator = DefaultShiftGenerator::new(
            0,
            i64::try_from(MAX_DISKS_PER_INSTANCE).unwrap(),
            0,
        )
        .expect("invalid min/max shift");
        Self { inner: NextItem::new_scoped(generator, instance_id) }
    }
}

impl QueryId for NextDiskSlot {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for NextDiskSlot {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        out.push_sql("(");
        self.inner.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

// Lets the subquery be used as the value assigned to `disk.slot`.
impl Expression for NextDiskSlot {
    type SqlType = <disk::slot as Expression>::SqlType;
}

impl AppearsOnTable<disk::table> for NextDiskSlot {}

/// Produces the SET clause to use in the UPDATE statement when attaching a
/// disk to an instance.
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
/// This can be passed to an `attach_resource` operation by supplying it as the
/// argument to a `set`, e.g.
/// `diesel::update(disk::dsl::disk).set(DiskSetClauseForAttach::new(instance_id))`.
#[derive(Debug, Clone)]
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

type DiskAttachAssignments = (
    diesel::dsl::Eq<disk::attach_instance_id, Uuid>,
    diesel::dsl::Eq<disk::disk_state, &'static str>,
    diesel::dsl::Eq<disk::slot, NextDiskSlot>,
);

// Required to pass `DiskSetClauseForAttach` to `set`.
//
// The changeset is built from Diesel's own assignment types, rather than a
// hand-written `QueryFragment`, because Diesel only allows a `WHERE` clause on
// an `UPDATE` whose changeset is made of those.
impl AsChangeset for DiskSetClauseForAttach {
    type Target = disk::table;
    type Changeset = <DiskAttachAssignments as AsChangeset>::Changeset;

    fn as_changeset(self) -> Self::Changeset {
        let attached_label =
            DiskState::Attached(self.attach_instance_id).label();
        (
            disk::attach_instance_id.eq(self.attach_instance_id),
            disk::disk_state.eq(attached_label),
            disk::slot.eq(self.next_slot),
        )
            .as_changeset()
    }
}
