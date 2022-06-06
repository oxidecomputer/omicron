// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Tools for working with a multi-table object in an atomic fashion.
//!
//! Database normalization is the process of structuring tables to reduce
//! duplication and increase integrity or validity checking. A very common
//! example is something like an `Employee` table, with a `Manager` column. In
//! the case where multiple people report to the same manager, those rows will
//! contain the same name or entry in the `Manager` column. Normalizing these
//! tables involes creating a new `Manager` table, and referring to the primary
//! key of that table in the `Employee` table. This enables storing all the
//! information about the manager in its own table, and referring to all that
//! data from the employee, by foreign key.
//!
//! There are several objects in the Oxide API where such a representation is
//! important. A good example is a VPC Firewall. This resource has its own ID,
//! name, and other metadata. However, it also refers to a potentially variable
//! number of firewall _rules_. One might store those inline in the firewall
//! table, as an array of objects for example.
//!
//! This has several downsides. First, it dramatically limits the indexing and
//! search operations that can be efficiently performed. Imagine we wanted to
//! search for a particular firewall rule. One either needs to create a full
//! inverted index on the rule column, or perform a table scan _and_ a
//! potentially linear search within the array of items. The former is very
//! space-inefficient, the latter time-inefficient.
//!
//! Instead, we could normalize these tables, putting the rules in a separate
//! table. That enables efficient querying with more standard indexes, such as
//! B-trees.
//!
//! There are problems here, however. We've split the "real" firewall object
//! into multiple tables. The firewall is not really a "collection" of rules, it
//! _is_ the rules. It has no independent identity beyond the list of rules that
//! make it up. Put another way, the primitive operation on a firewall is a
//! `PUT`, i.e., a complete replacement of all rules within it. One could do
//! that inside an interactive transaction, or with joins or other more
//! complicated operations such as common table expressions.
//!
//! This module provides tools for working with such normalized, multi-table
//! objects, in an atomic fashion using common table expressions.

use diesel::Table;
use diesel::associations::BelongsTo;

pub struct MultiTableObject<ParentModel, ChildModel> {
    parent: ParentModel,
    children: Vec<ChildModel>,
}

/*
#[derive(Debug, Clone, Copy)]
pub struct MultiTableObject<
    Model,
    ParentTable
    ParentModel,
    ChildTable,
    ChildModel
> {
    parent_table: ParentTable,
    parent_model: ParentModel,
    child_table: ChildTable,
    child_model: ChildModel,
}
*/

/*
// ChildTable: BelongsTo<Parent>
//
// Want to basically have ability to CRUD this one object, and have that do the
// right operation on both tables.
//
// insert -> (insert parent, insert all children)
// list -> (join parent, children, grouped by id or similar)
// get -> (join parent, children with particular parent id)
// update -> (remove all children, insert all children, update parent rcgen)
// delete -> (remove all children, remove parent)
//
//
// So we'd have
//  VpcFirewallParent
//  VpcFirewallRule
//
//  VpcFirewall is MultiTableObject<VpcFirewallParent, VpcFirewallRule>;
//
//
//  Problems:
//
//  How do you list all of these? It looks like we'd need do use `grouped_by` to
//  get a vec of these fuckers. Or generate an actual GROUP BY and HAVING
//  <conditions>

impl<Parent, Child> MultiTableObject<Parent, Child>
where
    Parent: Table + Default,
    Child: Table + BelongsTo<Parent> + Default,
{
    pub fn new() -> Self {
        Self { parent: Parent::default(), child: Child::default() }
    }

    pub fn insert(self, conn: &DbConnection) -> CreateResult<Self> {
        /*
        WITH
        inserted_parent
                AS (
                        INSERT
                        INTO
                                p (id)
                        VALUES
                                ('cd9963c3-3507-c29e-baea-a79faf887122')
                        RETURNING
                                *
                ),
        inserted_children
                AS (
                        INSERT
                        INTO
                                c (id, p_id)
                        VALUES
                                (
                                        '90f1ab21-502e-edd6-b655-c165e504a85b',
                                        'cd9963c3-3507-c29e-baea-a79faf887122'
                                ),
                                (
                                        '04c64cb5-48dc-c430-ed58-a8fc8d77b2ea',
                                        'cd9963c3-3507-c29e-baea-a79faf887122'
                                )
                        RETURNING
                                *
                )
        SELECT
                *
        FROM
                inserted_parent, inserted_children
        WHERE
                inserted_parent.id = inserted_children.p_id
                AND inserted_parent.id
                        = 'cd9963c3-3507-c29e-baea-a79faf887122'
        */
        todo!()
    }

    pub fn list(conn: &DbConnection) -> ListResultVec<Self> {
        todo!()
    }

    pub fn get(conn: &DbConnection) -> ListResult<Model> {
        /*
         * SELECT * FROM
         *  parent, child
         * WHERE
         *  parent.id = child.parent_id AND
         *  parent.id = <parent_id> AND
         *  parent.time_deleted IS NOT NULL;
         */
        todo!()
    }

    pub fn update(self, conn: &DbConnection) -> UpdateResult<Self> {
        /*
         * WITH
         *  updated_children AS (
         *      UPDATE child SET <foo> = <bar>
         *      where parent.id = <parent_id>
         *      returning *
         *  ),
         *  updated_parent AS (
         *      UPDATE parent SET
         *          (time_modified, rcgen)  = (NOW(), rcgen + 1)
         *      where id = <parent_id>
         *      returning *
         *  )
         *  select * from updated_parent, updated_children
         *  where updated_parent.id = updated_children.parent_id
         *  and updated_parent.id = <parent_id>
         */
        todo!()
    }

    pub fn delete(self, conn: &DbConnection) -> DeleteResult {
        /* Delete children, delete parent, return...count? */
        todo!()
    }
}
*/

macro_rules! multi_table_object {
    ($model:ident, $parent:ty, $child:ty) => {
        pub struct $model {
            mto: crate::db::multi_table_object::MultiTableObject<
                <$parent as ::diesel::associations::HasTable>::Table,
                <$child as ::diesel::associations::HasTable>::Table,
            >,
            parent: $parent,
            children: Vec<$child>,
        }

        impl $model {
            pub fn new(parent: $parent, children: &[$child]) -> Self {
                assert!(!children.is_empty());
                let children = children.to_vec();
                let mto = crate::db::multi_table_object::MultiTableObject::new(&parent, &children);
                Self { mto, parent, children }
            }
        }
    }
}
