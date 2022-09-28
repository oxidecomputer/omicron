// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes subqueries which may be issues as a part of CTEs.
//!
//! When possible, it's preferable to define subqueries close to their
//! usage. However, certain Diesel traits (such as those enabling joins)
//! require the table structures to be defined in the same crate.

// TODO: We're currently piggy-backing on the table macro for convenience.
// We actually do not want to generate an entire table for each subquery - we'd
// like to have a query source (which we can use to generate SELECT statements,
// JOIN, etc), but we don't want this to be an INSERT/UPDATE/DELETE target.
//
// Similarly, we don't want to force callers to supply a "primary key".
//
// I've looked into Diesel's `alias!` macro for this purpose, but unfortunately
// that implementation is too opinionated about the output QueryFragment.
// It expects to use the form:
//
// "<SOURCE> as <ALIAS NAME>", which is actually the opposite of what we want in
// a CTE (where we want the alias name to come first).

use crate::schema::dataset;
use crate::schema::zpool;

table! {
    old_regions {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        dataset_id -> Uuid,
        volume_id -> Uuid,

        block_size -> Int8,
        blocks_per_extent -> Int8,
        extent_count -> Int8,
    }
}

table! {
    candidate_datasets {
        id -> Uuid,
        pool_id -> Uuid,
    }
}

table! {
    candidate_zpools {
        id -> Uuid,
        total_size -> Int8,
    }
}

table! {
    candidate_regions {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        dataset_id -> Uuid,
        volume_id -> Uuid,

        block_size -> Int8,
        blocks_per_extent -> Int8,
        extent_count -> Int8,
    }
}

table! {
    zpool_size_delta (pool_id) {
        pool_id -> Uuid,
        size_used_delta -> Numeric,
    }
}

table! {
    proposed_dataset_changes {
        id -> Uuid,
        pool_id -> Uuid,
        size_used_delta -> Int8,
    }
}

table! {
    old_zpool_usage (pool_id) {
        pool_id -> Uuid,
        size_used -> Numeric,
    }
}

table! {
    proposed_datasets_fit (fits) {
        fits -> Bool,
    }
}

table! {
    do_insert (insert) {
        insert -> Bool,
    }
}

table! {
    inserted_regions {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        dataset_id -> Uuid,
        volume_id -> Uuid,

        block_size -> Int8,
        blocks_per_extent -> Int8,
        extent_count -> Int8,
    }
}

table! {
    updated_datasets (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,

        pool_id -> Uuid,

        ip -> Inet,
        port -> Int4,

        kind -> crate::DatasetKindEnum,
        size_used -> Nullable<Int8>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(candidate_datasets, zpool,);

diesel::allow_tables_to_appear_in_same_query!(
    proposed_dataset_changes,
    dataset,
);

diesel::allow_tables_to_appear_in_same_query!(
    do_insert,
    candidate_regions,
    dataset,
    zpool,
);

diesel::allow_tables_to_appear_in_same_query!(candidate_zpools, dataset,);

diesel::allow_tables_to_appear_in_same_query!(
    old_zpool_usage,
    zpool,
    zpool_size_delta,
    proposed_dataset_changes,
);

diesel::allow_tables_to_appear_in_same_query!(old_regions, dataset,);

diesel::allow_tables_to_appear_in_same_query!(
    inserted_regions,
    updated_datasets,
);
