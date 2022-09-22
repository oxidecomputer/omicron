// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes subqueries which may be issues as a part of CTEs.
//!
//! When possible, it's preferable to define subqueries close to their
//! usage. However, certain Diesel traits (such as those enabling joins)
//! require the table structures to be defined in the same crate.

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
    updated_datasets {
        id -> Uuid,
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
