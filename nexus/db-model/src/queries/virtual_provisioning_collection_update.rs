// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the resource provisioning update CTE
//!
//! Refer to <nexus/src/db/queries/virtual_provisioning_collection_update.rs>
//! for the construction of this query.

use crate::schema::organization;
use crate::schema::silo;
use crate::schema::virtual_provisioning_collection;

table! {
    parent_org {
        id -> Uuid,
    }
}

table! {
    parent_silo {
        id -> Uuid,
    }
}

table! {
    all_collections {
        id -> Uuid,
    }
}

table! {
    do_update (update) {
        update -> Bool,
    }
}

diesel::allow_tables_to_appear_in_same_query!(organization, parent_org,);
diesel::allow_tables_to_appear_in_same_query!(silo, parent_silo,);

diesel::allow_tables_to_appear_in_same_query!(
    virtual_provisioning_collection,
    parent_org,
    parent_silo,
    all_collections,
    do_update,
);
