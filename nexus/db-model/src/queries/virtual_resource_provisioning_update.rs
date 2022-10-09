// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the resource usage update CTE

use crate::schema::organization;
use crate::schema::silo;
use crate::schema::virtual_resource_provisioning;

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
    parent_fleet {
        id -> Uuid,
    }
}

table! {
    all_collections {
        id -> Uuid,
    }
}

diesel::allow_tables_to_appear_in_same_query!(organization, parent_org,);
diesel::allow_tables_to_appear_in_same_query!(silo, parent_silo,);

diesel::allow_tables_to_appear_in_same_query!(
    virtual_resource_provisioning,
    parent_org,
    parent_silo,
    parent_fleet,
    all_collections,
);
