// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Service prosioning subqueries used by CTEs.

table! {
    sled_allocation_pool {
        id -> Uuid,
    }
}

table! {
    previously_allocated_services {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        sled_id -> Uuid,
        ip -> Inet,
        kind -> crate::ServiceKindEnum,
    }
}

table! {
    old_service_count (count) {
        count -> Int8,
    }
}

table! {
    new_service_count (count) {
        count -> Int8,
    }
}

table! {
    candidate_sleds {
        id -> Uuid,
    }
}

table! {
    new_internal_ips {
        id -> Uuid,
        last_used_address -> Inet,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    candidate_sleds,
    new_internal_ips,
);

table! {
    candidate_services {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        sled_id -> Uuid,
        ip -> Inet,
        kind -> crate::ServiceKindEnum,
    }
}

table! {
    inserted_services {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        sled_id -> Uuid,
        ip -> Inet,
        kind -> crate::ServiceKindEnum,
    }
}
