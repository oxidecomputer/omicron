//! Describes the Diesel database schema.
//!
//! NOTE: Should be kept up-to-date with dbinit.sql.

table! {
    disk (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        project_id -> Uuid,
        disk_state -> Text,
        attach_instance_id -> Nullable<Uuid>,
        state_generation -> Int8,
        time_state_updated -> Timestamptz,
        size_bytes -> Int8,
        origin_snapshot -> Nullable<Uuid>,
    }
}

table! {
    instance (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        project_id -> Uuid,
        state -> Text,
        time_state_updated -> Timestamptz,
        state_generation -> Int8,
        active_server_id -> Uuid,
        ncpus -> Int8,
        memory -> Int8,
        hostname -> Text,
    }
}

table! {
    metric_producer (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        ip -> Inet,
        port -> Int4,
        interval -> Float8,
        base_route -> Text,
        oximeter_id -> Uuid,
    }
}

table! {
    network_interface (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        vpc_id -> Uuid,
        subnet_id -> Uuid,
        mac -> Text,
        ip -> Inet,
    }
}

table! {
    organization (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,
    }
}

table! {
    oximeter (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        ip -> Inet,
        port -> Int4,
    }
}

table! {
    project (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        organization_id -> Uuid,
    }
}

table! {
    saga (id) {
        id -> Uuid,
        creator -> Uuid,
        template_name -> Text,
        time_created -> Timestamptz,
        saga_params -> Jsonb,
        saga_state -> Text,
        current_sec -> Nullable<Uuid>,
        adopt_generation -> Int8,
        adopt_time -> Timestamptz,
    }
}

table! {
    saga_node_event (saga_id, node_id, event_type) {
        saga_id -> Uuid,
        node_id -> Int8,
        event_type -> Text,
        data -> Nullable<Jsonb>,
        event_time -> Timestamptz,
        creator -> Uuid,
    }
}

table! {
    rack (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
    }
}

table! {
    console_session (token) {
        token -> Text,
        time_created -> Timestamptz,
        time_last_used -> Timestamptz,
        user_id -> Uuid,
    }
}

table! {
    sled (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        ip -> Inet,
        port -> Int4,
    }
}

table! {
    vpc (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        project_id -> Uuid,
        system_router_id -> Uuid,
        dns_name -> Text,
    }
}

table! {
    vpc_subnet (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        vpc_id -> Uuid,
        ipv4_block -> Nullable<Inet>,
        ipv6_block -> Nullable<Inet>,
    }
}

table! {
    vpc_router (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        kind -> crate::db::model::VpcRouterKindEnum,
        vpc_id -> Uuid,
        rcgen -> Int8,
    }
}

table! {
    router_route (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        kind -> crate::db::model::RouterRouteKindEnum,
        router_id -> Uuid,
        target -> Text,
        destination -> Text,
    }
}

allow_tables_to_appear_in_same_query!(
    disk,
    instance,
    metric_producer,
    network_interface,
    organization,
    oximeter,
    project,
    saga,
    saga_node_event,
    console_session,
    sled,
    router_route,
    vpc,
    vpc_subnet,
    vpc_router
);
