// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the Diesel database schema.
//!
//! NOTE: Should be kept up-to-date with dbinit.sql.

use omicron_common::api::external::SemverVersion;

table! {
    disk (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,
        project_id -> Uuid,
        volume_id -> Uuid,
        disk_state -> Text,
        attach_instance_id -> Nullable<Uuid>,
        state_generation -> Int8,
        time_state_updated -> Timestamptz,
        slot -> Nullable<Int2>,
        size_bytes -> Int8,
        block_size -> crate::BlockSizeEnum,
        origin_snapshot -> Nullable<Uuid>,
        origin_image -> Nullable<Uuid>,
        pantry_address -> Nullable<Text>,
    }
}

table! {
    image (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        silo_id -> Uuid,
        project_id -> Nullable<Uuid>,
        volume_id -> Uuid,
        url -> Nullable<Text>,
        os -> Text,
        version -> Text,
        digest -> Nullable<Text>,
        block_size -> crate::BlockSizeEnum,
        size_bytes -> Int8,
    }
}

table! {
    project_image (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        silo_id -> Uuid,
        project_id -> Uuid,
        volume_id -> Uuid,
        url -> Nullable<Text>,
        os -> Text,
        version -> Text,
        digest -> Nullable<Text>,
        block_size -> crate::BlockSizeEnum,
        size_bytes -> Int8,
    }
}

table! {
    silo_image (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        silo_id -> Uuid,
        volume_id -> Uuid,
        url -> Nullable<Text>,
        os -> Text,
        version -> Text,
        digest -> Nullable<Text>,
        block_size -> crate::BlockSizeEnum,
        size_bytes -> Int8,
    }
}

table! {
    switch_port (id) {
        id -> Uuid,
        rack_id -> Uuid,
        switch_location -> Text,
        port_name -> Text,
        port_settings_id -> Nullable<Uuid>,
    }
}

table! {
    switch_port_settings_group (id) {
        id -> Uuid,
        port_settings_id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
    }
}

table! {
    switch_port_settings_groups (port_settings_id, port_settings_group_id) {
        port_settings_id -> Uuid,
        port_settings_group_id -> Uuid,
    }
}

table! {
    switch_port_settings (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
    }
}

table! {
    switch_port_settings_port_config (port_settings_id) {
        port_settings_id -> Uuid,
        geometry -> crate::SwitchPortGeometryEnum,
    }
}

table! {
    switch_port_settings_link_config (port_settings_id, link_name) {
        port_settings_id -> Uuid,
        lldp_service_config_id -> Uuid,
        link_name -> Text,
        mtu -> Int4,
    }
}

table! {
    lldp_service_config (id) {
        id -> Uuid,
        enabled -> Bool,
        lldp_config_id -> Nullable<Uuid>,
    }
}

table! {
    lldp_config (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        chassis_id -> Text,
        system_name -> Text,
        system_description -> Text,
        management_ip -> Inet,
    }
}

table! {
    switch_port_settings_interface_config (id) {
        port_settings_id -> Uuid,
        id -> Uuid,
        interface_name -> Text,
        v6_enabled -> Bool,
        kind -> crate::DbSwitchInterfaceKindEnum,
    }
}

table! {
    switch_vlan_interface_config (interface_config_id, vid) {
        interface_config_id -> Uuid,
        vid -> Int4,
    }
}

table! {
    switch_port_settings_route_config (port_settings_id, interface_name, dst, gw, vid) {
        port_settings_id -> Uuid,
        interface_name -> Text,
        dst -> Inet,
        gw -> Inet,
        vid -> Nullable<Int4>,
    }
}

table! {
    switch_port_settings_bgp_peer_config (port_settings_id, interface_name, addr) {
        port_settings_id -> Uuid,
        bgp_announce_set_id -> Uuid,
        bgp_config_id -> Uuid,
        interface_name -> Text,
        addr -> Inet,
    }
}

table! {
    bgp_config (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        asn -> Int8,
        vrf -> Nullable<Text>,
    }
}

table! {
    bgp_announce_set (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
    }
}

table! {
    bgp_announcement (announce_set_id, network) {
        announce_set_id -> Uuid,
        address_lot_block_id -> Uuid,
        network -> Inet,
    }
}

table! {
    switch_port_settings_address_config (port_settings_id, address, interface_name) {
        port_settings_id -> Uuid,
        address_lot_block_id -> Uuid,
        rsvd_address_lot_block_id -> Uuid,
        address -> Inet,
        interface_name -> Text,
    }
}

table! {
    address_lot (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        kind -> crate::AddressLotKindEnum,
    }
}

table! {
    address_lot_block (id) {
        id -> Uuid,
        address_lot_id -> Uuid,
        first_address -> Inet,
        last_address -> Inet,
    }
}

table! {
    address_lot_rsvd_block (id) {
        id -> Uuid,
        address_lot_id -> Uuid,
        first_address -> Inet,
        last_address -> Inet,
        anycast -> Bool,
    }
}

table! {
    loopback_address (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        address_lot_block_id -> Uuid,
        rsvd_address_lot_block_id -> Uuid,
        rack_id -> Uuid,
        switch_location -> Text,
        address -> Inet,
        anycast -> Bool,
    }
}

table! {
    global_image (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        volume_id -> Uuid,
        url -> Nullable<Text>,
        distribution -> Text,
        version -> Text,
        digest -> Nullable<Text>,
        block_size -> crate::BlockSizeEnum,
        size_bytes -> Int8,
    }
}

table! {
    snapshot (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        project_id -> Uuid,
        disk_id -> Uuid,
        volume_id -> Uuid,

        destination_volume_id -> Uuid,

        gen -> Int8,
        state -> crate::SnapshotStateEnum,
        block_size -> crate::BlockSizeEnum,
        size_bytes -> Int8,
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
        user_data -> Binary,
        ncpus -> Int8,
        memory -> Int8,
        hostname -> Text,
        boot_on_fault -> Bool,
        state -> crate::InstanceStateEnum,
        time_state_updated -> Timestamptz,
        state_generation -> Int8,
        active_propolis_id -> Nullable<Uuid>,
        target_propolis_id -> Nullable<Uuid>,
        migration_id -> Nullable<Uuid>,
    }
}

table! {
    vmm (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        instance_id -> Uuid,
        sled_id -> Uuid,
        propolis_ip -> Inet,
        state -> crate::InstanceStateEnum,
        time_state_updated -> Timestamptz,
        state_generation -> Int8,
    }
}

table! {
    sled_instance (id) {
        id -> Uuid,
        name -> Text,
        silo_name -> Text,
        project_name -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        state -> crate::InstanceStateEnum,
        active_sled_id -> Uuid,
        migration_id -> Nullable<Uuid>,
        ncpus -> Int8,
        memory -> Int8,
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
        kind -> crate::NetworkInterfaceKindEnum,
        parent_id -> Uuid,
        vpc_id -> Uuid,
        subnet_id -> Uuid,
        mac -> Int8,
        ip -> Inet,
        slot -> Int2,
        is_primary -> Bool,
    }
}

table! {
    instance_network_interface (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        instance_id -> Uuid,
        vpc_id -> Uuid,
        subnet_id -> Uuid,
        mac -> Int8,
        ip -> Inet,
        slot -> Int2,
        is_primary -> Bool,
    }
}

table! {
    service_network_interface (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        service_id -> Uuid,
        vpc_id -> Uuid,
        subnet_id -> Uuid,
        mac -> Int8,
        ip -> Inet,
        slot -> Int2,
        is_primary -> Bool,
    }
}

table! {
    ip_pool (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,
        silo_id -> Nullable<Uuid>,
        is_default -> Bool,
    }
}

table! {
    ip_pool_range (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        first_address -> Inet,
        last_address -> Inet,
        ip_pool_id -> Uuid,
        rcgen -> Int8,
    }
}

table! {
    external_ip (id) {
        id -> Uuid,
        name -> Nullable<Text>,
        description -> Nullable<Text>,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        ip_pool_id -> Uuid,
        ip_pool_range_id -> Uuid,
        is_service -> Bool,
        parent_id -> Nullable<Uuid>,
        kind -> crate::IpKindEnum,
        ip -> Inet,
        first_port -> Int4,
        last_port -> Int4,
    }
}

table! {
    silo (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        discoverable -> Bool,
        authentication_mode -> crate::AuthenticationModeEnum,
        user_provision_type -> crate::UserProvisionTypeEnum,

        mapped_fleet_roles -> Jsonb,

        rcgen -> Int8,
    }
}

table! {
    silo_user (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        silo_id -> Uuid,
        external_id -> Text,
    }
}

table! {
    silo_user_password_hash (silo_user_id) {
        silo_user_id -> Uuid,
        hash -> Text,
        time_created -> Timestamptz,
    }
}

table! {
    silo_group (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        silo_id -> Uuid,
        external_id -> Text,
    }
}

table! {
    silo_group_membership (silo_group_id, silo_user_id) {
        silo_group_id -> Uuid,
        silo_user_id -> Uuid,
    }
}

allow_tables_to_appear_in_same_query!(silo_user, silo_user_password_hash);
allow_tables_to_appear_in_same_query!(
    silo_group,
    silo_group_membership,
    silo_user,
);
allow_tables_to_appear_in_same_query!(role_assignment, silo_group_membership);

table! {
    identity_provider (silo_id, id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        silo_id -> Uuid,
        provider_type -> crate::IdentityProviderTypeEnum,
    }
}

table! {
    saml_identity_provider (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        silo_id -> Uuid,

        idp_metadata_document_string -> Text,

        idp_entity_id -> Text,
        sp_client_id -> Text,
        acs_url -> Text,
        slo_url -> Text,
        technical_contact_email -> Text,
        public_cert -> Nullable<Text>,
        private_key -> Nullable<Text>,
        group_attribute_name -> Nullable<Text>,
    }
}

table! {
    ssh_key (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        silo_user_id -> Uuid,
        public_key -> Text,
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
        rcgen -> Int8,
        silo_id -> Uuid,
    }
}

table! {
    saga (id) {
        id -> Uuid,
        creator -> Uuid,
        time_created -> Timestamptz,
        name -> Text,
        saga_dag -> Jsonb,
        saga_state -> crate::saga_types::SagaCachedStateEnum,
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
        initialized -> Bool,
        tuf_base_url -> Nullable<Text>,
    }
}

table! {
    console_session (token) {
        token -> Text,
        time_created -> Timestamptz,
        time_last_used -> Timestamptz,
        silo_user_id -> Uuid,
    }
}

table! {
    sled (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,

        rack_id -> Uuid,
        is_scrimlet -> Bool,
        serial_number -> Text,
        part_number -> Text,
        revision -> Int8,

        usable_hardware_threads -> Int8,
        usable_physical_ram -> Int8,
        reservoir_size -> Int8,

        ip -> Inet,
        port -> Int4,
        last_used_address -> Inet,
    }
}

table! {
    sled_resource (id) {
        id -> Uuid,
        sled_id -> Uuid,
        kind -> crate::SledResourceKindEnum,
        hardware_threads -> Int8,
        rss_ram -> Int8,
        reservoir_ram -> Int8,
    }
}

table! {
    switch (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,

        rack_id -> Uuid,
        serial_number -> Text,
        part_number -> Text,
        revision -> Int8,
    }
}

table! {
    service (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        sled_id -> Uuid,
        zone_id -> Nullable<Uuid>,
        ip -> Inet,
        port -> Int4,
        kind -> crate::ServiceKindEnum,
    }
}

table! {
    physical_disk (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,

        vendor -> Text,
        serial -> Text,
        model -> Text,

        variant -> crate::PhysicalDiskKindEnum,
        sled_id -> Uuid,
    }
}

table! {
    certificate (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        silo_id -> Uuid,
        service -> crate::ServiceKindEnum,
        cert -> Binary,
        key -> Binary,
    }
}

table! {
    virtual_provisioning_collection {
        id -> Uuid,
        // This type isn't actually "Nullable" - it's just handy to use the
        // same type for insertion and querying, and doing so requires this
        // field to appear optional so we can let this (default) field appear
        // optional.
        time_modified -> Nullable<Timestamptz>,
        collection_type -> Text,
        virtual_disk_bytes_provisioned -> Int8,
        cpus_provisioned -> Int8,
        ram_provisioned -> Int8,
    }
}

table! {
    virtual_provisioning_resource {
        id -> Uuid,
        // This type isn't actually "Nullable" - it's just handy to use the
        // same type for insertion and querying, and doing so requires this
        // field to appear optional so we can let this (default) field appear
        // optional.
        time_modified -> Nullable<Timestamptz>,
        resource_type -> Text,
        virtual_disk_bytes_provisioned -> Int8,
        cpus_provisioned -> Int8,
        ram_provisioned -> Int8,
    }
}

table! {
    zpool (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,

        sled_id -> Uuid,
        physical_disk_id -> Uuid,

        total_size -> Int8,
    }
}

table! {
    dataset (id) {
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

table! {
    region (id) {
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
    region_snapshot (dataset_id, region_id, snapshot_id) {
        dataset_id -> Uuid,
        region_id -> Uuid,
        snapshot_id -> Uuid,
        snapshot_addr -> Text,
        volume_references -> Int8,
        deleting -> Bool,
    }
}

table! {
    volume (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,

        data -> Text,

        /*
         * During volume deletion, a serialized list of Crucible resources to
         * clean up will be written here, along with setting time_deleted.
         */
        resources_to_clean_up -> Nullable<Text>,
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
        vni -> Int4,
        ipv6_prefix -> Inet,
        dns_name -> Text,
        firewall_gen -> Int8,
        subnet_gen -> Int8,
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
        rcgen -> Int8,
        ipv4_block -> Inet,
        ipv6_block -> Inet,
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
        kind -> crate::VpcRouterKindEnum,
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
        kind -> crate::RouterRouteKindEnum,
        vpc_router_id -> Uuid,
        target -> Text,
        destination -> Text,
    }
}

table! {
    use diesel::sql_types::*;

    vpc_firewall_rule (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        vpc_id -> Uuid,
        status -> crate::VpcFirewallRuleStatusEnum,
        direction -> crate::VpcFirewallRuleDirectionEnum,
        targets -> Array<Text>,
        filter_hosts -> Nullable<Array<Text>>,
        filter_ports -> Nullable<Array<Text>>,
        filter_protocols -> Nullable<Array<crate::VpcFirewallRuleProtocolEnum>>,
        action -> crate::VpcFirewallRuleActionEnum,
        priority -> Int4,
    }
}

table! {
    dns_zone (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        dns_group -> crate::DnsGroupEnum,
        zone_name -> Text,
    }
}

table! {
    dns_version (dns_group, version) {
        dns_group -> crate::DnsGroupEnum,
        version -> Int8,
        time_created -> Timestamptz,
        creator -> Text,
        comment -> Text,
    }
}

table! {
    dns_name (dns_zone_id, version_added, name) {
        dns_zone_id -> Uuid,
        version_added -> Int8,
        version_removed -> Nullable<Int8>,
        name -> Text,
        dns_record_data -> Jsonb,
    }
}

table! {
    user_builtin (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
    }
}

table! {
    device_auth_request (user_code) {
        user_code -> Text,
        client_id -> Uuid,
        device_code -> Text,
        time_created -> Timestamptz,
        time_expires -> Timestamptz,
    }
}

table! {
    device_access_token (token) {
        token -> Text,
        client_id -> Uuid,
        device_code -> Text,
        silo_user_id -> Uuid,
        time_requested -> Timestamptz,
        time_created -> Timestamptz,
        time_expires -> Nullable<Timestamptz>,
    }
}

table! {
    role_builtin (resource_type, role_name) {
        resource_type -> Text,
        role_name -> Text,
        description -> Text,
    }
}

table! {
    role_assignment (
        identity_type,
        identity_id,
        resource_type,
        resource_id,
        role_name
    ) {
        identity_type -> crate::IdentityTypeEnum,
        identity_id -> Uuid,
        resource_type -> Text,
        role_name -> Text,
        resource_id -> Uuid,
    }
}

table! {
    update_artifact (name, version, kind) {
        name -> Text,
        version -> Text,
        kind -> crate::KnownArtifactKindEnum,
        targets_role_version -> Int8,
        valid_until -> Timestamptz,
        target_name -> Text,
        target_sha256 -> Text,
        target_length -> Int8,
    }
}

table! {
    system_update (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        version -> Text,
    }
}

table! {
    update_deployment (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        version -> Text,
        status -> crate::UpdateStatusEnum,
        // TODO: status reason for updateable_component
    }
}

table! {
    component_update (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        version -> Text,
        component_type -> crate::UpdateableComponentTypeEnum,
    }
}

table! {
    updateable_component (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        device_id -> Text,
        version -> Text,
        system_version -> Text,
        component_type -> crate::UpdateableComponentTypeEnum,
        status -> crate::UpdateStatusEnum,
        // TODO: status reason for updateable_component
    }
}

table! {
    system_update_component_update (system_update_id, component_update_id) {
        system_update_id -> Uuid,
        component_update_id -> Uuid,
    }
}

/* hardware inventory */

table! {
    hw_baseboard_id (id) {
        id -> Uuid,
        part_number -> Text,
        serial_number -> Text,
    }
}

table! {
    sw_caboose (id) {
        id -> Uuid,
        board -> Text,
        git_commit -> Text,
        name -> Text,
        version -> Text,
    }
}

table! {
    inv_collection (id) {
        id -> Uuid,
        time_started -> Timestamptz,
        time_done -> Timestamptz,
        collector -> Text,
    }
}

table! {
    inv_collection_error (inv_collection_id, idx) {
        inv_collection_id -> Uuid,
        idx -> Int4,
        message -> Text,
    }
}

table! {
    inv_service_processor (inv_collection_id, hw_baseboard_id) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        sp_type -> crate::SpTypeEnum,
        sp_slot -> Int4,

        baseboard_revision -> Int8,
        hubris_archive_id -> Text,
        power_state -> crate::HwPowerStateEnum,
    }
}

table! {
    inv_root_of_trust (inv_collection_id, hw_baseboard_id) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        slot_active -> crate::HwRotSlotEnum,
        slot_boot_pref_transient -> Nullable<crate::HwRotSlotEnum>,
        slot_boot_pref_persistent -> crate::HwRotSlotEnum,
        slot_boot_pref_persistent_pending -> Nullable<crate::HwRotSlotEnum>,
        slot_a_sha3_256 -> Nullable<Text>,
        slot_b_sha3_256 -> Nullable<Text>,
    }
}

table! {
    inv_caboose (inv_collection_id, hw_baseboard_id, which) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        which -> crate::CabooseWhichEnum,
        sw_caboose_id -> Uuid,
    }
}

table! {
    db_metadata (singleton) {
        singleton -> Bool,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        version -> Text,
        target_version -> Nullable<Text>,
    }
}

/// The version of the database schema this particular version of Nexus was
/// built against.
///
/// This should be updated whenever the schema is changed. For more details,
/// refer to: schema/crdb/README.adoc
pub const SCHEMA_VERSION: SemverVersion = SemverVersion::new(7, 0, 0);

allow_tables_to_appear_in_same_query!(
    system_update,
    component_update,
    system_update_component_update,
);
joinable!(system_update_component_update -> component_update (component_update_id));

allow_tables_to_appear_in_same_query!(ip_pool_range, ip_pool);
joinable!(ip_pool_range -> ip_pool (ip_pool_id));

allow_tables_to_appear_in_same_query!(inv_collection, inv_collection_error);
joinable!(inv_collection_error -> inv_collection (inv_collection_id));
allow_tables_to_appear_in_same_query!(sw_caboose, inv_caboose);

allow_tables_to_appear_in_same_query!(
    dataset,
    disk,
    image,
    project_image,
    silo_image,
    instance,
    metric_producer,
    network_interface,
    instance_network_interface,
    service_network_interface,
    oximeter,
    project,
    rack,
    region,
    region_snapshot,
    saga,
    saga_node_event,
    silo,
    identity_provider,
    console_session,
    service,
    sled,
    sled_resource,
    router_route,
    vmm,
    volume,
    vpc,
    vpc_subnet,
    vpc_router,
    vpc_firewall_rule,
    user_builtin,
    role_builtin,
    role_assignment,
);

allow_tables_to_appear_in_same_query!(dns_zone, dns_version, dns_name);
allow_tables_to_appear_in_same_query!(external_ip, service);

allow_tables_to_appear_in_same_query!(
    switch_port,
    switch_port_settings_route_config
);
