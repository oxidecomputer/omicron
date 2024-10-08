// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
        link_name -> Text,
        mtu -> Int4,
        fec -> crate::SwitchLinkFecEnum,
        speed -> crate::SwitchLinkSpeedEnum,
        autoneg -> Bool,
        lldp_link_config_id -> Nullable<Uuid>,
    }
}

table! {
    lldp_link_config (id) {
        id -> Uuid,
        enabled -> Bool,
        link_name -> Nullable<Text>,
        link_description -> Nullable<Text>,
        chassis_id -> Nullable<Text>,
        system_name -> Nullable<Text>,
        system_description -> Nullable<Text>,
        management_ip -> Nullable<Inet>,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
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
    switch_port_settings_route_config (port_settings_id, interface_name, dst, gw) {
        port_settings_id -> Uuid,
        interface_name -> Text,
        dst -> Inet,
        gw -> Inet,
        vid -> Nullable<Int4>,
        local_pref -> Nullable<Int2>,
    }
}

table! {
    switch_port_settings_bgp_peer_config (port_settings_id, interface_name, addr) {
        port_settings_id -> Uuid,
        bgp_config_id -> Uuid,
        interface_name -> Text,
        addr -> Inet,
        hold_time -> Int8,
        idle_hold_time -> Int8,
        delay_open -> Int8,
        connect_retry -> Int8,
        keepalive -> Int8,
        remote_asn -> Nullable<Int8>,
        min_ttl -> Nullable<Int2>,
        md5_auth_key -> Nullable<Text>,
        multi_exit_discriminator -> Nullable<Int8>,
        local_pref -> Nullable<Int8>,
        enforce_first_as -> Bool,
        allow_import_list_active -> Bool,
        allow_export_list_active -> Bool,
        vlan_id -> Nullable<Int4>
    }
}

table! {
    switch_port_settings_bgp_peer_config_communities (port_settings_id, interface_name, addr, community) {
        port_settings_id -> Uuid,
        interface_name -> Text,
        addr -> Inet,
        community -> Int8,
    }
}

table! {
    switch_port_settings_bgp_peer_config_allow_export (port_settings_id, interface_name, addr, prefix) {
        port_settings_id -> Uuid,
        interface_name -> Text,
        addr -> Inet,
        prefix -> Inet,
    }
}

table! {
    switch_port_settings_bgp_peer_config_allow_import (port_settings_id, interface_name, addr, prefix) {
        port_settings_id -> Uuid,
        interface_name -> Text,
        addr -> Inet,
        prefix -> Inet,
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
        bgp_announce_set_id -> Uuid,
        vrf -> Nullable<Text>,
        shaper -> Nullable<Text>,
        checker -> Nullable<Text>,
    }
}

table! {
    bgp_peer_view (switch_location, port_name) {
        switch_location -> Text,
        port_name -> Text,
        addr -> Inet,
        asn -> Int8,
        connect_retry -> Int8,
        delay_open -> Int8,
        hold_time -> Int8,
        idle_hold_time -> Int8,
        keepalive -> Int8,
        remote_asn -> Nullable<Int8>,
        min_ttl -> Nullable<Int8>,
        md5_auth_key -> Nullable<Text>,
        multi_exit_discriminator -> Nullable<Int8>,
        local_pref -> Nullable<Int8>,
        enforce_first_as -> Bool,
        vlan_id -> Nullable<Int4>,
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
        vlan_id -> Nullable<Int4>,
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
        auto_restart_policy -> Nullable<crate::InstanceAutoRestartPolicyEnum>,
        auto_restart_cooldown -> Nullable<Interval>,
        boot_disk_id -> Nullable<Uuid>,
        time_state_updated -> Timestamptz,
        state_generation -> Int8,
        active_propolis_id -> Nullable<Uuid>,
        target_propolis_id -> Nullable<Uuid>,
        migration_id -> Nullable<Uuid>,
        state -> crate::InstanceStateEnum,
        time_last_auto_restarted -> Nullable<Timestamptz>,
        updater_id -> Nullable<Uuid>,
        updater_gen-> Int8,
    }
}

joinable!(instance -> vmm (active_propolis_id));

table! {
    vmm (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        instance_id -> Uuid,
        sled_id -> Uuid,
        propolis_ip -> Inet,
        propolis_port -> Int4,
        time_state_updated -> Timestamptz,
        state_generation -> Int8,
        state -> crate::VmmStateEnum,
    }
}
joinable!(vmm -> sled (sled_id));

table! {
    sled_instance (id) {
        id -> Uuid,
        name -> Text,
        silo_name -> Text,
        project_name -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        state -> crate::VmmStateEnum,
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
        kind -> crate::ProducerKindEnum,
        ip -> Inet,
        port -> Int4,
        interval -> Float8,
        oximeter_id -> Uuid,
    }
}

table! {
    silo_quotas(silo_id) {
        silo_id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        cpus -> Int8,
        memory_bytes -> Int8,
        storage_bytes -> Int8,
    }
}

table! {
    silo_utilization(silo_id) {
        silo_id -> Uuid,
        silo_name -> Text,
        silo_discoverable -> Bool,
        cpus_provisioned -> Int8,
        memory_provisioned -> Int8,
        storage_provisioned -> Int8,
        cpus_allocated -> Int8,
        memory_allocated -> Int8,
        storage_allocated -> Int8,
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
        transit_ips -> Array<Inet>,
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
        transit_ips -> Array<Inet>,
    }
}
joinable!(instance_network_interface -> instance (instance_id));

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
    }
}

table! {
    ip_pool_resource (ip_pool_id, resource_type, resource_id) {
        ip_pool_id -> Uuid,
        resource_type -> crate::IpPoolResourceTypeEnum,
        resource_id -> Uuid,
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
    ipv4_nat_entry (id) {
        id -> Uuid,
        external_address -> Inet,
        first_port -> Int4,
        last_port -> Int4,
        sled_address -> Inet,
        vni -> Int4,
        mac -> Int8,
        version_added -> Int8,
        version_removed -> Nullable<Int8>,
        time_created -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
    }
}

// View used for summarizing changes to ipv4_nat_entry
table! {
    ipv4_nat_changes (version) {
        external_address -> Inet,
        first_port -> Int4,
        last_port -> Int4,
        sled_address -> Inet,
        vni -> Int4,
        mac -> Int8,
        version -> Int8,
        deleted -> Bool,
    }
}

// This is the sequence used for the version number
// in ipv4_nat_entry.
table! {
    ipv4_nat_version (last_value) {
        last_value -> Int8,
        log_cnt -> Int8,
        is_called -> Bool,
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

        project_id -> Nullable<Uuid>,
        state -> crate::IpAttachStateEnum,
        is_probe -> Bool,
    }
}

table! {
    floating_ip (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        ip_pool_id -> Uuid,
        ip_pool_range_id -> Uuid,
        is_service -> Bool,
        parent_id -> Nullable<Uuid>,
        ip -> Inet,
        project_id -> Uuid,
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
    instance_ssh_key (instance_id, ssh_key_id) {
        instance_id -> Uuid,
        ssh_key_id -> Uuid,
    }
}

table! {
    oximeter (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_expunged -> Nullable<Timestamptz>,
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
        rack_subnet -> Nullable<Inet>,
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
        sled_policy -> crate::sled_policy::SledPolicyEnum,
        sled_state -> crate::SledStateEnum,
        sled_agent_gen -> Int8,
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
    sled_underlay_subnet_allocation (rack_id, sled_id) {
        rack_id -> Uuid,
        sled_id -> Uuid,
        subnet_octet -> Int2,
        hw_baseboard_id -> Uuid,
    }
}
allow_tables_to_appear_in_same_query!(rack, sled_underlay_subnet_allocation);

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
        disk_policy -> crate::PhysicalDiskPolicyEnum,
        disk_state -> crate::PhysicalDiskStateEnum,
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

allow_tables_to_appear_in_same_query! {
    virtual_provisioning_resource,
    instance
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
    }
}

allow_tables_to_appear_in_same_query! {
    zpool,
    physical_disk
}

table! {
    dataset (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,

        pool_id -> Uuid,

        ip -> Nullable<Inet>,
        port -> Nullable<Int4>,

        kind -> crate::DatasetKindEnum,
        size_used -> Nullable<Int8>,
        zone_name -> Nullable<Text>,
    }
}

allow_tables_to_appear_in_same_query!(zpool, dataset);

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

        port -> Nullable<Int4>,

        read_only -> Bool,
    }
}

allow_tables_to_appear_in_same_query!(zpool, region);

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
        custom_router_id -> Nullable<Uuid>,
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
        resolved_version -> Int8,
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
    internet_gateway(id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        vpc_id -> Uuid,
        rcgen -> Int8,
        resolved_version -> Int8,
    }
}

table! {
    internet_gateway_ip_pool(id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        internet_gateway_id -> Uuid,
        ip_pool_id -> Uuid,
    }
}

table! {
    internet_gateway_ip_address(id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        internet_gateway_id -> Uuid,
        address -> Inet,
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
    tuf_repo (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        sha256 -> Text,
        targets_role_version -> Int8,
        valid_until -> Timestamptz,
        system_version -> Text,
        file_name -> Text,
    }
}

table! {
    tuf_artifact (name, version, kind) {
        name -> Text,
        version -> Text,
        kind -> Text,
        time_created -> Timestamptz,
        sha256 -> Text,
        artifact_size -> Int8,
    }
}

table! {
    tuf_repo_artifact (tuf_repo_id, tuf_artifact_name, tuf_artifact_version, tuf_artifact_kind) {
        tuf_repo_id -> Uuid,
        tuf_artifact_name -> Text,
        tuf_artifact_version -> Text,
        tuf_artifact_kind -> Text,
    }
}

allow_tables_to_appear_in_same_query!(
    tuf_repo,
    tuf_repo_artifact,
    tuf_artifact
);
joinable!(tuf_repo_artifact -> tuf_repo (tuf_repo_id));
// Can't specify joinable for a composite primary key (tuf_repo_artifact ->
// tuf_artifact).

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
    sw_root_of_trust_page (id) {
        id -> Uuid,
        data_base64 -> Text,
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
        stage0_fwid -> Nullable<Text>,
        stage0next_fwid -> Nullable<Text>,

        slot_a_error -> Nullable<crate::RotImageErrorEnum>,
        slot_b_error -> Nullable<crate::RotImageErrorEnum>,
        stage0_error -> Nullable<crate::RotImageErrorEnum>,
        stage0next_error -> Nullable<crate::RotImageErrorEnum>,
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
    inv_root_of_trust_page (inv_collection_id, hw_baseboard_id, which) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        which -> crate::RotPageWhichEnum,
        sw_root_of_trust_page_id -> Uuid,
    }
}

table! {
    inv_sled_agent (inv_collection_id, sled_id) {
        inv_collection_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,
        sled_id -> Uuid,

        hw_baseboard_id -> Nullable<Uuid>,

        sled_agent_ip -> Inet,
        sled_agent_port -> Int4,
        sled_role -> crate::SledRoleEnum,
        usable_hardware_threads -> Int8,
        usable_physical_ram -> Int8,
        reservoir_size -> Int8,
    }
}

table! {
    inv_physical_disk (inv_collection_id, sled_id, slot) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        slot -> Int8,

        vendor -> Text,
        model -> Text,
        serial -> Text,

        variant -> crate::PhysicalDiskKindEnum,
    }
}

table! {
    inv_nvme_disk_firmware (inv_collection_id, sled_id, slot) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        slot -> Int8,

        number_of_slots -> Int2,
        active_slot -> Int2,
        next_active_slot -> Nullable<Int2>,
        slot1_is_read_only -> Bool,
        slot_firmware_versions -> Array<Nullable<Text>>,
    }
}

table! {
    inv_zpool (inv_collection_id, sled_id, id) {
        inv_collection_id -> Uuid,
        time_collected -> Timestamptz,
        id -> Uuid,
        sled_id -> Uuid,
        total_size -> Int8,
    }
}

table! {
    inv_dataset (inv_collection_id, sled_id, name) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,

        id -> Nullable<Uuid>,
        name -> Text,
        available -> Int8,
        used -> Int8,
        quota -> Nullable<Int8>,
        reservation -> Nullable<Int8>,
        compression -> Text,
    }
}

table! {
    inv_sled_omicron_zones (inv_collection_id, sled_id) {
        inv_collection_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,
        sled_id -> Uuid,

        generation -> Int8,
    }
}

table! {
    inv_omicron_zone (inv_collection_id, id) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,

        id -> Uuid,
        underlay_address -> Inet,
        zone_type -> crate::ZoneTypeEnum,

        primary_service_ip -> Inet,
        primary_service_port -> Int4,
        second_service_ip -> Nullable<Inet>,
        second_service_port -> Nullable<Int4>,
        dataset_zpool_name -> Nullable<Text>,
        nic_id -> Nullable<Uuid>,
        dns_gz_address -> Nullable<Inet>,
        dns_gz_address_index -> Nullable<Int8>,
        ntp_ntp_servers -> Nullable<Array<Text>>,
        ntp_dns_servers -> Nullable<Array<Inet>>,
        ntp_domain -> Nullable<Text>,
        nexus_external_tls -> Nullable<Bool>,
        nexus_external_dns_servers -> Nullable<Array<Inet>>,
        snat_ip -> Nullable<Inet>,
        snat_first_port -> Nullable<Int4>,
        snat_last_port -> Nullable<Int4>,
        filesystem_pool -> Nullable<Uuid>,
    }
}

table! {
    inv_omicron_zone_nic (inv_collection_id, id) {
        inv_collection_id -> Uuid,
        id -> Uuid,
        name -> Text,
        ip -> Inet,
        mac -> Int8,
        subnet -> Inet,
        vni -> Int8,
        is_primary -> Bool,
        slot -> Int2,
    }
}

/* blueprints */

table! {
    blueprint (id) {
        id -> Uuid,

        parent_blueprint_id -> Nullable<Uuid>,

        time_created -> Timestamptz,
        creator -> Text,
        comment -> Text,

        internal_dns_version -> Int8,
        external_dns_version -> Int8,
        cockroachdb_fingerprint -> Text,

        cockroachdb_setting_preserve_downgrade -> Nullable<Text>,
    }
}

table! {
    bp_target (version) {
        version -> Int8,

        blueprint_id -> Uuid,

        enabled -> Bool,
        time_made_target -> Timestamptz,
    }
}

table! {
    bp_sled_state (blueprint_id, sled_id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,

        sled_state -> crate::SledStateEnum,
    }
}

table! {
    bp_sled_omicron_physical_disks (blueprint_id, sled_id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,

        generation -> Int8,
    }
}

table! {
    bp_omicron_physical_disk (blueprint_id, id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,

        vendor -> Text,
        serial -> Text,
        model -> Text,

        id -> Uuid,
        pool_id -> Uuid,
    }
}

table! {
    bp_sled_omicron_zones (blueprint_id, sled_id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,

        generation -> Int8,
    }
}

table! {
    bp_omicron_zone (blueprint_id, id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,

        id -> Uuid,
        underlay_address -> Inet,
        zone_type -> crate::ZoneTypeEnum,

        primary_service_ip -> Inet,
        primary_service_port -> Int4,
        second_service_ip -> Nullable<Inet>,
        second_service_port -> Nullable<Int4>,
        dataset_zpool_name -> Nullable<Text>,
        bp_nic_id -> Nullable<Uuid>,
        dns_gz_address -> Nullable<Inet>,
        dns_gz_address_index -> Nullable<Int8>,
        ntp_ntp_servers -> Nullable<Array<Text>>,
        ntp_dns_servers -> Nullable<Array<Inet>>,
        ntp_domain -> Nullable<Text>,
        nexus_external_tls -> Nullable<Bool>,
        nexus_external_dns_servers -> Nullable<Array<Inet>>,
        snat_ip -> Nullable<Inet>,
        snat_first_port -> Nullable<Int4>,
        snat_last_port -> Nullable<Int4>,
        disposition -> crate::DbBpZoneDispositionEnum,
        external_ip_id -> Nullable<Uuid>,
        filesystem_pool -> Nullable<Uuid>,
    }
}

table! {
    bp_omicron_zone_nic (blueprint_id, id) {
        blueprint_id -> Uuid,
        id -> Uuid,
        name -> Text,
        ip -> Inet,
        mac -> Int8,
        subnet -> Inet,
        vni -> Int8,
        is_primary -> Bool,
        slot -> Int2,
    }
}

table! {
    bp_clickhouse_cluster_config (blueprint_id) {
        blueprint_id -> Uuid,
        generation -> Int8,
        max_used_server_id -> Int8,
        max_used_keeper_id -> Int8,
        cluster_name -> Text,
        cluster_secret -> Text,
        highest_seen_keeper_leader_committed_log_index -> Int8,
    }
}

table! {
    bp_clickhouse_keeper_zone_id_to_node_id (blueprint_id, omicron_zone_id, keeper_id) {
        blueprint_id -> Uuid,
        omicron_zone_id -> Uuid,
        keeper_id -> Int8,
    }
}

table! {
    bp_clickhouse_server_zone_id_to_node_id (blueprint_id, omicron_zone_id, server_id) {
        blueprint_id -> Uuid,
        omicron_zone_id -> Uuid,
        server_id -> Int8,
    }
}

table! {
    cockroachdb_zone_id_to_node_id (omicron_zone_id, crdb_node_id) {
        omicron_zone_id -> Uuid,
        crdb_node_id -> Text,
    }
}

table! {
    bootstore_keys (key, generation) {
        key -> Text,
        generation -> Int8,
    }
}

table! {
    bootstore_config (key, generation) {
        key -> Text,
        generation -> Int8,
        data -> Jsonb,
        time_created -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
    }
}

table! {
    bfd_session (remote, switch) {
        id -> Uuid,
        local -> Nullable<Inet>,
        remote -> Inet,
        detection_threshold -> Int8,
        required_rx -> Int8,
        switch -> Text,
        mode -> crate::BfdModeEnum,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
    }
}

table! {
    probe (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        project_id -> Uuid,
        sled -> Uuid,
    }
}

table! {
    upstairs_repair_notification (repair_id, upstairs_id, session_id, region_id, notification_type) {
        time -> Timestamptz,

        repair_id -> Uuid,
        repair_type -> crate::UpstairsRepairTypeEnum,
        upstairs_id -> Uuid,
        session_id -> Uuid,

        region_id -> Uuid,
        target_ip -> Inet,
        target_port -> Int4,

        notification_type -> crate::UpstairsRepairNotificationTypeEnum,
    }
}

table! {
    upstairs_repair_progress (repair_id, time, current_item, total_items) {
        repair_id -> Uuid,
        time -> Timestamptz,
        current_item -> Int8,
        total_items -> Int8,
    }
}

table! {
    downstairs_client_stop_request_notification (time, upstairs_id, downstairs_id, reason) {
        time -> Timestamptz,
        upstairs_id -> Uuid,
        downstairs_id -> Uuid,
        reason -> crate::DownstairsClientStopRequestReasonEnum,
    }
}

table! {
    downstairs_client_stopped_notification (time, upstairs_id, downstairs_id, reason) {
        time -> Timestamptz,
        upstairs_id -> Uuid,
        downstairs_id -> Uuid,
        reason -> crate::DownstairsClientStoppedReasonEnum,
    }
}

table! {
    allow_list (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        allowed_ips -> Nullable<Array<Inet>>,
    }
}

table! {
    region_replacement (id) {
        id -> Uuid,
        request_time -> Timestamptz,
        old_region_id -> Uuid,
        volume_id -> Uuid,
        old_region_volume_id -> Nullable<Uuid>,
        new_region_id -> Nullable<Uuid>,
        replacement_state -> crate::RegionReplacementStateEnum,
        operating_saga_id -> Nullable<Uuid>,
    }
}

table! {
    volume_repair (volume_id) {
        volume_id -> Uuid,
        repair_id -> Uuid,
    }
}

table! {
    region_replacement_step (replacement_id, step_time, step_type) {
        replacement_id -> Uuid,
        step_time -> Timestamptz,
        step_type -> crate::RegionReplacementStepTypeEnum,

        step_associated_instance_id -> Nullable<Uuid>,
        step_associated_vmm_id -> Nullable<Uuid>,

        step_associated_pantry_ip -> Nullable<Inet>,
        step_associated_pantry_port -> Nullable<Int4>,
        step_associated_pantry_job_id -> Nullable<Uuid>,
    }
}

table! {
    region_snapshot_replacement (id) {
        id -> Uuid,
        request_time -> Timestamptz,
        old_dataset_id -> Uuid,
        old_region_id -> Uuid,
        old_snapshot_id -> Uuid,
        old_snapshot_volume_id -> Nullable<Uuid>,
        new_region_id -> Nullable<Uuid>,
        replacement_state -> crate::RegionSnapshotReplacementStateEnum,
        operating_saga_id -> Nullable<Uuid>,
    }
}

allow_tables_to_appear_in_same_query!(zpool, region_snapshot);

table! {
    region_snapshot_replacement_step (id) {
        id -> Uuid,
        request_id -> Uuid,
        request_time -> Timestamptz,
        volume_id -> Uuid,
        old_snapshot_volume_id -> Nullable<Uuid>,
        replacement_state -> crate::RegionSnapshotReplacementStepStateEnum,
        operating_saga_id -> Nullable<Uuid>,
    }
}

allow_tables_to_appear_in_same_query!(
    region_snapshot_replacement,
    region_snapshot_replacement_step,
    volume
);

table! {
    db_metadata (singleton) {
        singleton -> Bool,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        version -> Text,
        target_version -> Nullable<Text>,
    }
}

table! {
    migration (id) {
        id -> Uuid,
        instance_id -> Uuid,
        time_created -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        source_state -> crate::MigrationStateEnum,
        source_propolis_id -> Uuid,
        source_gen -> Int8,
        time_source_updated -> Nullable<Timestamptz>,
        target_state -> crate::MigrationStateEnum,
        target_propolis_id -> Uuid,
        target_gen -> Int8,
        time_target_updated -> Nullable<Timestamptz>,
    }
}

allow_tables_to_appear_in_same_query!(instance, migration);
allow_tables_to_appear_in_same_query!(migration, vmm);
joinable!(instance -> migration (migration_id));

allow_tables_to_appear_in_same_query!(
    ip_pool_range,
    ip_pool,
    ip_pool_resource,
    silo
);
joinable!(ip_pool_range -> ip_pool (ip_pool_id));
joinable!(ip_pool_resource -> ip_pool (ip_pool_id));

allow_tables_to_appear_in_same_query!(inv_collection, inv_collection_error);
joinable!(inv_collection_error -> inv_collection (inv_collection_id));
allow_tables_to_appear_in_same_query!(hw_baseboard_id, sw_caboose, inv_caboose);
allow_tables_to_appear_in_same_query!(
    hw_baseboard_id,
    sw_root_of_trust_page,
    inv_root_of_trust_page
);
allow_tables_to_appear_in_same_query!(hw_baseboard_id, inv_sled_agent,);

allow_tables_to_appear_in_same_query!(
    bp_omicron_zone,
    bp_target,
    dataset,
    disk,
    image,
    project_image,
    silo_image,
    instance,
    metric_producer,
    network_interface,
    instance_network_interface,
    inv_physical_disk,
    inv_nvme_disk_firmware,
    service_network_interface,
    oximeter,
    physical_disk,
    project,
    rack,
    region,
    region_snapshot,
    saga,
    saga_node_event,
    silo,
    identity_provider,
    console_session,
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
    probe,
    internet_gateway,
    internet_gateway_ip_pool,
    internet_gateway_ip_address,
);

allow_tables_to_appear_in_same_query!(dns_zone, dns_version, dns_name);

// used for query to check whether an IP pool association has any allocated IPs before deleting
allow_tables_to_appear_in_same_query!(external_ip, instance);
allow_tables_to_appear_in_same_query!(external_ip, project);
allow_tables_to_appear_in_same_query!(external_ip, ip_pool_resource);
allow_tables_to_appear_in_same_query!(external_ip, vmm);
allow_tables_to_appear_in_same_query!(external_ip, network_interface);
allow_tables_to_appear_in_same_query!(external_ip, inv_omicron_zone);
allow_tables_to_appear_in_same_query!(external_ip, inv_omicron_zone_nic);
allow_tables_to_appear_in_same_query!(inv_omicron_zone, inv_omicron_zone_nic);
allow_tables_to_appear_in_same_query!(network_interface, inv_omicron_zone);
allow_tables_to_appear_in_same_query!(network_interface, inv_omicron_zone_nic);
allow_tables_to_appear_in_same_query!(network_interface, inv_collection);
allow_tables_to_appear_in_same_query!(inv_omicron_zone, inv_collection);
allow_tables_to_appear_in_same_query!(inv_omicron_zone_nic, inv_collection);
allow_tables_to_appear_in_same_query!(external_ip, inv_collection);
allow_tables_to_appear_in_same_query!(external_ip, internet_gateway);
allow_tables_to_appear_in_same_query!(external_ip, internet_gateway_ip_pool);
allow_tables_to_appear_in_same_query!(external_ip, internet_gateway_ip_address);

allow_tables_to_appear_in_same_query!(
    switch_port,
    switch_port_settings_route_config
);

allow_tables_to_appear_in_same_query!(
    switch_port,
    switch_port_settings_bgp_peer_config,
    bgp_config
);

allow_tables_to_appear_in_same_query!(disk, virtual_provisioning_resource);

allow_tables_to_appear_in_same_query!(volume, virtual_provisioning_resource);

allow_tables_to_appear_in_same_query!(ssh_key, instance_ssh_key, instance);
joinable!(instance_ssh_key -> ssh_key (ssh_key_id));
joinable!(instance_ssh_key -> instance (instance_id));

allow_tables_to_appear_in_same_query!(sled, sled_instance);

joinable!(network_interface -> probe (parent_id));
