// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the Diesel database schema.
//!
//! NOTE: Should be kept up-to-date with dbinit.sql.

use diesel::{allow_tables_to_appear_in_same_query, joinable, table};

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
        disk_state -> Text,
        attach_instance_id -> Nullable<Uuid>,
        state_generation -> Int8,
        time_state_updated -> Timestamptz,
        slot -> Nullable<Int2>,
        size_bytes -> Int8,
        block_size -> crate::enums::BlockSizeEnum,
        disk_type -> crate::enums::DiskTypeEnum,
    }
}

table! {
    disk_type_crucible (disk_id) {
        disk_id -> Uuid,
        volume_id -> Uuid,
        origin_snapshot -> Nullable<Uuid>,
        origin_image -> Nullable<Uuid>,
        pantry_address -> Nullable<Text>,
    }
}

allow_tables_to_appear_in_same_query!(disk, disk_type_crucible);
allow_tables_to_appear_in_same_query!(volume, disk_type_crucible);
allow_tables_to_appear_in_same_query!(
    disk_type_crucible,
    virtual_provisioning_resource
);

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
        block_size -> crate::enums::BlockSizeEnum,
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
        block_size -> crate::enums::BlockSizeEnum,
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
        block_size -> crate::enums::BlockSizeEnum,
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
        geometry -> crate::enums::SwitchPortGeometryEnum,
    }
}

table! {
    switch_port_settings_link_config (port_settings_id, link_name) {
        port_settings_id -> Uuid,
        link_name -> Text,
        mtu -> Int4,
        fec -> Nullable<crate::enums::SwitchLinkFecEnum>,
        speed -> crate::enums::SwitchLinkSpeedEnum,
        autoneg -> Bool,
        lldp_link_config_id -> Nullable<Uuid>,
        tx_eq_config_id -> Nullable<Uuid>,
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
    tx_eq_config (id) {
        id -> Uuid,
        pre1 -> Nullable<Int4>,
        pre2 -> Nullable<Int4>,
        main -> Nullable<Int4>,
        post2 -> Nullable<Int4>,
        post1 -> Nullable<Int4>,
    }
}

table! {
    switch_port_settings_interface_config (id) {
        port_settings_id -> Uuid,
        id -> Uuid,
        interface_name -> Text,
        v6_enabled -> Bool,
        kind -> crate::enums::SwitchInterfaceKindEnum,
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
        rib_priority -> Nullable<Int2>,
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
        kind -> crate::enums::AddressLotKindEnum,
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
        block_size -> crate::enums::BlockSizeEnum,
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

        r#gen -> Int8,
        state -> crate::enums::SnapshotStateEnum,
        block_size -> crate::enums::BlockSizeEnum,
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
        auto_restart_policy -> Nullable<crate::enums::InstanceAutoRestartPolicyEnum>,
        auto_restart_cooldown -> Nullable<Interval>,
        boot_disk_id -> Nullable<Uuid>,
        cpu_platform -> Nullable<crate::enums::InstanceCpuPlatformEnum>,
        time_state_updated -> Timestamptz,
        state_generation -> Int8,
        active_propolis_id -> Nullable<Uuid>,
        target_propolis_id -> Nullable<Uuid>,
        migration_id -> Nullable<Uuid>,
        state -> crate::enums::InstanceStateEnum,
        time_last_auto_restarted -> Nullable<Timestamptz>,
        intended_state -> crate::enums::InstanceIntendedStateEnum,
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
        cpu_platform -> crate::enums::VmmCpuPlatformEnum,
        time_state_updated -> Timestamptz,
        state_generation -> Int8,
        state -> crate::enums::VmmStateEnum,
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
        state -> crate::enums::VmmStateEnum,
        active_sled_id -> Uuid,
        migration_id -> Nullable<Uuid>,
        ncpus -> Int8,
        memory -> Int8,
    }
}

table! {
    affinity_group (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        project_id -> Uuid,
        policy -> crate::enums::AffinityPolicyEnum,
        failure_domain -> crate::enums::FailureDomainEnum,
    }
}

table! {
    anti_affinity_group (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        project_id -> Uuid,
        policy -> crate::enums::AffinityPolicyEnum,
        failure_domain -> crate::enums::FailureDomainEnum,
    }
}

table! {
    affinity_group_instance_membership (group_id, instance_id) {
        group_id -> Uuid,
        instance_id -> Uuid,
    }
}

table! {
    anti_affinity_group_instance_membership (group_id, instance_id) {
        group_id -> Uuid,
        instance_id -> Uuid,
    }
}

table! {
    metric_producer (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        kind -> crate::enums::ProducerKindEnum,
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
    silo_auth_settings(silo_id) {
        silo_id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        device_token_max_ttl_seconds -> Nullable<Int8>,
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
        kind -> crate::enums::NetworkInterfaceKindEnum,
        parent_id -> Uuid,
        vpc_id -> Uuid,
        subnet_id -> Uuid,
        mac -> Int8,
        // NOTE: This is the IPv4 address, despite the name. We kept the
        // original name of `ip` because renaming columns is not idempotent in
        // CRDB as of today.
        ip -> Nullable<Inet>,
        ipv6 -> Nullable<Inet>,
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
        ipv4 -> Nullable<Inet>,
        ipv6 -> Nullable<Inet>,
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
        ipv4 -> Nullable<Inet>,
        ipv6 -> Nullable<Inet>,
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
        ip_version -> crate::enums::IpVersionEnum,
        rcgen -> Int8,
        reservation_type -> crate::enums::IpPoolReservationTypeEnum,
        pool_type -> crate::enums::IpPoolTypeEnum,
    }
}

table! {
    ip_pool_resource (ip_pool_id, resource_type, resource_id) {
        ip_pool_id -> Uuid,
        resource_type -> crate::enums::IpPoolResourceTypeEnum,
        resource_id -> Uuid,
        is_default -> Bool,
        pool_type -> crate::enums::IpPoolTypeEnum,
        ip_version -> crate::enums::IpVersionEnum,
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
    nat_entry (id) {
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

// View used for summarizing changes to nat_entry
table! {
    nat_changes (version) {
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
// in nat_entry.
table! {
    nat_version (last_value) {
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
        kind -> crate::enums::IpKindEnum,
        ip -> Inet,
        first_port -> Int4,
        last_port -> Int4,

        project_id -> Nullable<Uuid>,
        state -> crate::enums::IpAttachStateEnum,
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
        authentication_mode -> crate::enums::AuthenticationModeEnum,
        user_provision_type -> crate::enums::UserProvisionTypeEnum,

        mapped_fleet_roles -> Jsonb,

        rcgen -> Int8,

        admin_group_name -> Nullable<Text>,
    }
}

table! {
    silo_user (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,

        silo_id -> Uuid,
        external_id -> Nullable<Text>,
        user_provision_type -> crate::enums::UserProvisionTypeEnum,
        user_name -> Nullable<Text>,
        active -> Nullable<Bool>,
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
        external_id -> Nullable<Text>,
        user_provision_type -> crate::enums::UserProvisionTypeEnum,
        display_name -> Nullable<Text>,
        active -> Nullable<Bool>,
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
allow_tables_to_appear_in_same_query!(silo_group, silo);
allow_tables_to_appear_in_same_query!(silo_user, silo);
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
        provider_type -> crate::enums::IdentityProviderTypeEnum,
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
        saga_state -> crate::enums::SagaStateEnum,
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
    clickhouse_policy (version) {
        version -> Int8,
        clickhouse_mode -> crate::enums::ClickhouseModeEnum,
        clickhouse_cluster_target_servers -> Int2,
        clickhouse_cluster_target_keepers -> Int2,
        time_created -> Timestamptz,
    }
}

table! {
    oximeter_read_policy (version) {
        version -> Int8,
        oximeter_read_mode -> crate::enums::OximeterReadModeEnum,
        time_created -> Timestamptz,
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
    console_session (id) {
        id -> Uuid,
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
        sled_policy -> crate::enums::SledPolicyEnum,
        sled_state -> crate::enums::SledStateEnum,
        sled_agent_gen -> Int8,
        repo_depot_port -> Int4,
        cpu_family -> crate::enums::SledCpuFamilyEnum,
    }
}

table! {
    sled_resource_vmm (id) {
        id -> Uuid,
        sled_id -> Uuid,
        hardware_threads -> Int8,
        rss_ram -> Int8,
        reservoir_ram -> Int8,
        instance_id -> Nullable<Uuid>,
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

        variant -> crate::enums::PhysicalDiskKindEnum,
        disk_policy -> crate::enums::PhysicalDiskPolicyEnum,
        disk_state -> crate::enums::PhysicalDiskStateEnum,
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
        service -> crate::enums::ServiceKindEnum,
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

        control_plane_storage_buffer -> Int8,
    }
}

allow_tables_to_appear_in_same_query! {
    zpool,
    physical_disk
}

table! {
    crucible_dataset (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rcgen -> Int8,

        pool_id -> Uuid,

        ip -> Inet,
        port -> Int4,

        size_used -> Int8,

        no_provision -> Bool,
    }
}

allow_tables_to_appear_in_same_query!(zpool, crucible_dataset);

table! {
    debug_log_blueprint_planning (blueprint_id) {
        blueprint_id -> Uuid,
        debug_blob -> Jsonb,
    }
}

allow_tables_to_appear_in_same_query!(blueprint, debug_log_blueprint_planning);

table! {
    rendezvous_debug_dataset (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_tombstoned -> Nullable<Timestamptz>,
        pool_id -> Uuid,
        blueprint_id_when_created -> Uuid,
        blueprint_id_when_tombstoned -> Nullable<Uuid>,
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

        port -> Nullable<Int4>,

        read_only -> Bool,

        deleting -> Bool,

        reservation_percent -> crate::enums::RegionReservationPercentEnum,
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
        kind -> crate::enums::VpcRouterKindEnum,
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
        kind -> crate::enums::RouterRouteKindEnum,
        vpc_router_id -> Uuid,
        target -> Text,
        destination -> Text,
        vpc_subnet_id -> Nullable<Uuid>,
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
        status -> crate::enums::VpcFirewallRuleStatusEnum,
        direction -> crate::enums::VpcFirewallRuleDirectionEnum,
        targets -> Array<Text>,
        filter_hosts -> Nullable<Array<Text>>,
        filter_ports -> Nullable<Array<Text>>,
        action -> crate::enums::VpcFirewallRuleActionEnum,
        priority -> Int4,
        filter_protocols -> Nullable<Array<Text>>,
    }
}

table! {
    dns_zone (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        dns_group -> crate::enums::DnsGroupEnum,
        zone_name -> Text,
    }
}

table! {
    dns_version (dns_group, version) {
        dns_group -> crate::enums::DnsGroupEnum,
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
        token_ttl_seconds -> Nullable<Int8>,
    }
}

table! {
    device_access_token (id) {
        id -> Uuid,
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
    role_assignment (
        identity_type,
        identity_id,
        resource_type,
        resource_id,
        role_name
    ) {
        identity_type -> crate::enums::IdentityTypeEnum,
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
        time_pruned -> Nullable<Timestamptz>,
    }
}

table! {
    tuf_artifact (id) {
        id -> Uuid,
        name -> Text,
        version -> Text,
        kind -> Text,
        time_created -> Timestamptz,
        sha256 -> Text,
        artifact_size -> Int8,
        generation_added -> Int8,
        sign -> Nullable<Binary>,
        board -> Nullable<Text>,
    }
}

table! {
    tuf_repo_artifact (tuf_repo_id, tuf_artifact_id) {
        tuf_repo_id -> Uuid,
        tuf_artifact_id -> Uuid,
    }
}

allow_tables_to_appear_in_same_query!(
    tuf_repo,
    tuf_repo_artifact,
    tuf_artifact
);
joinable!(tuf_repo_artifact -> tuf_repo (tuf_repo_id));
joinable!(tuf_repo_artifact -> tuf_artifact (tuf_artifact_id));

table! {
    tuf_generation (singleton) {
        singleton -> Bool,
        generation -> Int8,
    }
}

table! {
    tuf_trust_root (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        root_role -> Jsonb,
    }
}

table! {
    target_release (generation) {
        generation -> Int8,
        time_requested -> Timestamptz,
        release_source -> crate::enums::TargetReleaseSourceEnum,
        tuf_repo_id -> Nullable<Uuid>,
    }
}

table! {
    support_bundle {
        id -> Uuid,
        time_created -> Timestamptz,
        reason_for_creation -> Text,
        reason_for_failure -> Nullable<Text>,
        state -> crate::enums::SupportBundleStateEnum,
        zpool_id -> Uuid,
        dataset_id -> Uuid,

        assigned_nexus -> Nullable<Uuid>,
        user_comment -> Nullable<Text>,
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
        sign -> Nullable<Text>,
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

        sp_type -> crate::enums::SpTypeEnum,
        sp_slot -> Int4,

        baseboard_revision -> Int8,
        hubris_archive_id -> Text,
        power_state -> crate::enums::HwPowerStateEnum,
    }
}

table! {
    inv_root_of_trust (inv_collection_id, hw_baseboard_id) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        slot_active -> crate::enums::HwRotSlotEnum,
        slot_boot_pref_transient -> Nullable<crate::enums::HwRotSlotEnum>,
        slot_boot_pref_persistent -> crate::enums::HwRotSlotEnum,
        slot_boot_pref_persistent_pending -> Nullable<crate::enums::HwRotSlotEnum>,
        slot_a_sha3_256 -> Nullable<Text>,
        slot_b_sha3_256 -> Nullable<Text>,
        stage0_fwid -> Nullable<Text>,
        stage0next_fwid -> Nullable<Text>,

        slot_a_error -> Nullable<crate::enums::RotImageErrorEnum>,
        slot_b_error -> Nullable<crate::enums::RotImageErrorEnum>,
        stage0_error -> Nullable<crate::enums::RotImageErrorEnum>,
        stage0next_error -> Nullable<crate::enums::RotImageErrorEnum>,
    }
}

table! {
    inv_host_phase_1_active_slot (inv_collection_id, hw_baseboard_id) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        slot -> crate::enums::HwM2SlotEnum,
    }
}

table! {
    inv_host_phase_1_flash_hash (inv_collection_id, hw_baseboard_id, slot) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        slot -> crate::enums::HwM2SlotEnum,
        hash -> Text,
    }
}

table! {
    inv_caboose (inv_collection_id, hw_baseboard_id, which) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        which -> crate::enums::CabooseWhichEnum,
        sw_caboose_id -> Uuid,
    }
}

table! {
    inv_root_of_trust_page (inv_collection_id, hw_baseboard_id, which) {
        inv_collection_id -> Uuid,
        hw_baseboard_id -> Uuid,
        time_collected -> Timestamptz,
        source -> Text,

        which -> crate::enums::RotPageWhichEnum,
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
        sled_role -> crate::enums::SledRoleEnum,
        usable_hardware_threads -> Int8,
        usable_physical_ram -> Int8,
        cpu_family -> crate::enums::SledCpuFamilyEnum,
        reservoir_size -> Int8,

        ledgered_sled_config -> Nullable<Uuid>,
        reconciler_status_kind -> crate::enums::InvConfigReconcilerStatusKindEnum,
        reconciler_status_sled_config -> Nullable<Uuid>,
        reconciler_status_timestamp -> Nullable<Timestamptz>,
        reconciler_status_duration_secs -> Nullable<Float8>,

        zone_manifest_boot_disk_path -> Text,
        zone_manifest_source -> Nullable<crate::enums::InvZoneManifestSourceEnum>,
        zone_manifest_mupdate_id -> Nullable<Uuid>,
        zone_manifest_boot_disk_error -> Nullable<Text>,

        mupdate_override_boot_disk_path -> Text,
        mupdate_override_id -> Nullable<Uuid>,
        mupdate_override_boot_disk_error -> Nullable<Text>,
    }
}

table! {
    inv_sled_config_reconciler (inv_collection_id, sled_id) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,

        last_reconciled_config -> Uuid,

        boot_disk_slot -> Nullable<Int2>,
        boot_disk_error -> Nullable<Text>,

        boot_partition_a_error -> Nullable<Text>,
        boot_partition_b_error -> Nullable<Text>,

        clear_mupdate_override_boot_success -> Nullable<crate::enums::RemoveMupdateOverrideBootSuccessEnum>,
        clear_mupdate_override_boot_error -> Nullable<Text>,
        clear_mupdate_override_non_boot_message -> Nullable<Text>,
    }
}

table! {
    inv_sled_boot_partition (inv_collection_id, sled_id, boot_disk_slot) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        boot_disk_slot -> Int2,

        artifact_hash -> Text,
        artifact_size -> Int8,

        header_flags -> Int8,
        header_data_size -> Int8,
        header_image_size -> Int8,
        header_target_size -> Int8,
        header_sha256 -> Text,
        header_image_name -> Text,
    }
}

table! {
    inv_last_reconciliation_disk_result (inv_collection_id, sled_id, disk_id) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        disk_id -> Uuid,

        error_message -> Nullable<Text>,
    }
}

table! {
    inv_last_reconciliation_dataset_result
        (inv_collection_id, sled_id, dataset_id)
    {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        dataset_id -> Uuid,

        error_message -> Nullable<Text>,
    }
}

table! {
    inv_last_reconciliation_orphaned_dataset
        (inv_collection_id, sled_id, pool_id, kind, zone_name)
    {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        pool_id -> Uuid,
        kind -> crate::enums::DatasetKindEnum,
        zone_name -> Text,
        reason -> Text,
        id -> Nullable<Uuid>,
        mounted -> Bool,
        available -> Int8,
        used -> Int8,
    }
}

table! {
    inv_last_reconciliation_zone_result (inv_collection_id, sled_id, zone_id) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        zone_id -> Uuid,

        error_message -> Nullable<Text>,
    }
}

table! {
    inv_zone_manifest_zone (inv_collection_id, sled_id, zone_file_name) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        zone_file_name -> Text,
        path -> Text,
        expected_size -> Int8,
        expected_sha256 -> Text,
        error -> Nullable<Text>,
    }
}

table! {
    inv_zone_manifest_non_boot (inv_collection_id, sled_id, non_boot_zpool_id) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        non_boot_zpool_id -> Uuid,
        path -> Text,
        is_valid -> Bool,
        message -> Text,
    }
}

table! {
    inv_mupdate_override_non_boot (inv_collection_id, sled_id, non_boot_zpool_id) {
        inv_collection_id -> Uuid,
        sled_id -> Uuid,
        non_boot_zpool_id -> Uuid,
        path -> Text,
        is_valid -> Bool,
        message -> Text,
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

        variant -> crate::enums::PhysicalDiskKindEnum,
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
    inv_omicron_sled_config (inv_collection_id, id) {
        inv_collection_id -> Uuid,
        id -> Uuid,

        generation -> Int8,
        remove_mupdate_override -> Nullable<Uuid>,
        host_phase_2_desired_slot_a -> Nullable<Text>,
        host_phase_2_desired_slot_b -> Nullable<Text>,
    }
}

table! {
    inv_omicron_sled_config_zone (inv_collection_id, sled_config_id, id) {
        inv_collection_id -> Uuid,
        sled_config_id -> Uuid,

        id -> Uuid,
        zone_type -> crate::enums::ZoneTypeEnum,

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

        image_source -> crate::enums::InvZoneImageSourceEnum,
        image_artifact_sha256 -> Nullable<Text>,

        nexus_lockstep_port -> Nullable<Int4>,
    }
}

table! {
    inv_omicron_sled_config_zone_nic (inv_collection_id, sled_config_id, id) {
        inv_collection_id -> Uuid,
        sled_config_id -> Uuid,
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
    inv_omicron_sled_config_dataset (inv_collection_id, sled_config_id, id) {
        inv_collection_id -> Uuid,
        sled_config_id -> Uuid,
        sled_id -> Uuid,
        id -> Uuid,

        pool_id -> Uuid,
        kind -> crate::enums::DatasetKindEnum,
        zone_name -> Nullable<Text>,

        quota -> Nullable<Int8>,
        reservation -> Nullable<Int8>,
        compression -> Text,
    }
}

table! {
    inv_omicron_sled_config_disk (inv_collection_id, sled_config_id, id) {
        inv_collection_id -> Uuid,
        sled_config_id -> Uuid,
        sled_id -> Uuid,
        id -> Uuid,

        vendor -> Text,
        serial -> Text,
        model -> Text,

        pool_id -> Uuid,
    }
}

table! {
    inv_clickhouse_keeper_membership (inv_collection_id, queried_keeper_id) {
        inv_collection_id -> Uuid,
        queried_keeper_id -> Int8,
        leader_committed_log_index -> Int8,
        raft_config -> Array<Int8>,
    }
}

table! {
    reconfigurator_config (version) {
        version -> Int8,
        planner_enabled -> Bool,
        time_modified -> Timestamptz,
        add_zones_with_mupdate_override -> Bool,
        tuf_repo_pruner_enabled -> Bool,
    }
}

table! {
    inv_cockroachdb_status (inv_collection_id, node_id) {
        inv_collection_id -> Uuid,
        node_id -> Text,
        ranges_underreplicated -> Nullable<Int8>,
        liveness_live_nodes -> Nullable<Int8>,
    }
}

table! {
    inv_ntp_timesync (inv_collection_id, zone_id) {
        inv_collection_id -> Uuid,
        zone_id -> Uuid,
        synced -> Bool,
    }
}

table! {
    inv_internal_dns (inv_collection_id, zone_id) {
        inv_collection_id -> Uuid,
        zone_id -> Uuid,
        generation -> Int8,
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

        target_release_minimum_generation -> Int8,

        nexus_generation -> Int8,

        source -> crate::enums::BpSourceEnum,
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
    bp_sled_metadata (blueprint_id, sled_id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,

        sled_state -> crate::enums::SledStateEnum,
        sled_agent_generation -> Int8,
        remove_mupdate_override -> Nullable<Uuid>,

        host_phase_2_desired_slot_a -> Nullable<Text>,
        host_phase_2_desired_slot_b -> Nullable<Text>,

        subnet -> Inet,
    }
}

allow_tables_to_appear_in_same_query!(bp_sled_metadata, tuf_artifact);

table! {
    bp_omicron_physical_disk (blueprint_id, id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,

        vendor -> Text,
        serial -> Text,
        model -> Text,

        id -> Uuid,
        pool_id -> Uuid,

        disposition -> crate::enums::BpPhysicalDiskDispositionEnum,
        disposition_expunged_as_of_generation -> Nullable<Int8>,
        disposition_expunged_ready_for_cleanup -> Bool,
    }
}

table! {
    bp_omicron_dataset (blueprint_id, id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,
        id -> Uuid,

        disposition -> crate::enums::BpDatasetDispositionEnum,

        pool_id -> Uuid,
        kind -> crate::enums::DatasetKindEnum,
        zone_name -> Nullable<Text>,
        ip -> Nullable<Inet>,
        port -> Nullable<Int4>,

        quota -> Nullable<Int8>,
        reservation -> Nullable<Int8>,
        compression -> Text,
    }
}

table! {
    bp_omicron_zone (blueprint_id, id) {
        blueprint_id -> Uuid,
        sled_id -> Uuid,

        id -> Uuid,
        underlay_address -> Inet,
        zone_type -> crate::enums::ZoneTypeEnum,

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
        disposition -> crate::enums::BpZoneDispositionEnum,
        disposition_expunged_as_of_generation -> Nullable<Int8>,
        disposition_expunged_ready_for_cleanup -> Bool,
        external_ip_id -> Nullable<Uuid>,
        filesystem_pool -> Uuid,
        image_source -> crate::enums::BpZoneImageSourceEnum,
        image_artifact_sha256 -> Nullable<Text>,
        nexus_generation -> Nullable<Int8>,
        nexus_lockstep_port -> Nullable<Int4>,
    }
}

allow_tables_to_appear_in_same_query!(bp_omicron_zone, tuf_artifact);

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
    bp_oximeter_read_policy (blueprint_id) {
        blueprint_id -> Uuid,
        version -> Int8,
        oximeter_read_mode -> crate::enums::OximeterReadModeEnum,
    }
}

table! {
    bp_pending_mgs_update_rot_bootloader (blueprint_id, hw_baseboard_id) {
        blueprint_id -> Uuid,
        hw_baseboard_id -> Uuid,
        sp_type -> crate::enums::SpTypeEnum,
        sp_slot -> Int4,
        artifact_sha256 -> Text,
        artifact_version -> Text,
        expected_stage0_version -> Text,
        expected_stage0_next_version -> Nullable<Text>,
    }
}

table! {
    bp_pending_mgs_update_sp (blueprint_id, hw_baseboard_id) {
        blueprint_id -> Uuid,
        hw_baseboard_id -> Uuid,
        sp_type -> crate::enums::SpTypeEnum,
        sp_slot -> Int4,
        artifact_sha256 -> Text,
        artifact_version -> Text,
        expected_active_version -> Text,
        expected_inactive_version -> Nullable<Text>,
    }
}

table! {
    bp_pending_mgs_update_rot (blueprint_id, hw_baseboard_id) {
        blueprint_id -> Uuid,
        hw_baseboard_id -> Uuid,
        sp_type -> crate::enums::SpTypeEnum,
        sp_slot -> Int4,
        artifact_sha256 -> Text,
        artifact_version -> Text,
        expected_active_slot -> crate::enums::HwRotSlotEnum,
        expected_active_version -> Text,
        expected_inactive_version -> Nullable<Text>,
        expected_persistent_boot_preference -> crate::enums::HwRotSlotEnum,
        expected_pending_persistent_boot_preference -> Nullable<crate::enums::HwRotSlotEnum>,
        expected_transient_boot_preference -> Nullable<crate::enums::HwRotSlotEnum>,
    }
}

table! {
    bp_pending_mgs_update_host_phase_1 (blueprint_id, hw_baseboard_id) {
        blueprint_id -> Uuid,
        hw_baseboard_id -> Uuid,
        sp_type -> crate::enums::SpTypeEnum,
        sp_slot -> Int4,
        artifact_sha256 -> Text,
        artifact_version -> Text,
        expected_active_phase_1_slot -> crate::enums::HwM2SlotEnum,
        expected_boot_disk -> crate::enums::HwM2SlotEnum,
        expected_active_phase_1_hash -> Text,
        expected_active_phase_2_hash -> Text,
        expected_inactive_phase_1_hash -> Text,
        expected_inactive_phase_2_hash -> Text,
        sled_agent_ip -> Inet,
        sled_agent_port -> Int4,
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
        mode -> crate::enums::BfdModeEnum,
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
        repair_type -> crate::enums::UpstairsRepairTypeEnum,
        upstairs_id -> Uuid,
        session_id -> Uuid,

        region_id -> Uuid,
        target_ip -> Inet,
        target_port -> Int4,

        notification_type -> crate::enums::UpstairsRepairNotificationTypeEnum,
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
        reason -> crate::enums::DownstairsClientStopRequestReasonEnum,
    }
}

table! {
    downstairs_client_stopped_notification (time, upstairs_id, downstairs_id, reason) {
        time -> Timestamptz,
        upstairs_id -> Uuid,
        downstairs_id -> Uuid,
        reason -> crate::enums::DownstairsClientStoppedReasonEnum,
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
        replacement_state -> crate::enums::RegionReplacementStateEnum,
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
        step_type -> crate::enums::RegionReplacementStepTypeEnum,

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
        old_dataset_id -> Nullable<Uuid>,
        old_region_id -> Uuid,
        old_snapshot_id -> Nullable<Uuid>,
        old_snapshot_volume_id -> Nullable<Uuid>,
        new_region_id -> Nullable<Uuid>,
        replacement_state -> crate::enums::RegionSnapshotReplacementStateEnum,
        operating_saga_id -> Nullable<Uuid>,
        new_region_volume_id -> Nullable<Uuid>,
        replacement_type -> crate::enums::ReadOnlyTargetReplacementTypeEnum,
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
        replacement_state -> crate::enums::RegionSnapshotReplacementStepStateEnum,
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
    db_metadata_nexus (nexus_id) {
        nexus_id -> Uuid,
        last_drained_blueprint_id -> Nullable<Uuid>,
        state -> crate::enums::DbMetadataNexusStateEnum,
    }
}

table! {
    migration (id) {
        id -> Uuid,
        instance_id -> Uuid,
        time_created -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        source_state -> crate::enums::MigrationStateEnum,
        source_propolis_id -> Uuid,
        source_gen -> Int8,
        time_source_updated -> Nullable<Timestamptz>,
        target_state -> crate::enums::MigrationStateEnum,
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
    anti_affinity_group,
    anti_affinity_group_instance_membership,
    affinity_group,
    affinity_group_instance_membership,
    bp_omicron_zone,
    bp_target,
    rendezvous_debug_dataset,
    crucible_dataset,
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
    sled_resource_vmm,
    support_bundle,
    router_route,
    vmm,
    volume,
    vpc,
    vpc_subnet,
    vpc_router,
    vpc_firewall_rule,
    user_builtin,
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
allow_tables_to_appear_in_same_query!(
    external_ip,
    inv_omicron_sled_config_zone
);
allow_tables_to_appear_in_same_query!(
    external_ip,
    inv_omicron_sled_config_zone_nic
);
allow_tables_to_appear_in_same_query!(
    inv_omicron_sled_config_zone,
    inv_omicron_sled_config_zone_nic
);
allow_tables_to_appear_in_same_query!(
    network_interface,
    inv_omicron_sled_config_zone
);
allow_tables_to_appear_in_same_query!(
    network_interface,
    inv_omicron_sled_config_zone_nic
);
allow_tables_to_appear_in_same_query!(network_interface, inv_collection);
allow_tables_to_appear_in_same_query!(
    inv_omicron_sled_config_zone,
    inv_collection
);
allow_tables_to_appear_in_same_query!(
    inv_omicron_sled_config_zone_nic,
    inv_collection
);
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

allow_tables_to_appear_in_same_query!(
    address_lot,
    address_lot_block,
    switch_port_settings,
);

allow_tables_to_appear_in_same_query!(disk, virtual_provisioning_resource);

allow_tables_to_appear_in_same_query!(volume, virtual_provisioning_resource);

allow_tables_to_appear_in_same_query!(ssh_key, instance_ssh_key, instance);
joinable!(instance_ssh_key -> ssh_key (ssh_key_id));
joinable!(instance_ssh_key -> instance (instance_id));

allow_tables_to_appear_in_same_query!(sled, sled_instance);

joinable!(network_interface -> probe (parent_id));
allow_tables_to_appear_in_same_query!(probe, external_ip);
allow_tables_to_appear_in_same_query!(external_ip, vpc_subnet);
allow_tables_to_appear_in_same_query!(external_ip, vpc);

table! {
    volume_resource_usage (usage_id) {
        usage_id -> Uuid,

        volume_id -> Uuid,

        usage_type -> crate::enums::VolumeResourceUsageTypeEnum,

        region_id -> Nullable<Uuid>,

        region_snapshot_dataset_id -> Nullable<Uuid>,
        region_snapshot_region_id -> Nullable<Uuid>,
        region_snapshot_snapshot_id -> Nullable<Uuid>,
    }
}

table! {
    alert_receiver (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        secret_gen -> Int8,
        subscription_gen -> Int8,
        endpoint -> Text,
    }
}

table! {
    webhook_secret (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        rx_id -> Uuid,
        secret -> Text,
    }
}

table! {
    alert_subscription (rx_id, alert_class) {
        rx_id -> Uuid,
        alert_class -> crate::enums::AlertClassEnum,
        glob -> Nullable<Text>,
        time_created -> Timestamptz,
    }
}

table! {
    alert_glob (rx_id, glob) {
        rx_id -> Uuid,
        glob -> Text,
        regex -> Text,
        time_created -> Timestamptz,
        schema_version -> Nullable<Text>,
    }
}

allow_tables_to_appear_in_same_query!(
    alert_receiver,
    webhook_secret,
    alert_subscription,
    alert_glob,
    alert,
);
joinable!(alert_subscription -> alert_receiver (rx_id));
joinable!(webhook_secret -> alert_receiver (rx_id));
joinable!(alert_glob -> alert_receiver (rx_id));

table! {
    alert (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        alert_class -> crate::enums::AlertClassEnum,
        payload -> Jsonb,
        time_dispatched -> Nullable<Timestamptz>,
        num_dispatched -> Int8,
    }
}

table! {
    webhook_delivery (id) {
        id -> Uuid,
        alert_id -> Uuid,
        rx_id -> Uuid,
        triggered_by -> crate::enums::AlertDeliveryTriggerEnum,
        attempts -> Int2,
        time_created -> Timestamptz,
        time_completed -> Nullable<Timestamptz>,
        state -> crate::enums::AlertDeliveryStateEnum,
        deliverator_id -> Nullable<Uuid>,
        time_leased -> Nullable<Timestamptz>,
    }
}

allow_tables_to_appear_in_same_query!(alert_receiver, webhook_delivery);
joinable!(webhook_delivery -> alert_receiver (rx_id));
allow_tables_to_appear_in_same_query!(webhook_delivery, alert);
allow_tables_to_appear_in_same_query!(webhook_delivery_attempt, alert);
joinable!(webhook_delivery -> alert (alert_id));

table! {
    webhook_delivery_attempt (id) {
        id -> Uuid,
        delivery_id -> Uuid,
        attempt -> Int2,
        rx_id -> Uuid,
        result -> crate::enums::WebhookDeliveryAttemptResultEnum,
        response_status -> Nullable<Int4>,
        response_duration -> Nullable<Interval>,
        time_created -> Timestamptz,
        deliverator_id -> Uuid,
    }
}

allow_tables_to_appear_in_same_query!(
    webhook_delivery,
    webhook_delivery_attempt
);
joinable!(webhook_delivery_attempt -> webhook_delivery (delivery_id));

table! {
    ereport (restart_id, ena) {
        restart_id -> Uuid,
        ena -> Int8,
        time_deleted -> Nullable<Timestamptz>,
        time_collected -> Timestamptz,
        collector_id -> Uuid,

        part_number -> Nullable<Text>,
        serial_number -> Nullable<Text>,

        class -> Nullable<Text>,
        report -> Jsonb,

        reporter -> crate::enums::EreporterTypeEnum,
        sp_type -> Nullable<crate::enums::SpTypeEnum>,
        sp_slot -> Nullable<Int4>,
        sled_id -> Nullable<Uuid>,
    }
}

table! {
    user_data_export (id) {
        id -> Uuid,

        state -> crate::enums::UserDataExportStateEnum,
        operating_saga_id -> Nullable<Uuid>,
        generation -> Int8,

        resource_id -> Uuid,
        resource_type -> crate::enums::UserDataExportResourceTypeEnum,
        resource_deleted -> Bool,

        pantry_ip -> Nullable<Inet>,
        pantry_port -> Nullable<Int4>,
        volume_id -> Nullable<Uuid>,
    }
}

table! {
    multicast_group (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        ip_pool_id -> Uuid,
        ip_pool_range_id -> Uuid,
        vni -> Int4,
        multicast_ip -> Inet,
        source_ips -> Array<Inet>,
        underlay_group_id -> Nullable<Uuid>,
        tag -> Nullable<Text>,
        state -> crate::enums::MulticastGroupStateEnum,
        version_added -> Int8,
        version_removed -> Nullable<Int8>,
    }
}

table! {
    multicast_group_member (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        external_group_id -> Uuid,
        multicast_ip -> Inet,
        parent_id -> Uuid,
        sled_id -> Nullable<Uuid>,
        state -> crate::enums::MulticastGroupMemberStateEnum,
        version_added -> Int8,
        version_removed -> Nullable<Int8>,
    }
}

table! {
    underlay_multicast_group (id) {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        multicast_ip -> Inet,
        tag -> Nullable<Text>,
        version_added -> Int8,
        version_removed -> Nullable<Int8>,
    }
}

// Allow multicast tables to appear together for NOT EXISTS subqueries
allow_tables_to_appear_in_same_query!(multicast_group, multicast_group_member);

allow_tables_to_appear_in_same_query!(user_data_export, snapshot, image);

table! {
    audit_log (id) {
        id -> Uuid,
        time_started -> Timestamptz,
        request_id -> Text,
        request_uri -> Text,
        operation_id -> Text,
        source_ip -> Inet,
        user_agent -> Nullable<Text>,
        actor_id -> Nullable<Uuid>,
        actor_silo_id -> Nullable<Uuid>,
        actor_kind -> crate::enums::AuditLogActorKindEnum,
        auth_method -> Nullable<Text>,
        time_completed -> Nullable<Timestamptz>,
        http_status_code -> Nullable<Int4>, // SqlU16
        error_code -> Nullable<Text>,
        error_message -> Nullable<Text>,
        result_kind -> Nullable<crate::enums::AuditLogResultKindEnum>,
    }
}

table! {
    audit_log_complete (id) {
        id -> Uuid,
        time_started -> Timestamptz,
        request_id -> Text,
        request_uri -> Text,
        operation_id -> Text,
        source_ip -> Inet,
        user_agent -> Nullable<Text>,
        actor_id -> Nullable<Uuid>,
        actor_silo_id -> Nullable<Uuid>,
        actor_kind -> crate::enums::AuditLogActorKindEnum,
        auth_method -> Nullable<Text>,
        time_completed -> Timestamptz,
        http_status_code -> Nullable<Int4>, // SqlU16
        error_code -> Nullable<Text>,
        error_message -> Nullable<Text>,
        result_kind -> crate::enums::AuditLogResultKindEnum,
    }
}

table! {
    scim_client_bearer_token (id) {
        id -> Uuid,

        time_created -> Timestamptz,
        time_deleted -> Nullable<Timestamptz>,
        time_expires -> Nullable<Timestamptz>,

        silo_id -> Uuid,

        bearer_token -> Text,
    }
}

table! {
    rendezvous_local_storage_dataset (id) {
        id -> Uuid,

        time_created -> Timestamptz,
        time_tombstoned -> Nullable<Timestamptz>,

        blueprint_id_when_created -> Uuid,
        blueprint_id_when_tombstoned -> Nullable<Uuid>,

        pool_id -> Uuid,

        size_used -> Int8,

        no_provision -> Bool,
    }
}

allow_tables_to_appear_in_same_query!(zpool, rendezvous_local_storage_dataset);
allow_tables_to_appear_in_same_query!(
    physical_disk,
    rendezvous_local_storage_dataset
);

table! {
    fm_sitrep (id) {
        id -> Uuid,
        parent_sitrep_id -> Nullable<Uuid>,
        inv_collection_id -> Uuid,
        time_created -> Timestamptz,
        creator_id -> Uuid,
        comment -> Text,
    }
}

allow_tables_to_appear_in_same_query!(fm_sitrep, inv_collection);

table! {
    fm_sitrep_history (version) {
        version -> Int8,
        sitrep_id -> Uuid,
        time_made_current -> Timestamptz,
    }
}

allow_tables_to_appear_in_same_query!(fm_sitrep, fm_sitrep_history);
