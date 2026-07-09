// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Full-row models: structs that map an entire table row, with fields in the
//! same order as the table's columns.
//!
//! `Queryable` decodes rows positionally (see "Positional mapping" in the
//! crate docs), so any query that selects a table's default column list — a
//! `SELECT` without `.select(Model::as_select())`, an insert/update
//! `RETURNING` without `.returning(Model::as_returning())`, or the custom SQL
//! built by `check_if_exists` in nexus-db-queries — silently depends on the
//! struct's field order matching the table's column order. A mismatch between
//! columns of different types fails loudly at runtime as a deserialization
//! error; a mismatch between same-typed columns swaps values silently.
//!
//! `check_if_exists` requires its model to implement [`FullRowModel`], and
//! the only way to implement it is the [`full_row_models!`] list below, which
//! also generates a test verifying the field order for each entry. So a model
//! can't be used with `check_if_exists` without being checked here.
//!
//! Column order in `schema.rs` is itself checked against the real
//! dbinit-produced schema by `crdb_alignment_test` in nexus-db-schema, so
//! together these tests tie model structs all the way to the database.
//!
//! These assertions only make sense for models that map an entire table row.
//! Projection models that select a subset of columns are always used with an
//! explicit `as_select()`, where order doesn't matter, and would spuriously
//! fail the comparison.

use diesel::pg::Pg;
use diesel::prelude::*;

/// Marker trait for models that map an entire row of `Self::Table` with
/// fields in the table's column order.
///
/// Required by `check_if_exists` in nexus-db-queries, which decodes the found
/// row positionally from the table's `AllColumns` order. Implement it by
/// adding the model to the [`full_row_models!`] list, which also generates
/// the test enforcing the field-order claim; don't write impls by hand.
pub trait FullRowModel: Selectable<Pg> {
    type Table: Table;
}

/// Assert that `Q`'s `Selectable` implementation selects exactly the columns
/// of its table, in the table's column order.
///
/// Works by rendering both `table.select(Q::as_select())` (columns in struct
/// field order) and `table.select(all_columns())` (columns in table order) to
/// SQL text and comparing. Embedded `#[diesel(embed)]` fields render flat, so
/// nesting doesn't affect the comparison.
#[cfg(test)]
fn assert_model_matches_table<Q>()
where
    Q: FullRowModel,
    Q::SelectExpression: diesel::query_builder::QueryId,
    Q::Table: Default,
    Q::Table:
        diesel::query_dsl::methods::SelectDsl<diesel::dsl::AsSelect<Q, Pg>>,
    Q::Table:
        diesel::query_dsl::methods::SelectDsl<<Q::Table as Table>::AllColumns>,
    diesel::dsl::Select<Q::Table, diesel::dsl::AsSelect<Q, Pg>>:
        diesel::query_builder::QueryFragment<Pg>,
    diesel::dsl::Select<Q::Table, <Q::Table as Table>::AllColumns>:
        diesel::query_builder::QueryFragment<Pg>,
{
    let by_field_order = diesel::debug_query::<Pg, _>(
        &Q::Table::default().select(Q::as_select()),
    )
    .to_string();
    let by_table_order = diesel::debug_query::<Pg, _>(
        &Q::Table::default().select(<Q::Table as Table>::all_columns()),
    )
    .to_string();
    if by_field_order == by_table_order {
        return;
    }

    // Classify the mismatch to make failures easier to triage.
    fn column_list(sql: &str) -> Vec<&str> {
        let start = sql.find("SELECT ").expect("query has SELECT") + 7;
        let end = sql.find(" FROM ").expect("query has FROM");
        sql[start..end].split(", ").collect()
    }
    let model_cols = column_list(&by_field_order);
    let table_cols = column_list(&by_table_order);
    let name = std::any::type_name::<Q>();
    let mut sorted_model = model_cols.clone();
    let mut sorted_table = table_cols.clone();
    sorted_model.sort_unstable();
    sorted_table.sort_unstable();
    if sorted_model == sorted_table {
        panic!(
            "ORDER MISMATCH: {name} has all of its table's columns, \
             but in a different order\n  model: {model_cols:?}\n  \
             table: {table_cols:?}"
        );
    } else if model_cols.iter().all(|c| table_cols.contains(c)) {
        let missing: Vec<_> =
            table_cols.iter().filter(|c| !model_cols.contains(c)).collect();
        panic!(
            "SUBSET: {name} selects only some of its table's columns \
             (a projection, not a full-row model); missing: {missing:?}"
        );
    } else {
        panic!(
            "MISMATCH: {name} does not select its table's columns\n  \
             model: {model_cols:?}\n  table: {table_cols:?}"
        );
    }
}

/// Implements [`FullRowModel`] for each model and generates one test per
/// model so a mismatch in one doesn't hide others.
macro_rules! full_row_models {
    ($($name:ident: $model:ty => $table:ident;)+) => {
        $(
            impl FullRowModel for $model {
                type Table = nexus_db_schema::schema::$table::table;
            }
        )+

        #[cfg(test)]
        mod tests {
            use super::*;
            $(
                #[test]
                fn $name() {
                    assert_model_matches_table::<$model>();
                }
            )+
        }
    };
}

full_row_models! {
    address_lot_block: crate::address_lot::AddressLotBlock => address_lot_block;
    address_lot_reserved_block: crate::address_lot::AddressLotReservedBlock => address_lot_rsvd_block;
    address_lot: crate::address_lot::AddressLot => address_lot;
    affinity_group_instance_membership: crate::affinity::AffinityGroupInstanceMembership => affinity_group_instance_membership;
    affinity_group: crate::affinity::AffinityGroup => affinity_group;
    alert_glob: crate::alert_subscription::AlertGlob => alert_glob;
    alert_receiver: crate::webhook_rx::AlertReceiver => alert_receiver;
    alert_request: crate::fm::AlertRequest => fm_alert_request;
    alert_rx_glob: crate::alert_subscription::AlertRxGlob => alert_glob;
    alert_rx_subscription: crate::alert_subscription::AlertRxSubscription => alert_subscription;
    alert: crate::alert::Alert => alert;
    allow_list: crate::allow_list::AllowList => allow_list;
    anti_affinity_group_instance_membership: crate::affinity::AntiAffinityGroupInstanceMembership => anti_affinity_group_instance_membership;
    anti_affinity_group: crate::affinity::AntiAffinityGroup => anti_affinity_group;
    audit_log_entry_init: crate::audit_log::AuditLogEntryInit => audit_log;
    audit_log_entry: crate::audit_log::AuditLogEntry => audit_log_complete;
    bfd_session: crate::bfd::BfdSession => bfd_session;
    bgp_announce_set: crate::bgp::BgpAnnounceSet => bgp_announce_set;
    bgp_announcement: crate::bgp::BgpAnnouncement => bgp_announcement;
    bgp_config: crate::bgp::BgpConfig => bgp_config;
    bgp_peer_view: crate::bgp::BgpPeerView => bgp_peer_view;
    blueprint: crate::deployment::Blueprint => blueprint;
    bootstore_config: crate::bootstore::BootstoreConfig => bootstore_config;
    bootstore_keys: crate::bootstore::BootstoreKeys => bootstore_keys;
    bp_clickhouse_cluster_config: crate::deployment::BpClickhouseClusterConfig => bp_clickhouse_cluster_config;
    bp_clickhouse_keeper_zone_id_to_node_id: crate::deployment::BpClickhouseKeeperZoneIdToNodeId => bp_clickhouse_keeper_zone_id_to_node_id;
    bp_clickhouse_server_zone_id_to_node_id: crate::deployment::BpClickhouseServerZoneIdToNodeId => bp_clickhouse_server_zone_id_to_node_id;
    bp_omicron_dataset: crate::deployment::BpOmicronDataset => bp_omicron_dataset;
    bp_omicron_physical_disk: crate::deployment::BpOmicronPhysicalDisk => bp_omicron_physical_disk;
    bp_omicron_zone_nic: crate::deployment::BpOmicronZoneNic => bp_omicron_zone_nic;
    bp_omicron_zone: crate::deployment::BpOmicronZone => bp_omicron_zone;
    bp_oximeter_read_policy: crate::deployment::BpOximeterReadPolicy => bp_oximeter_read_policy;
    bp_pending_mgs_update_host_phase1: crate::deployment::BpPendingMgsUpdateHostPhase1 => bp_pending_mgs_update_host_phase_1;
    bp_pending_mgs_update_rot_bootloader: crate::deployment::BpPendingMgsUpdateRotBootloader => bp_pending_mgs_update_rot_bootloader;
    bp_pending_mgs_update_rot: crate::deployment::BpPendingMgsUpdateRot => bp_pending_mgs_update_rot;
    bp_pending_mgs_update_sp: crate::deployment::BpPendingMgsUpdateSp => bp_pending_mgs_update_sp;
    bp_single_measurement: crate::deployment::BpSingleMeasurement => bp_single_measurements;
    bp_sled_metadata: crate::deployment::BpSledMetadata => bp_sled_metadata;
    bp_target: crate::deployment::BpTarget => bp_target;
    case_ereport: crate::fm::CaseEreport => fm_ereport_in_case;
    case_metadata: crate::fm::CaseMetadata => fm_case;
    certificate: crate::certificate::Certificate => certificate;
    clickhouse_policy: crate::clickhouse_policy::ClickhousePolicy => clickhouse_policy;
    cockroach_zone_id_to_node_id: crate::cockroachdb_node_id::CockroachZoneIdToNodeId => cockroachdb_zone_id_to_node_id;
    console_session: crate::console_session::ConsoleSession => console_session;
    crucible_dataset: crate::crucible_dataset::CrucibleDataset => crucible_dataset;
    db_host_phase2_desired_slots: crate::inventory::DbHostPhase2DesiredSlots => inv_omicron_sled_config;
    db_metadata_nexus: crate::db_metadata::DbMetadataNexus => db_metadata_nexus;
    db_metadata: crate::db_metadata::DbMetadata => db_metadata;
    db_omicron_measurements: crate::inventory::DbOmicronMeasurements => inv_omicron_sled_config;
    debug_log_blueprint_planning: crate::deployment::DebugLogBlueprintPlanning => debug_log_blueprint_planning;
    device_access_token: crate::device_auth::DeviceAccessToken => device_access_token;
    device_auth_request: crate::device_auth::DeviceAuthRequest => device_auth_request;
    disk_runtime_state: crate::disk::DiskRuntimeState => disk;
    disk_type_crucible: crate::disk_type_crucible::DiskTypeCrucible => disk_type_crucible;
    disk_type_local_storage: crate::disk_type_local_storage::DiskTypeLocalStorage => disk_type_local_storage;
    disk: crate::disk::Disk => disk;
    dns_name: crate::dns::DnsName => dns_name;
    dns_version: crate::dns::DnsVersion => dns_version;
    dns_zone: crate::dns::DnsZone => dns_zone;
    downstairs_client_stop_request_notification: crate::downstairs::DownstairsClientStopRequestNotification => downstairs_client_stop_request_notification;
    downstairs_client_stopped_notification: crate::downstairs::DownstairsClientStoppedNotification => downstairs_client_stopped_notification;
    ereport: crate::ereport::Ereport => ereport;
    ereporter_restart: crate::ereporter_restart::EreporterRestart => ereporter_restart;
    external_ip: crate::external_ip::ExternalIp => external_ip;
    external_multicast_group: crate::multicast_group::ExternalMulticastGroup => multicast_group;
    external_subnet: crate::external_subnet::ExternalSubnet => external_subnet;
    floating_ip: crate::external_ip::FloatingIp => floating_ip;
    fm_fact_physical_disk: crate::fm::FmFactPhysicalDisk => fm_fact_physical_disk;
    fm_support_bundle_request_data_selection_ereports: crate::fm::Ereports => fm_support_bundle_request_data_selection_ereports;
    fm_support_bundle_request_data_selection_flags: crate::fm::DataSelectionFlags => fm_support_bundle_request_data_selection_flags;
    fm_support_bundle_request_data_selection_host_info: crate::fm::HostInfo => fm_support_bundle_request_data_selection_host_info;
    hw_baseboard_id: crate::inventory::HwBaseboardId => hw_baseboard_id;
    identity_provider: crate::identity_provider::IdentityProvider => identity_provider;
    image: crate::image::Image => image;
    instance_auto_restart: crate::instance::InstanceAutoRestart => instance;
    instance_network_interface: crate::network_interface::InstanceNetworkInterface => instance_network_interface;
    instance_runtime_state: crate::instance::InstanceRuntimeState => instance;
    instance_ssh_key: crate::ssh_key::InstanceSshKey => instance_ssh_key;
    instance: crate::instance::Instance => instance;
    internet_gateway_ip_address: crate::internet_gateway::InternetGatewayIpAddress => internet_gateway_ip_address;
    internet_gateway_ip_pool: crate::internet_gateway::InternetGatewayIpPool => internet_gateway_ip_pool;
    internet_gateway: crate::internet_gateway::InternetGateway => internet_gateway;
    inv_caboose: crate::inventory::InvCaboose => inv_caboose;
    inv_clickhouse_keeper_membership: crate::inventory::InvClickhouseKeeperMembership => inv_clickhouse_keeper_membership;
    inv_cockroach_status: crate::inventory::InvCockroachStatus => inv_cockroachdb_status;
    inv_collection_error: crate::inventory::InvCollectionError => inv_collection_error;
    inv_collection: crate::inventory::InvCollection => inv_collection;
    inv_config_reconciler_status: crate::inventory::InvConfigReconcilerStatus => inv_sled_agent;
    inv_dataset: crate::inventory::InvDataset => inv_dataset;
    inv_fmd_host_case: crate::inventory::InvFmdHostCase => inv_fmd_host_case;
    inv_fmd_resource: crate::inventory::InvFmdResource => inv_fmd_resource;
    inv_fmd_status: crate::inventory::InvFmdStatus => inv_fmd_status;
    inv_host_phase1_active_slot: crate::inventory::InvHostPhase1ActiveSlot => inv_host_phase_1_active_slot;
    inv_host_phase1_flash_hash: crate::inventory::InvHostPhase1FlashHash => inv_host_phase_1_flash_hash;
    inv_internal_dns: crate::inventory::InvInternalDns => inv_internal_dns;
    inv_last_reconciliation_dataset_result: crate::inventory::InvLastReconciliationDatasetResult => inv_last_reconciliation_dataset_result;
    inv_last_reconciliation_disk_result: crate::inventory::InvLastReconciliationDiskResult => inv_last_reconciliation_disk_result;
    inv_last_reconciliation_orphaned_dataset: crate::inventory::InvLastReconciliationOrphanedDataset => inv_last_reconciliation_orphaned_dataset;
    inv_last_reconciliation_zone_result: crate::inventory::InvLastReconciliationZoneResult => inv_last_reconciliation_zone_result;
    inv_measurement_manifest_non_boot: crate::inventory::InvMeasurementManifestNonBoot => inv_measurement_manifest_non_boot;
    inv_mupdate_override_non_boot: crate::inventory::InvMupdateOverrideNonBoot => inv_mupdate_override_non_boot;
    inv_ntp_timesync: crate::inventory::InvNtpTimesync => inv_ntp_timesync;
    inv_nvme_disk_firmware: crate::inventory::InvNvmeDiskFirmware => inv_nvme_disk_firmware;
    inv_omicron_file_source_resolver: crate::inventory::InvOmicronFileSourceResolver => inv_sled_agent;
    inv_omicron_sled_config_dataset: crate::inventory::InvOmicronSledConfigDataset => inv_omicron_sled_config_dataset;
    inv_omicron_sled_config_disk: crate::inventory::InvOmicronSledConfigDisk => inv_omicron_sled_config_disk;
    inv_omicron_sled_config_zone_nic: crate::inventory::InvOmicronSledConfigZoneNic => inv_omicron_sled_config_zone_nic;
    inv_omicron_sled_config_zone: crate::inventory::InvOmicronSledConfigZone => inv_omicron_sled_config_zone;
    inv_omicron_sled_config: crate::inventory::InvOmicronSledConfig => inv_omicron_sled_config;
    inv_physical_disk: crate::inventory::InvPhysicalDisk => inv_physical_disk;
    inv_remove_mupdate_override: crate::inventory::InvRemoveMupdateOverride => inv_sled_config_reconciler;
    inv_root_of_trust: crate::inventory::InvRootOfTrust => inv_root_of_trust;
    inv_rot_page: crate::inventory::InvRotPage => inv_root_of_trust_page;
    inv_service_processor: crate::inventory::InvServiceProcessor => inv_service_processor;
    inv_single_measurements: crate::inventory::InvSingleMeasurements => inv_single_measurements;
    inv_sled_agent: crate::inventory::InvSledAgent => inv_sled_agent;
    inv_sled_boot_partition: crate::inventory::InvSledBootPartition => inv_sled_boot_partition;
    inv_sled_config_reconciler: crate::inventory::InvSledConfigReconciler => inv_sled_config_reconciler;
    inv_svc_enabled_not_online_parse_error: crate::inventory::InvSvcEnabledNotOnlineParseError => inv_svc_enabled_not_online_parse_error;
    inv_svc_enabled_not_online_service: crate::inventory::InvSvcEnabledNotOnlineService => inv_svc_enabled_not_online_service;
    inv_svc_enabled_not_online: crate::inventory::InvSvcEnabledNotOnline => inv_svc_enabled_not_online;
    inv_zone_manifest_measurement: crate::inventory::InvZoneManifestMeasurement => inv_zone_manifest_measurement;
    inv_zone_manifest_non_boot: crate::inventory::InvZoneManifestNonBoot => inv_zone_manifest_non_boot;
    inv_zone_manifest_zone: crate::inventory::InvZoneManifestZone => inv_zone_manifest_zone;
    inv_zpool: crate::inventory::InvZpool => inv_zpool;
    ip_pool_range: crate::ip_pool::IpPoolRange => ip_pool_range;
    ip_pool_resource: crate::ip_pool::IpPoolResource => ip_pool_resource;
    ip_pool: crate::ip_pool::IpPool => ip_pool;
    lldp_link_config: crate::switch_port::LldpLinkConfig => lldp_link_config;
    local_storage_dataset_allocation: crate::local_storage_dataset_allocation::LocalStorageDatasetAllocation => local_storage_dataset_allocation;
    local_storage_unencrypted_dataset_allocation: crate::local_storage_dataset_allocation::LocalStorageUnencryptedDatasetAllocation => local_storage_unencrypted_dataset_allocation;
    loopback_address: crate::switch_interface::LoopbackAddress => loopback_address;
    migration: crate::migration::Migration => migration;
    multicast_group_member: crate::multicast_group::MulticastGroupMember => multicast_group_member;
    nat_change: crate::nat_entry::NatChange => nat_changes;
    nat_entry: crate::nat_entry::NatEntry => nat_entry;
    network_interface: crate::network_interface::NetworkInterface => network_interface;
    oximeter_info: crate::oximeter_info::OximeterInfo => oximeter;
    oximeter_read_policy: crate::oximeter_read_policy::OximeterReadPolicy => oximeter_read_policy;
    physical_disk_adoption_request: crate::physical_disk::PhysicalDiskAdoptionRequest => physical_disk_adoption_request;
    physical_disk: crate::physical_disk::PhysicalDisk => physical_disk;
    probe: crate::probe::Probe => probe;
    producer_endpoint: crate::producer_endpoint::ProducerEndpoint => metric_producer;
    project_image: crate::image::ProjectImage => project_image;
    project: crate::project::Project => project;
    rack: crate::rack::Rack => rack;
    reconfigurator_config: crate::reconfigurator_config::ReconfiguratorConfig => reconfigurator_config;
    region_replacement_step: crate::region_replacement_step::RegionReplacementStep => region_replacement_step;
    region_replacement: crate::region_replacement::RegionReplacement => region_replacement;
    region_snapshot_replacement_step: crate::region_snapshot_replacement_step::RegionSnapshotReplacementStep => region_snapshot_replacement_step;
    region_snapshot_replacement: crate::region_snapshot_replacement::RegionSnapshotReplacement => region_snapshot_replacement;
    region_snapshot: crate::region_snapshot::RegionSnapshot => region_snapshot;
    region: crate::region::Region => region;
    rendezvous_alert_created: crate::fm::RendezvousAlertCreated => rendezvous_alert_created;
    rendezvous_debug_dataset: crate::rendezvous_debug_dataset::RendezvousDebugDataset => rendezvous_debug_dataset;
    rendezvous_local_storage_dataset: crate::local_storage::RendezvousLocalStorageDataset => rendezvous_local_storage_dataset;
    rendezvous_local_storage_unencrypted_dataset: crate::local_storage::RendezvousLocalStorageUnencryptedDataset => rendezvous_local_storage_unencrypted_dataset;
    rendezvous_support_bundle_created: crate::fm::RendezvousSupportBundleCreated => rendezvous_support_bundle_created;
    reporter: crate::ereport::Reporter => ereport;
    resources: crate::sled_resource_vmm::Resources => sled_resource_vmm;
    role_assignment: crate::role_assignment::RoleAssignment => role_assignment;
    router_route: crate::vpc_route::RouterRoute => router_route;
    saga_node_event: crate::saga_types::SagaNodeEvent => saga_node_event;
    saga: crate::saga_types::Saga => saga;
    saml_identity_provider: crate::identity_provider::SamlIdentityProvider => saml_identity_provider;
    scim_client_bearer_token: crate::scim_client_bearer_token::ScimClientBearerToken => scim_client_bearer_token;
    service_network_interface: crate::network_interface::ServiceNetworkInterface => service_network_interface;
    silo_auth_settings: crate::silo_auth_settings::SiloAuthSettings => silo_auth_settings;
    silo_group_membership: crate::silo_group::SiloGroupMembership => silo_group_membership;
    silo_group: crate::silo_group::SiloGroup => silo_group;
    silo_image: crate::image::SiloImage => silo_image;
    silo_quotas: crate::quota::SiloQuotas => silo_quotas;
    silo_user_password_hash: crate::silo_user_password_hash::SiloUserPasswordHash => silo_user_password_hash;
    silo_user: crate::silo_user::SiloUser => silo_user;
    silo_utilization: crate::utilization::SiloUtilization => silo_utilization;
    silo: crate::silo::Silo => silo;
    sitrep_analysis_report: crate::fm::SitrepAnalysisReport => fm_sitrep_analysis_report;
    sitrep_metadata: crate::fm::SitrepMetadata => fm_sitrep;
    sitrep_version: crate::fm::SitrepVersion => fm_sitrep_history;
    sled_instance: crate::sled_instance::SledInstance => sled_instance;
    sled_resource_vmm: crate::sled_resource_vmm::SledResourceVmm => sled_resource_vmm;
    sled_underlay_subnet_allocation: crate::sled_underlay_subnet_allocation::SledUnderlaySubnetAllocation => sled_underlay_subnet_allocation;
    sled: crate::sled::Sled => sled;
    snapshot: crate::snapshot::Snapshot => snapshot;
    ssh_key: crate::ssh_key::SshKey => ssh_key;
    subnet_pool_member: crate::external_subnet::SubnetPoolMember => subnet_pool_member;
    subnet_pool_silo_link: crate::external_subnet::SubnetPoolSiloLink => subnet_pool_silo_link;
    subnet_pool: crate::external_subnet::SubnetPool => subnet_pool;
    support_bundle_data_selection_ereports: crate::support_bundle::Ereports => support_bundle_data_selection_ereports;
    support_bundle_data_selection_flags: crate::support_bundle::DataSelectionFlags => support_bundle_data_selection_flags;
    support_bundle_data_selection_host_info: crate::support_bundle::HostInfo => support_bundle_data_selection_host_info;
    support_bundle_request: crate::fm::SupportBundleRequest => fm_support_bundle_request;
    support_bundle: crate::support_bundle::SupportBundle => support_bundle;
    sw_caboose: crate::inventory::SwCaboose => sw_caboose;
    sw_rot_page: crate::inventory::SwRotPage => sw_root_of_trust_page;
    switch_interface_config: crate::switch_port::SwitchInterfaceConfig => switch_port_settings_interface_config;
    switch_port_address_config: crate::switch_port::SwitchPortAddressConfig => switch_port_settings_address_config;
    switch_port_bgp_peer_config_allow_export: crate::switch_port::SwitchPortBgpPeerConfigAllowExport => switch_port_settings_bgp_peer_config_allow_export;
    switch_port_bgp_peer_config_allow_import: crate::switch_port::SwitchPortBgpPeerConfigAllowImport => switch_port_settings_bgp_peer_config_allow_import;
    switch_port_bgp_peer_config_community: crate::switch_port::SwitchPortBgpPeerConfigCommunity => switch_port_settings_bgp_peer_config_communities;
    switch_port_bgp_peer_config: crate::switch_port::SwitchPortBgpPeerConfig => switch_port_settings_bgp_peer_config;
    switch_port_config: crate::switch_port::SwitchPortConfig => switch_port_settings_port_config;
    switch_port_link_config: crate::switch_port::SwitchPortLinkConfig => switch_port_settings_link_config;
    switch_port_route_config: crate::switch_port::SwitchPortRouteConfig => switch_port_settings_route_config;
    switch_port_settings_group: crate::switch_port::SwitchPortSettingsGroup => switch_port_settings_group;
    switch_port_settings_groups: crate::switch_port::SwitchPortSettingsGroups => switch_port_settings_groups;
    switch_port_settings: crate::switch_port::SwitchPortSettings => switch_port_settings;
    switch_port: crate::switch_port::SwitchPort => switch_port;
    switch_vlan_interface_config: crate::switch_interface::SwitchVlanInterfaceConfig => switch_vlan_interface_config;
    switch: crate::switch::Switch => switch;
    system_networking_settings: crate::system_networking_settings::SystemNetworkingSettings => system_networking_settings;
    target_release: crate::target_release::TargetRelease => target_release;
    trust_quorum_configuration: crate::trust_quorum::TrustQuorumConfiguration => trust_quorum_configuration;
    trust_quorum_member: crate::trust_quorum::TrustQuorumMember => trust_quorum_member;
    tuf_artifact: crate::tuf_repo::TufArtifact => tuf_artifact;
    tuf_repo_artifact: crate::tuf_repo::TufRepoArtifact => tuf_repo_artifact;
    tuf_repo: crate::tuf_repo::TufRepo => tuf_repo;
    tuf_trust_root: crate::tuf_repo::TufTrustRoot => tuf_trust_root;
    tx_eq_config: crate::switch_port::TxEqConfig => tx_eq_config;
    underlay_multicast_group: crate::multicast_group::UnderlayMulticastGroup => underlay_multicast_group;
    upstairs_repair_notification: crate::upstairs_repair::UpstairsRepairNotification => upstairs_repair_notification;
    upstairs_repair_progress: crate::upstairs_repair::UpstairsRepairProgress => upstairs_repair_progress;
    user_builtin: crate::user_builtin::UserBuiltin => user_builtin;
    user_data_export_record: crate::user_data_export::UserDataExportRecord => user_data_export;
    virtual_provisioning_collection: crate::virtual_provisioning_collection::VirtualProvisioningCollection => virtual_provisioning_collection;
    virtual_provisioning_resource: crate::virtual_provisioning_resource::VirtualProvisioningResource => virtual_provisioning_resource;
    vmm: crate::vmm::Vmm => vmm;
    volume_repair: crate::volume_repair::VolumeRepair => volume_repair;
    volume_resource_usage_record: crate::volume_resource_usage::VolumeResourceUsageRecord => volume_resource_usage;
    volume: crate::volume::Volume => volume;
    vpc_firewall_rule: crate::vpc_firewall_rule::VpcFirewallRule => vpc_firewall_rule;
    vpc_router: crate::vpc_router::VpcRouter => vpc_router;
    vpc_subnet: crate::vpc_subnet::VpcSubnet => vpc_subnet;
    vpc: crate::vpc::Vpc => vpc;
    webhook_delivery_attempt: crate::webhook_delivery::WebhookDeliveryAttempt => webhook_delivery_attempt;
    webhook_delivery: crate::webhook_delivery::WebhookDelivery => webhook_delivery;
    webhook_secret: crate::webhook_rx::WebhookSecret => webhook_secret;
    zpool: crate::zpool::Zpool => zpool;
}
