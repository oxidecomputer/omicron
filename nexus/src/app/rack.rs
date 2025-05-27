// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack management

use crate::app::CONTROL_PLANE_STORAGE_BUFFER;
use crate::external_api::params;
use crate::external_api::params::CertificateCreate;
use crate::external_api::shared::ServiceUsingCertificate;
use crate::internal_api::params::RackInitializationRequest;
use gateway_client::types::SpType;
use internal_dns_types::names::DNS_ZONE;
use ipnetwork::{IpNetwork, Ipv6Network};
use nexus_db_lookup::LookupPath;
use nexus_db_model::DnsGroup;
use nexus_db_model::INFRA_LOT;
use nexus_db_model::InitialDnsGroup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
use nexus_db_queries::db::datastore::RackInit;
use nexus_db_queries::db::datastore::SledUnderlayAllocationResult;
use nexus_types::deployment::CockroachDbClusterVersion;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::params::Address;
use nexus_types::external_api::params::AddressConfig;
use nexus_types::external_api::params::AddressLotBlockCreate;
use nexus_types::external_api::params::BgpAnnounceSetCreate;
use nexus_types::external_api::params::BgpAnnouncementCreate;
use nexus_types::external_api::params::BgpConfigCreate;
use nexus_types::external_api::params::LinkConfigCreate;
use nexus_types::external_api::params::LldpLinkConfigCreate;
use nexus_types::external_api::params::RouteConfig;
use nexus_types::external_api::params::SwitchPortConfigCreate;
use nexus_types::external_api::params::UninitializedSledId;
use nexus_types::external_api::params::{
    AddressLotCreate, BgpPeerConfig, Route, SiloCreate,
    SwitchPortSettingsCreate,
};
use nexus_types::external_api::shared::Baseboard;
use nexus_types::external_api::shared::FleetRole;
use nexus_types::external_api::shared::SiloIdentityMode;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::external_api::shared::UninitializedSled;
use nexus_types::external_api::views;
use nexus_types::silo::silo_dns_name;
use omicron_common::address::{Ipv6Subnet, RACK_PREFIX, get_64_subnet};
use omicron_common::api::external::AddressLotKind;
use omicron_common::api::external::BgpPeer;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::internal::shared::ExternalPortDiscovery;
use omicron_common::api::internal::shared::LldpAdminStatus;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use oxnet::IpNet;
use sled_agent_client::types::AddSledRequest;
use sled_agent_client::types::StartSledAgentRequest;
use sled_agent_client::types::StartSledAgentRequestBody;

use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::str::FromStr;
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn racks_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Rack> {
        self.db_datastore.rack_list(&opctx, pagparams).await
    }

    pub(crate) async fn rack_lookup(
        &self,
        opctx: &OpContext,
        rack_id: &Uuid,
    ) -> LookupResult<db::model::Rack> {
        let (.., db_rack) = LookupPath::new(opctx, &self.db_datastore)
            .rack_id(*rack_id)
            .fetch()
            .await?;
        Ok(db_rack)
    }

    /// Marks the rack as initialized with information supplied by RSS.
    ///
    /// This function is a no-op if the rack has already been initialized.
    pub(crate) async fn rack_initialize(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        request: RackInitializationRequest,
    ) -> Result<(), Error> {
        let log = &opctx.log;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let physical_disks: Vec<_> = request
            .physical_disks
            .into_iter()
            .map(|disk| {
                db::model::PhysicalDisk::new(
                    disk.id,
                    disk.vendor,
                    disk.serial,
                    disk.model,
                    disk.variant.into(),
                    disk.sled_id,
                )
            })
            .collect();

        let zpools: Vec<_> = request
            .zpools
            .into_iter()
            .map(|pool| {
                db::model::Zpool::new(
                    pool.id,
                    pool.sled_id,
                    pool.physical_disk_id,
                    CONTROL_PLANE_STORAGE_BUFFER.into(),
                )
            })
            .collect();

        let datasets: Vec<_> = request
            .crucible_datasets
            .into_iter()
            .map(|dataset| {
                db::model::CrucibleDataset::new(
                    dataset.dataset_id,
                    dataset.zpool_id.into_untyped_uuid(),
                    dataset.address,
                )
            })
            .collect();

        let service_ip_pool_ranges = request.internal_services_ip_pool_ranges;
        let tls_certificates: Vec<_> = request
            .certs
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                // The indexes that appear in user-visible names for these
                // certificates start from one (e.g., certificate names
                // "default-1", "default-2", etc).
                let i = i + 1;
                CertificateCreate {
                    identity: IdentityMetadataCreateParams {
                        name: Name::try_from(format!("default-{i}")).unwrap(),
                        description: format!(
                            "x.509 certificate #{i} initialized at rack install"
                        ),
                    },
                    cert: c.cert,
                    key: c.key,
                    service: ServiceUsingCertificate::ExternalApi,
                }
            })
            .collect();

        let dns_zone = request
            .internal_dns_zone_config
            .zones
            .into_iter()
            .find(|z| z.zone_name == DNS_ZONE)
            .ok_or_else(|| {
                Error::invalid_request(
                    "expected initial DNS config to include control plane zone",
                )
            })?;

        let internal_dns = InitialDnsGroup::new(
            DnsGroup::Internal,
            &dns_zone.zone_name,
            &self.id.to_string(),
            "rack setup",
            dns_zone.names,
        );

        let external_dns = InitialDnsGroup::new(
            DnsGroup::External,
            request.external_dns_zone_name.as_str(),
            &self.id.to_string(),
            "rack setup",
            HashMap::new(),
        );

        let silo_name = &request.recovery_silo.silo_name;

        let mut dns_update = DnsVersionUpdateBuilder::new(
            DnsGroup::External,
            format!("create silo: {:?}", silo_name.as_str()),
            self.id.to_string(),
        );
        let silo_dns_name = silo_dns_name(silo_name);
        let recovery_silo_fq_dns_name =
            format!("{silo_dns_name}.{}", request.external_dns_zone_name);

        // sled-agent, in service of RSS, has configured internal DNS. We got
        // its DNS configuration in `request.internal_dns_zone_config` and are
        // appending to it before committing the initial RSS state to the
        // database
        let external_dns_config =
            nexus_types::deployment::execution::blueprint_external_dns_config(
                &request.blueprint,
                vec![silo_name],
                request.external_dns_zone_name,
            );
        for (name, records) in external_dns_config.names.into_iter() {
            dns_update.add_name(name, records)?;
        }

        // We're providing an update to the initial `external_dns` group we
        // defined above; also bump RSS's blueprint's `external_dns_version` to
        // match this update.
        let mut blueprint = request.blueprint;
        blueprint.external_dns_version = blueprint.external_dns_version.next();

        // Fill in the CockroachDB metadata for the initial blueprint, and set
        // the `cluster.preserve_downgrade_option` setting ahead of blueprint
        // execution.
        let cockroachdb_settings = self
            .datastore()
            .cockroachdb_settings(opctx)
            .await
            .internal_context(
                "fetching cockroachdb settings for rack initialization",
            )?;
        blueprint.cockroachdb_setting_preserve_downgrade =
            if cockroachdb_settings.preserve_downgrade.is_empty() {
                // Set the option to the current policy in both the database and
                // the blueprint.
                self.datastore()
                    .cockroachdb_setting_set_string(
                        opctx,
                        cockroachdb_settings.state_fingerprint.clone(),
                        "cluster.preserve_downgrade_option",
                        CockroachDbClusterVersion::NEWLY_INITIALIZED
                            .to_string(),
                    )
                    .await
                    .internal_context(
                        "setting `cluster.preserve_downgrade_option` \
                        for rack initialization",
                    )?;
                CockroachDbClusterVersion::NEWLY_INITIALIZED
            } else {
                // `cluster.preserve_downgrade_option` is set, so fill in the
                // blueprint with the current value. This branch should never
                // be hit during normal rack initialization; it's here for
                // eventual test cases where `cluster.preserve_downgrade_option`
                // is set by a test harness prior to rack initialization.
                CockroachDbClusterVersion::from_str(
                    &cockroachdb_settings.preserve_downgrade,
                )
                .map_err(|_| {
                    Error::internal_error(&format!(
                        "database has `cluster.preserve_downgrade_option` \
                        set to invalid version {}",
                        cockroachdb_settings.preserve_downgrade
                    ))
                })?
            }
            .into();
        blueprint.cockroachdb_fingerprint =
            cockroachdb_settings.state_fingerprint;

        // Administrators of the Recovery Silo are automatically made
        // administrators of the Fleet.
        let mapped_fleet_roles = BTreeMap::from([(
            SiloRole::Admin,
            BTreeSet::from([FleetRole::Admin]),
        )]);

        let recovery_silo = SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: request.recovery_silo.silo_name,
                description: "built-in recovery Silo".to_string(),
            },
            // The recovery silo is initialized with no allocated capacity given
            // it's not intended to be used to deploy workloads. Operators can
            // add capacity after the fact if they want to use it for that
            // purpose.
            quotas: params::SiloQuotasCreate::empty(),
            discoverable: false,
            identity_mode: SiloIdentityMode::LocalOnly,
            admin_group_name: None,
            tls_certificates,
            mapped_fleet_roles,
        };

        let rack_network_config = &request.rack_network_config;

        // The `rack` row is created with the rack ID we know when Nexus starts,
        // but we didn't know the rack subnet until now. Set it.
        let mut rack = self.rack_lookup(opctx, &self.rack_id).await?;
        rack.rack_subnet =
            Some(IpNet::from(rack_network_config.rack_subnet).into());
        self.datastore().update_rack_subnet(opctx, &rack).await?;

        // TODO - https://github.com/oxidecomputer/omicron/pull/3359
        // register all switches found during rack initialization
        // identify requested switch from config and associate
        // uplink records to that switch
        match request.external_port_count {
            ExternalPortDiscovery::Auto(switch_mgmt_addrs) => {
                use dpd_client::Client as DpdClient;
                info!(log, "Using automatic external switchport discovery");

                for (switch, addr) in switch_mgmt_addrs {
                    let dpd_client = DpdClient::new(
                        &format!(
                            "http://[{}]:{}",
                            addr,
                            omicron_common::address::DENDRITE_PORT
                        ),
                        dpd_client::ClientState {
                            tag: "nexus".to_string(),
                            log: log.new(o!("component" => "DpdClient")),
                        },
                    );

                    let all_ports =
                        dpd_client.port_list().await.map_err(|e| {
                            Error::internal_error(&format!("encountered error while discovering ports for {switch:#?}: {e}"))
                        })?;

                    info!(log, "discovered ports for {switch}: {all_ports:#?}");

                    let qsfp_ports: Vec<Name> = all_ports
                        .iter()
                        .filter(|port| {
                            matches!(port, dpd_client::types::PortId::Qsfp(_))
                        })
                        .map(|port| port.to_string().parse().unwrap())
                        .collect();

                    info!(
                        log,
                        "populating ports for {switch}: {qsfp_ports:#?}"
                    );

                    self.populate_switch_ports(
                        &opctx,
                        &qsfp_ports,
                        switch.to_string().parse().unwrap(),
                    )
                    .await?;
                }
            }
            // TODO: #3602 Eliminate need for static port mappings for switch ports
            ExternalPortDiscovery::Static(port_mappings) => {
                info!(
                    log,
                    "Using static configuration for external switchports"
                );
                for (switch, ports) in port_mappings {
                    self.populate_switch_ports(
                        &opctx,
                        &ports,
                        switch.to_string().parse().unwrap(),
                    )
                    .await?;
                }
            }
        }

        // TODO
        // configure rack networking / boundary services here
        // Currently calling some of the apis directly, but should we be using
        // sagas going forward via self.sagas.saga_execute()? Note that
        // this may not be available within this scope.
        info!(log, "Recording Rack Network Configuration");
        let address_lot_name = Name::from_str(INFRA_LOT).map_err(|e| {
            Error::internal_error(&format!(
                "unable to use `initial-infra` as `Name`: {e}"
            ))
        })?;
        let identity = IdentityMetadataCreateParams {
            name: address_lot_name.clone(),
            description: "initial infrastructure ip address lot".to_string(),
        };

        let kind = AddressLotKind::Infra;

        let first_address = IpAddr::V4(rack_network_config.infra_ip_first);
        let last_address = IpAddr::V4(rack_network_config.infra_ip_last);
        let ipv4_block = AddressLotBlockCreate { first_address, last_address };

        let blocks = vec![ipv4_block];

        let address_lot_params = AddressLotCreate { identity, kind, blocks };

        match self
            .db_datastore
            .address_lot_create(opctx, &address_lot_params)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => match e {
                Error::ObjectAlreadyExists { type_name: _, object_name: _ } => {
                    Ok(())
                }
                _ => Err(e),
            },
        }?;

        let mut bgp_configs = HashMap::new();

        for bgp_config in &rack_network_config.bgp {
            bgp_configs.insert(bgp_config.asn, bgp_config.clone());

            let bgp_config_name: Name =
                format!("as{}", bgp_config.asn).parse().unwrap();

            let announce_set_name: Name =
                format!("as{}-announce", bgp_config.asn).parse().unwrap();

            let address_lot_name: Name =
                format!("as{}-lot", bgp_config.asn).parse().unwrap();

            match self
                .db_datastore
                .address_lot_create(
                    &opctx,
                    &AddressLotCreate {
                        identity: IdentityMetadataCreateParams {
                            name: address_lot_name,
                            description: format!(
                                "Address lot for announce set in as {}",
                                bgp_config.asn
                            ),
                        },
                        kind: AddressLotKind::Infra,
                        blocks: bgp_config
                            .originate
                            .iter()
                            .map(|o| AddressLotBlockCreate {
                                first_address: o.first_addr().into(),
                                last_address: o.last_addr().into(),
                            })
                            .collect(),
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => match e {
                    Error::ObjectAlreadyExists { .. } => Ok(()),
                    _ => Err(Error::internal_error(&format!(
                        "unable to create address lot for BGP as {}: {e}",
                        bgp_config.asn
                    ))),
                },
            }?;

            match self
                .db_datastore
                .bgp_create_announce_set(
                    &opctx,
                    &BgpAnnounceSetCreate {
                        identity: IdentityMetadataCreateParams {
                            name: announce_set_name.clone(),
                            description: format!(
                                "Announce set for AS {}",
                                bgp_config.asn
                            ),
                        },
                        announcement: bgp_config
                            .originate
                            .iter()
                            .map(|ipv4_net| BgpAnnouncementCreate {
                                address_lot_block: NameOrId::Name(
                                    format!("as{}", bgp_config.asn)
                                        .parse()
                                        .unwrap(),
                                ),
                                network: (*ipv4_net).into(),
                            })
                            .collect(),
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => match e {
                    Error::ObjectAlreadyExists { .. } => Ok(()),
                    _ => Err(Error::internal_error(&format!(
                        "unable to create bgp announce set for as {}: {e}",
                        bgp_config.asn
                    ))),
                },
            }?;

            match self
                .db_datastore
                .bgp_config_create(
                    &opctx,
                    &BgpConfigCreate {
                        identity: IdentityMetadataCreateParams {
                            name: bgp_config_name,
                            description: format!(
                                "BGP config for AS {}",
                                bgp_config.asn
                            ),
                        },
                        asn: bgp_config.asn,
                        bgp_announce_set_id: announce_set_name.into(),
                        vrf: None,
                        shaper: bgp_config.shaper.clone(),
                        checker: bgp_config.checker.clone(),
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => match e {
                    Error::ObjectAlreadyExists { .. } => Ok(()),
                    _ => Err(Error::internal_error(&format!(
                        "unable to set bgp config for as {}: {e}",
                        bgp_config.asn
                    ))),
                },
            }?;
        }

        for (idx, uplink_config) in rack_network_config.ports.iter().enumerate()
        {
            let switch = uplink_config.switch.to_string();
            let switch_location = Name::from_str(&switch).map_err(|e| {
                Error::internal_error(&format!(
                    "unable to use {switch} as Name: {e}"
                ))
            })?;

            let uplink_name = format!("default-uplink{idx}");
            let name = Name::from_str(&uplink_name).unwrap();

            let identity = IdentityMetadataCreateParams {
                name: name.clone(),
                description: "initial uplink configuration".to_string(),
            };

            let port_config = SwitchPortConfigCreate {
                    geometry: nexus_types::external_api::params::SwitchPortGeometry::Qsfp28x1,
                };

            let mut port_settings_params = SwitchPortSettingsCreate {
                identity,
                port_config,
                groups: vec![],
                links: HashMap::new(),
                interfaces: HashMap::new(),
                routes: HashMap::new(),
                bgp_peers: HashMap::new(),
                addresses: HashMap::new(),
            };

            let addresses: Vec<Address> = uplink_config
                .addresses
                .iter()
                .map(|a| Address {
                    address_lot: NameOrId::Name(address_lot_name.clone()),
                    address: a.address,
                    vlan_id: a.vlan_id,
                })
                .collect();

            port_settings_params
                .addresses
                .insert("phy0".to_string(), AddressConfig { addresses });

            let routes: Vec<Route> = uplink_config
                .routes
                .iter()
                .map(|r| Route {
                    dst: r.destination,
                    gw: r.nexthop,
                    vid: r.vlan_id,
                    rib_priority: r.rib_priority,
                })
                .collect();

            port_settings_params
                .routes
                .insert("phy0".to_string(), RouteConfig { routes });

            let peers: Vec<BgpPeer> = uplink_config
                .bgp_peers
                .iter()
                .map(|r| BgpPeer {
                    bgp_config: NameOrId::Name(
                        format!("as{}", r.asn).parse().unwrap(),
                    ),
                    interface_name: "phy0".into(),
                    addr: r.addr.into(),
                    hold_time: r.hold_time() as u32,
                    idle_hold_time: r.idle_hold_time() as u32,
                    delay_open: r.delay_open() as u32,
                    connect_retry: r.connect_retry() as u32,
                    keepalive: r.keepalive() as u32,
                    remote_asn: r.remote_asn,
                    min_ttl: r.min_ttl,
                    md5_auth_key: r.md5_auth_key.clone(),
                    multi_exit_discriminator: r.multi_exit_discriminator,
                    local_pref: r.local_pref,
                    enforce_first_as: r.enforce_first_as,
                    communities: r.communities.clone(),
                    allowed_import: r.allowed_import.clone(),
                    allowed_export: r.allowed_export.clone(),
                    vlan_id: r.vlan_id,
                })
                .collect();

            port_settings_params
                .bgp_peers
                .insert("phy0".to_string(), BgpPeerConfig { peers });

            let lldp = match &uplink_config.lldp {
                None => LldpLinkConfigCreate {
                    enabled: false,
                    ..Default::default()
                },
                Some(l) => LldpLinkConfigCreate {
                    enabled: l.status == LldpAdminStatus::Enabled,
                    link_name: l.port_id.clone(),
                    link_description: l.port_description.clone(),
                    chassis_id: l.chassis_id.clone(),
                    system_name: l.system_name.clone(),
                    system_description: l.system_description.clone(),
                    management_ip: match &l.management_addrs {
                        Some(a) if !a.is_empty() => Some(a[0]),
                        _ => None,
                    },
                },
            };

            let link = LinkConfigCreate {
                //TODO https://github.com/oxidecomputer/omicron/issues/2274
                mtu: 1500,
                fec: uplink_config.uplink_port_fec.map(|fec| fec.into()),
                speed: uplink_config.uplink_port_speed.into(),
                autoneg: uplink_config.autoneg,
                lldp,
                tx_eq: uplink_config.tx_eq.map(|t| t.into()),
            };

            port_settings_params.links.insert("phy".to_string(), link);

            match self
                .db_datastore
                .switch_port_settings_create(opctx, &port_settings_params, None)
                .await
            {
                Ok(_) | Err(Error::ObjectAlreadyExists { .. }) => Ok(()),
                Err(e) => Err(e),
            }?;

            let port_settings_id = self
                .db_datastore
                .switch_port_settings_get_id(
                    opctx,
                    nexus_db_model::Name(name.clone()),
                )
                .await?;

            let switch_port_id = self
                .db_datastore
                .switch_port_get_id(
                    opctx,
                    rack_id,
                    switch_location.into(),
                    Name::from_str(&uplink_config.port).unwrap().into(),
                )
                .await?;

            self.db_datastore
                .switch_port_set_settings_id(
                    opctx,
                    switch_port_id,
                    Some(port_settings_id),
                    db::datastore::UpdatePrecondition::Null,
                )
                .await?;
        } // TODO - https://github.com/oxidecomputer/omicron/issues/3277
        // record port speed

        self.db_datastore
            .rack_set_initialized(
                opctx,
                RackInit {
                    rack_subnet: IpNet::from(rack_network_config.rack_subnet)
                        .into(),
                    rack_id,
                    blueprint,
                    physical_disks,
                    zpools,
                    datasets,
                    service_ip_pool_ranges,
                    internal_dns,
                    external_dns,
                    recovery_silo,
                    recovery_silo_fq_dns_name,
                    recovery_user_id: request.recovery_silo.user_name,
                    recovery_user_password_hash: request
                        .recovery_silo
                        .user_password_hash
                        .into(),
                    dns_update,
                    allowed_source_ips: request.allowed_source_ips,
                },
            )
            .await?;

        // Plumb the firewall rules for the built-in services
        self.plumb_service_firewall_rules(opctx, &[]).await?;

        // We've potentially updated the list of DNS servers and the DNS
        // configuration for both internal and external DNS, plus the Silo
        // certificates.  Activate the relevant background tasks.
        for task in &[
            &self.background_tasks.task_internal_dns_config,
            &self.background_tasks.task_internal_dns_servers,
            &self.background_tasks.task_external_dns_config,
            &self.background_tasks.task_external_dns_servers,
            &self.background_tasks.task_external_endpoints,
            &self.background_tasks.task_inventory_collection,
        ] {
            self.background_tasks.activate(task);
        }

        Ok(())
    }

    /// Awaits the initialization of the rack.
    ///
    /// This will occur by either:
    /// 1. RSS invoking the internal API, handing off responsibility, or
    /// 2. Re-reading a value from the DB, if the rack has already been
    ///    initialized.
    ///
    /// See RFD 278 for additional context.
    pub(crate) async fn await_rack_initialization(&self, opctx: &OpContext) {
        loop {
            let result = self.rack_lookup(&opctx, &self.rack_id).await;
            match result {
                Ok(rack) => {
                    if rack.initialized {
                        info!(self.log, "Rack initialized");
                        return;
                    }
                    info!(
                        self.log,
                        "Still waiting for rack initialization: {:?}", rack
                    );
                }
                Err(e) => {
                    warn!(self.log, "Cannot look up rack: {}", e);
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    /// Return the list of sleds that are inserted into an initialized rack
    /// but not yet initialized as part of a rack.
    //
    // TODO-multirack: We currently limit sleds to a single rack and we also
    // retrieve the `rack_uuid` from the Nexus instance used.
    pub(crate) async fn sled_list_uninitialized(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<UninitializedSled> {
        debug!(self.log, "Getting latest collection");
        // Grab the SPs from the last collection
        let collection =
            self.db_datastore.inventory_get_latest_collection(opctx).await?;

        // There can't be any uninitialized sleds we know about
        // if there is no inventory.
        let Some(collection) = collection else {
            return Ok(vec![]);
        };

        let pagparams = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Descending,
            // TODO: This limit is only suitable for a single sled cluster
            limit: NonZeroU32::new(32).unwrap(),
        };

        debug!(self.log, "Listing sleds");
        let sleds = self
            .db_datastore
            .sled_list(opctx, &pagparams, SledFilter::InService)
            .await?;

        let mut uninitialized_sleds: Vec<UninitializedSled> = collection
            .sps
            .into_iter()
            .filter_map(|(k, v)| {
                if v.sp_type == SpType::Sled {
                    Some(UninitializedSled {
                        baseboard: Baseboard {
                            serial: k.serial_number.clone(),
                            part: k.part_number.clone(),
                            revision: v.baseboard_revision,
                        },
                        rack_id: self.rack_id,
                        cubby: v.sp_slot,
                    })
                } else {
                    None
                }
            })
            .collect();

        let sled_baseboards: BTreeSet<Baseboard> =
            sleds.into_iter().map(|s| views::Sled::from(s).baseboard).collect();

        // Retain all sleds that exist but are not in the sled table
        uninitialized_sleds.retain(|s| !sled_baseboards.contains(&s.baseboard));
        Ok(uninitialized_sleds)
    }

    /// Add a sled to an intialized rack
    pub(crate) async fn sled_add(
        &self,
        opctx: &OpContext,
        sled: UninitializedSledId,
    ) -> Result<SledUuid, Error> {
        let baseboard_id = sled.clone().into();
        let hw_baseboard_id = self
            .db_datastore
            .find_hw_baseboard_id(opctx, &baseboard_id)
            .await?;

        let subnet = self.db_datastore.rack_subnet(opctx, self.rack_id).await?;
        let rack_subnet =
            Ipv6Subnet::<RACK_PREFIX>::from(rack_subnet(Some(subnet))?);

        let allocation = match self
            .db_datastore
            .allocate_sled_underlay_subnet_octets(
                opctx,
                self.rack_id,
                hw_baseboard_id,
            )
            .await?
        {
            SledUnderlayAllocationResult::New(allocation) => allocation,
            SledUnderlayAllocationResult::CommissionedSled(allocation) => {
                return Err(Error::ObjectAlreadyExists {
                    type_name: ResourceType::Sled,
                    object_name: format!(
                        "{} ({}): {}",
                        sled.serial, sled.part, allocation.sled_id
                    ),
                });
            }
        };

        // Convert `UninitializedSledId` to the sled-agent type
        let baseboard_id = sled_agent_client::types::BaseboardId {
            serial_number: sled.serial.clone(),
            part_number: sled.part.clone(),
        };

        // Make the call to sled-agent
        let req = AddSledRequest {
            sled_id: baseboard_id,
            start_request: StartSledAgentRequest {
                generation: 0,
                schema_version: 1,
                body: StartSledAgentRequestBody {
                    id: allocation.sled_id.into(),
                    rack_id: allocation.rack_id,
                    use_trust_quorum: true,
                    is_lrtq_learner: true,
                    subnet: sled_agent_client::types::Ipv6Subnet {
                        net: get_64_subnet(
                            rack_subnet,
                            allocation.subnet_octet.try_into().unwrap(),
                        )
                        .net(),
                    },
                },
            },
        };

        // This timeout value is fairly arbitrary (as they usually are).  As of
        // this writing, this operation is known to take close to two minutes on
        // production hardware.
        let dur = std::time::Duration::from_secs(300);
        let sa_url = self.get_any_sled_agent_url(opctx).await?;
        let reqwest_client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .map_err(|e| {
                Error::internal_error(&format!(
                    "failed to create reqwest client for sled agent: {}",
                    InlineErrorChain::new(&e)
                ))
            })?;
        let sa = sled_agent_client::Client::new_with_client(
            &sa_url,
            reqwest_client,
            self.log.new(o!("sled_agent_url" => sa_url.clone())),
        );
        sa.sled_add(&req).await.map_err(|e| {
            Error::internal_error(&format!(
                "failed to add sled with baseboard {:?} to rack {}: {}",
                sled,
                allocation.rack_id,
                InlineErrorChain::new(&e)
            ))
        })?;
        Ok(allocation.sled_id.into())
    }

    async fn get_any_sled_agent_url(
        &self,
        opctx: &OpContext,
    ) -> Result<String, Error> {
        let addr = self
            .sled_list(opctx, &DataPageParams::max_page())
            .await?
            .get(0)
            .ok_or(Error::InternalError {
                internal_message: "no sled agents available".into(),
            })?
            .address();
        Ok(format!("http://{}", addr))
    }
}

pub fn rack_subnet(
    rack_subnet: Option<IpNetwork>,
) -> Result<Ipv6Network, Error> {
    match rack_subnet {
        Some(IpNetwork::V6(subnet)) => Ok(subnet),
        Some(IpNetwork::V4(_)) => {
            return Err(Error::InternalError {
                internal_message: "rack subnet not IPv6".into(),
            });
        }
        None => {
            return Err(Error::InternalError {
                internal_message: "rack subnet not set".into(),
            });
        }
    }
}
