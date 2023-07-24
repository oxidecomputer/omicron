// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack management

use super::silo::silo_dns_name;
use crate::authz;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::external_api::params::CertificateCreate;
use crate::external_api::shared::ServiceUsingCertificate;
use crate::internal_api::params::RackInitializationRequest;
use nexus_db_model::DnsGroup;
use nexus_db_model::InitialDnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
use nexus_db_queries::db::datastore::RackInit;
use nexus_types::external_api::params::Address;
use nexus_types::external_api::params::AddressConfig;
use nexus_types::external_api::params::AddressLotBlockCreate;
use nexus_types::external_api::params::RouteConfig;
use nexus_types::external_api::params::SwitchPortConfig;
use nexus_types::external_api::params::{
    AddressLotCreate, LoopbackAddressCreate, Route, SiloCreate,
    SwitchPortSettingsCreate,
};
use nexus_types::external_api::shared::FleetRole;
use nexus_types::external_api::shared::SiloIdentityMode;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::internal_api::params::DnsRecord;
use omicron_common::api::external::AddressLotKind;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_common::api::internal::shared::ExternalPortDiscovery;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use uuid::Uuid;

impl super::Nexus {
    pub async fn racks_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Rack> {
        self.db_datastore.rack_list(&opctx, pagparams).await
    }

    pub async fn rack_lookup(
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

    /// Ensures that a rack exists in the DB.
    ///
    /// If the rack already exists, this function is a no-op.
    pub async fn rack_insert(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
    ) -> Result<(), Error> {
        self.datastore()
            .rack_insert(opctx, &db::model::Rack::new(rack_id))
            .await?;
        Ok(())
    }

    /// Marks the rack as initialized with a set of services.
    ///
    /// This function is a no-op if the rack has already been initialized.
    pub async fn rack_initialize(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        request: RackInitializationRequest,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let datasets: Vec<_> = request
            .datasets
            .into_iter()
            .map(|dataset| {
                db::model::Dataset::new(
                    dataset.dataset_id,
                    dataset.zpool_id,
                    dataset.request.address,
                    dataset.request.kind.into(),
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
                        description: format!("x.509 certificate #{i} initialized at rack install"),
                    },
                    cert: c.cert,
                    key: c.key,
                    service: ServiceUsingCertificate::ExternalApi,
                }
            })
            .collect();

        // internally ignores ObjectAlreadyExists, so will not error on repeat runs
        let _ = self.populate_mock_system_updates(&opctx).await?;

        let dns_zone = request
            .internal_dns_zone_config
            .zones
            .into_iter()
            .find(|z| z.zone_name == internal_dns::DNS_ZONE)
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
            dns_zone.records,
        );

        let external_dns = InitialDnsGroup::new(
            DnsGroup::External,
            request.external_dns_zone_name.as_str(),
            &self.id.to_string(),
            "rack setup",
            HashMap::new(),
        );

        let silo_name = &request.recovery_silo.silo_name;
        let dns_records = request
            .services
            .iter()
            .filter_map(|s| match &s.kind {
                nexus_types::internal_api::params::ServiceKind::Nexus {
                    external_address,
                    ..
                } => Some(match external_address {
                    IpAddr::V4(addr) => DnsRecord::A(*addr),
                    IpAddr::V6(addr) => DnsRecord::Aaaa(*addr),
                }),
                _ => None,
            })
            .collect();
        let mut dns_update = DnsVersionUpdateBuilder::new(
            DnsGroup::External,
            format!("create silo: {:?}", silo_name),
            self.id.to_string(),
        );
        dns_update.add_name(silo_dns_name(silo_name), dns_records)?;

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
            discoverable: false,
            identity_mode: SiloIdentityMode::LocalOnly,
            admin_group_name: None,
            tls_certificates,
            mapped_fleet_roles,
        };

        self.db_datastore
            .rack_set_initialized(
                opctx,
                RackInit {
                    rack_id,
                    services: request.services,
                    datasets,
                    service_ip_pool_ranges,
                    internal_dns,
                    external_dns,
                    recovery_silo,
                    recovery_user_id: request.recovery_silo.user_name,
                    recovery_user_password_hash: request
                        .recovery_silo
                        .user_password_hash
                        .into(),
                    dns_update,
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
        ] {
            self.background_tasks.activate(task);
        }

        // TODO - https://github.com/oxidecomputer/omicron/pull/3359
        // register all switches found during rack initialization
        // identify requested switch from config and associate
        // uplink records to that switch
        match request.external_port_count {
            ExternalPortDiscovery::Auto(switch_mgmt_addrs) => {
                use dpd_client::Client as DpdClient;
                info!(
                    self.log,
                    "Using automatic external switchport discovery"
                );

                for (switch, addr) in switch_mgmt_addrs {
                    let dpd_client = DpdClient::new(
                        &format!(
                            "http://[{}]:{}",
                            addr,
                            omicron_common::address::DENDRITE_PORT
                        ),
                        dpd_client::ClientState {
                            tag: "nexus".to_string(),
                            log: self.log.new(o!("component" => "DpdClient")),
                        },
                    );

                    let all_ports =
                        dpd_client.port_list().await.map_err(|e| {
                            Error::internal_error(&format!("encountered error while discovering ports for {switch:#?}: {e}"))
                        })?;

                    info!(
                        self.log,
                        "discovered ports for {switch}: {all_ports:#?}"
                    );

                    let qsfp_ports: Vec<Name> = all_ports
                        .iter()
                        .filter(|port| port.starts_with("qsfp"))
                        .map(|port| port.to_string().parse().unwrap())
                        .collect();

                    info!(
                        self.log,
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
                    self.log,
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
        // Currently calling some of the apis directly, but should we be using sagas
        // going forward via self.run_saga()? Note that self.create_runnable_saga and
        // self.execute_saga are currently not available within this scope.
        info!(self.log, "Checking for Rack Network Configuration");
        if let Some(rack_network_config) = &request.rack_network_config {
            info!(self.log, "Recording Rack Network Configuration");
            let address_lot_name =
                Name::from_str("initial-infra").map_err(|e| {
                    Error::internal_error(&format!(
                        "unable to use `initial-infra` as `Name`: {e}"
                    ))
                })?;
            let identity = IdentityMetadataCreateParams {
                name: address_lot_name.clone(),
                description: "initial infrastructure ip address lot"
                    .to_string(),
            };

            let kind = AddressLotKind::Infra;

            let first_address = IpAddr::V4(rack_network_config.infra_ip_first);
            let last_address = IpAddr::V4(rack_network_config.infra_ip_last);
            let ipv4_block =
                AddressLotBlockCreate { first_address, last_address };

            let first_address =
                IpAddr::from_str("fd00:99::1").map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to parse `fd00:99::1` as `IpAddr`: {e}"
                    ))
                })?;

            let last_address =
                IpAddr::from_str("fd00:99::ffff").map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to parse `fd00:99::ffff` as `IpAddr`: {e}"
                    ))
                })?;

            let ipv6_block =
                AddressLotBlockCreate { first_address, last_address };

            let blocks = vec![ipv4_block, ipv6_block];

            let address_lot_params =
                AddressLotCreate { identity, kind, blocks };

            match self
                .db_datastore
                .address_lot_create(opctx, &address_lot_params)
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => match e {
                    Error::ObjectAlreadyExists {
                        type_name: _,
                        object_name: _,
                    } => Ok(()),
                    _ => Err(e),
                },
            }?;

            let address_lot_lookup = self
                .address_lot_lookup(
                    &opctx,
                    NameOrId::Name(address_lot_name.clone()),
                )
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "unable to lookup infra address_lot: {e}"
                    ))
                })?;

            let (.., authz_address_lot) = address_lot_lookup
                .lookup_for(authz::Action::Modify)
                .await
                .map_err(|e| {
                    Error::internal_error(&format!("unable to retrieve authz_address_lot for infra address_lot: {e}"))
                })?;

            for (idx, uplink_config) in
                rack_network_config.uplinks.iter().enumerate()
            {
                let switch = uplink_config.switch.to_string();
                let switch_location = Name::from_str(&switch).map_err(|e| {
                    Error::internal_error(&format!(
                        "unable to use {switch} as Name: {e}"
                    ))
                })?;

                // TODO: #3603 Use separate address lots for loopback addresses and infra ips
                let loopback_address_params = LoopbackAddressCreate {
                    address_lot: NameOrId::Name(address_lot_name.clone()),
                    rack_id,
                    switch_location: switch_location.clone(),
                    address: first_address,
                    mask: 64,
                    anycast: true,
                };

                if self
                    .loopback_address_lookup(
                        &opctx,
                        rack_id,
                        switch_location.clone().into(),
                        ipnetwork::IpNetwork::new(
                            loopback_address_params.address,
                            loopback_address_params.mask,
                        )
                        .map_err(|_| {
                            Error::invalid_request("invalid loopback address")
                        })?
                        .into(),
                    )?
                    .lookup_for(authz::Action::Read)
                    .await
                    .is_err()
                {
                    self.db_datastore
                        .loopback_address_create(
                            opctx,
                            &loopback_address_params,
                            None,
                            &authz_address_lot,
                        )
                        .await?;
                }
                let uplink_name = format!("default-uplink{idx}");
                let name = Name::from_str(&uplink_name).unwrap();

                let identity = IdentityMetadataCreateParams {
                    name: name.clone(),
                    description: "initial uplink configuration".to_string(),
                };

                let port_config = SwitchPortConfig {
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

                let uplink_address =
                    IpNet::V4(Ipv4Net(uplink_config.uplink_cidr));
                let address = Address {
                    address_lot: NameOrId::Name(address_lot_name.clone()),
                    address: uplink_address,
                };
                port_settings_params.addresses.insert(
                    "phy0".to_string(),
                    AddressConfig { addresses: vec![address] },
                );

                let dst = IpNet::from_str("0.0.0.0/0").map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to parse provided default route CIDR: {e}"
                    ))
                })?;

                let gw = IpAddr::V4(uplink_config.gateway_ip);
                let vid = uplink_config.uplink_vid;
                let route = Route { dst, gw, vid };

                port_settings_params.routes.insert(
                    "phy0".to_string(),
                    RouteConfig { routes: vec![route] },
                );

                match self
                    .db_datastore
                    .switch_port_settings_create(opctx, &port_settings_params)
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
                        Name::from_str(&uplink_config.uplink_port)
                            .unwrap()
                            .into(),
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
        };

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
    pub async fn await_rack_initialization(&self, opctx: &OpContext) {
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
}
