// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack management

use crate::authz;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::external_api::params::CertificateCreate;
use crate::external_api::shared::ServiceUsingCertificate;
use crate::internal_api::params::RackInitializationRequest;
use dpd_client::types::Ipv6Entry;
use dpd_client::types::LinkCreate;
use dpd_client::types::LinkId;
use dpd_client::types::LinkSettings;
use dpd_client::types::PortFec;
use dpd_client::types::PortId;
use dpd_client::types::PortSettings;
use dpd_client::types::PortSpeed;
use dpd_client::types::RouteSettingsV4;
use dpd_client::Ipv4Cidr;
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
    AddressLotCreate, AddressLotKind, LoopbackAddressCreate, Route, SiloCreate,
    SwitchPortSettingsCreate,
};
use nexus_types::external_api::shared::SiloIdentityMode;
use nexus_types::internal_api::params::DnsRecord;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::str::FromStr;
use uuid::Uuid;

use super::sagas::NEXUS_DPD_TAG;

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
        self.populate_switch_ports(&opctx, request.external_port_count).await?;

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
        dns_update.add_name(Self::silo_dns_name(silo_name), dns_records)?;

        let recovery_silo = SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: request.recovery_silo.silo_name,
                description: "built-in recovery Silo".to_string(),
            },
            discoverable: false,
            identity_mode: SiloIdentityMode::LocalOnly,
            admin_group_name: None,
            tls_certificates,
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

        // We've potentially updated both the list of DNS servers and the DNS
        // configuration.  Activate both background tasks, for both internal and
        // external DNS.
        for task in &[
            &self.background_tasks.task_internal_dns_config,
            &self.background_tasks.task_internal_dns_servers,
            &self.background_tasks.task_external_dns_config,
            &self.background_tasks.task_external_dns_servers,
        ] {
            self.background_tasks.activate(task);
        }

        // TODO
        // configure rack networking / boundary services here
        // Currently calling some of the apis directly, but should we be using sagas
        // going forward via self.run_saga()? Note that self.create_runnable_saga and
        // self.execute_saga are currently not available within this scope.
        info!(self.log, "checking for rack networking configuration");
        if let Some(rack_network_config) = &request.rack_network_config {
            info!(self.log, "configuring rack networking");
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

            let first_address = IpAddr::from_str(
                &rack_network_config.infra_ip_first,
            )
            .map_err(|e| {
                Error::internal_error(&format!(
                    "encountered error while parsing `infra_ip_first`: {e}"
                ))
            })?;

            let last_address = IpAddr::from_str(
                &rack_network_config.infra_ip_last,
            )
            .map_err(|e| {
                Error::internal_error(&format!(
                    "encountered error while parsing `infra_ip_last`: {e}"
                ))
            })?;

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

            let switch_location = Name::from_str("switch0").map_err(|e| {
                Error::internal_error(&format!(
                    "unable to use `switch0` as Name: {e}"
                ))
            })?;

            let loopback_address_params = LoopbackAddressCreate {
                address_lot: NameOrId::Name(address_lot_name.clone()),
                rack_id,
                switch_location,
                address: first_address,
                mask: 64,
            };

            let address_lot_lookup = self
                .address_lot_lookup(
                    &opctx,
                    loopback_address_params.address_lot.clone(),
                )
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "unable to lookup infra address_lot: {e}"
                    ))
                })?;

            let (.., authz_address_lot) =
                address_lot_lookup.lookup_for(authz::Action::Modify)
                .await
                .map_err(|e| {
                    Error::internal_error(&format!("unable to retrieve authz_address_lot for infra address_lot: {e}"))
                })?;

            self.db_datastore
                .loopback_address_create(
                    opctx,
                    &loopback_address_params,
                    None,
                    &authz_address_lot,
                )
                .await?;

            let body = Ipv6Entry {
                addr: Ipv6Addr::from_str("fd00:99::1").map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to parse `fd00:99::1` as `Ipv6Addr`: {e}"
                    ))
                })?,
                tag: NEXUS_DPD_TAG.into(),
            };
            self.dpd_client.loopback_ipv6_create(&body).await.map_err(|e| {
                Error::internal_error(&format!(
                    "unable to create inital switch loopback address: {e}"
                ))
            })?;

            let name = Name::from_str("default-uplink").map_err(|e| {
                Error::internal_error(&format!(
                    "unable to use `default-uplink` as `Name`: {e}"
                ))
            })?;

            let identity = IdentityMetadataCreateParams {
                name,
                description: "initial uplink configuration".to_string(),
            };

            let port_config = SwitchPortConfig {
                geometry:
                    nexus_types::external_api::params::SwitchPortGeometry::Qsfp28x1,
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

            let uplink_address = IpNet::from_str(&format!(
                    "{}/32",
                    rack_network_config.uplink_ip
                ))
                .map_err(|e| Error::internal_error(&format!(
                    "failed to parse value provided for `rack_network_config.uplink_ip` as `IpNet`: {e}"
                )))?;

            let address = Address {
                address_lot: NameOrId::Name(address_lot_name),
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

            let gw = IpAddr::from_str(&rack_network_config.gateway_ip)
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to parse provided default gateway address: {e}"
                    ))
                })?;

            let route = Route { dst, gw };

            port_settings_params.routes.insert(
                "phy0".to_string(),
                RouteConfig { routes: vec![route] },
            );

            self.db_datastore
                .switch_port_settings_create(opctx, &port_settings_params)
                .await?;

            let mut dpd_port_settings = PortSettings {
                tag: NEXUS_DPD_TAG.into(),
                links: HashMap::new(),
                v4_routes: HashMap::new(),
                v6_routes: HashMap::new(),
            };

            // TODO handle breakouts
            // https://github.com/oxidecomputer/omicron/issues/3062
            let link_id = LinkId(0);
            let addr = IpAddr::from_str(&rack_network_config.uplink_ip)
                .map_err(|e| {
                    Error::internal_error(&format!(
                    "unable to parse rack_network_config.uplink_up as IpAddr: {e}"))
                })?;

            let link_settings = LinkSettings {
                // TODO Allow user to configure link properties
                // https://github.com/oxidecomputer/omicron/issues/3061
                params: LinkCreate {
                    autoneg: false,
                    kr: false,
                    fec: PortFec::None,
                    speed: PortSpeed::Speed100G,
                },
                addrs: vec![addr],
            };

            dpd_port_settings.links.insert(link_id.to_string(), link_settings);

            let port_id: PortId = PortId::from_str(&rack_network_config.uplink_port)
                .map_err(|e| Error::internal_error(
                        &format!("could not use value provided to rack_network_config.uplink_port as PortID: {e}")
                ))?;

            let nexthop = Some(Ipv4Addr::from_str(&rack_network_config.gateway_ip)
                .map_err(|e| Error::internal_error(
                        &format!("unable to parse rack_network_config.gateway_ip as Ipv4Addr: {e}")
                ))?);

            dpd_port_settings.v4_routes.insert(
                Ipv4Cidr {
                    prefix: Ipv4Addr::from_str("0.0.0.0").unwrap(),
                    prefix_len: 0,
                }
                .to_string(),
                RouteSettingsV4 { link_id: link_id.0, nexthop },
            );

            self.dpd_client
                .port_settings_apply(&port_id, &dpd_port_settings)
                .await
                .map_err(|e| Error::internal_error(&format!("unable to apply initial uplink port configuration: {e}")))?;
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
