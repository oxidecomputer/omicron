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
use crate::internal_api::params::ServiceKind;
use crate::internal_api::params::ServicePutRequest;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name;
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
        let certificates: Vec<_> = request
            .certs
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                // The indexes that appear in user-visible names for these
                // certificates start from one (e.g., certificate names
                // "default-1", "default-2", etc).
                let i = i + 1;
                db::model::Certificate::new(
                    Uuid::new_v4(),
                    db::model::ServiceKind::Nexus,
                    CertificateCreate {
                        identity: IdentityMetadataCreateParams {
                            name: Name::try_from(format!("default-{i}")).unwrap(),
                            description: format!("x.509 certificate #{i} initialized at rack install"),
                        },
                        cert: c.cert,
                        key: c.key,
                        service: ServiceUsingCertificate::ExternalApi,
                    }
                ).map_err(|e| Error::from(e))
            })
            .collect::<Result<_, Error>>()?;

        // internally ignores ObjectAlreadyExists, so will not error on repeat runs
        let _ = self.populate_mock_system_updates(&opctx).await?;

        self.db_datastore
            .rack_set_initialized(
                opctx,
                rack_id,
                &request.services,
                datasets,
                service_ip_pool_ranges,
                certificates,
            )
            .await?;

        // Configure the OPTE ports for any services that were initialized
        self.setup_service_opte_ports(&opctx, &request.services).await?;

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

    async fn setup_service_opte_ports(
        &self,
        opctx: &OpContext,
        services: &[ServicePutRequest],
    ) -> Result<(), Error> {
        for service in services {
            // Nexus is currently the only service that makes use of OPTE
            let ServiceKind::Nexus { external_address } = service.kind else {
                continue;
            };

            let (authz_service, _) =
                db::lookup::LookupPath::new(opctx, &self.db_datastore)
                    .service_id(service.service_id)
                    .fetch()
                    .await?;
            let service_nics = self
                .db_datastore
                .derive_service_network_interface_info(opctx, &authz_service)
                .await?;

            // For each NIC, we need to create a NAT entry on the switch
            let dpd_client = &self.dpd_client;

            // Grab the IP address of the sled this service is running on
            let sled = self.sled_lookup(opctx, &service.sled_id).await?;
            let sled_ip = sled.ip();

            for service_nic in service_nics {
                let mac = service_nic
                    .mac
                    .parse::<macaddr::MacAddr6>()
                    .map_err(|e| {
                        Error::internal_error(&format!(
                            "invalid MAC for service NIC: {e}"
                        ))
                    })?;

                let nat_target = dpd_client::types::NatTarget {
                    inner_mac: dpd_client::types::MacAddr {
                        a: mac.into_array().to_vec(),
                    },
                    internal_ip: sled_ip,
                    vni: service_nic.vni.0.into(),
                };

                // TODO-cleanup: Currently nexus is the only service that uses
                // OPTE and is just given a single Service External IP.
                // We'll want to also support services that only need outbound
                // access and thus can use SNAT External IPs.
                let first_port = 0;
                let last_port = u16::MAX;

                match external_address {
                    std::net::IpAddr::V4(ip4) => {
                        dpd_client
                            .nat_ipv4_create(
                                &ip4,
                                first_port,
                                last_port,
                                &nat_target,
                            )
                            .await
                    }
                    std::net::IpAddr::V6(ip6) => {
                        dpd_client
                            .nat_ipv6_create(
                                &ip6,
                                first_port,
                                last_port,
                                &nat_target,
                            )
                            .await
                    }
                }
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to create NAT entry for service NIC: {e}"
                    ))
                })?;
            }

            // Propogate firewall rules
            self.plumb_fw_rules_for_service(&opctx, service).await?;
        }

        Ok(())
    }

    async fn plumb_fw_rules_for_service(
        &self,
        opctx: &OpContext,
        service: &ServicePutRequest,
    ) -> Result<(), Error> {
        // The VPC names are derived from the service kind & ID.
        // See `DataStore::create_service_vpc`.
        let name = format!("{}-{}", service.kind, service.service_id)
            .parse::<Name>()
            .unwrap()
            .into();

        // Lookup the VPC in the built-in services project
        let vpc_lookup = db::lookup::LookupPath::new(opctx, &self.db_datastore)
            .project_id(*db::fixed_data::project::SERVICE_PROJECT_ID)
            .vpc_name(&name);

        let rules = self.vpc_list_firewall_rules(opctx, &vpc_lookup).await?;
        let (_, _, _, vpc) = vpc_lookup.fetch().await?;

        self.send_sled_agents_firewall_rules(opctx, &vpc, &rules).await?;

        Ok(())
    }
}
