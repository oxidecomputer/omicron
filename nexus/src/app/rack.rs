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
use nexus_db_model::DnsGroup;
use nexus_db_model::InitialDnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
use nexus_db_queries::db::datastore::RackInit;
use nexus_types::external_api::params::SiloCreate;
use nexus_types::external_api::shared::SiloIdentityMode;
use nexus_types::internal_api::params::DnsRecord;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::Name;
use std::collections::HashMap;
use std::net::IpAddr;
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
