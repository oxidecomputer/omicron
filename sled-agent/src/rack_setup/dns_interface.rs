// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Stubs for the DNS server interface.
//!
//! Defined to make testing easier.

use dns_service_client::multiclient::AAAARecord;
use dns_service_client::multiclient::DnsError;
use dns_service_client::multiclient::ResolveError;
use dns_service_client::multiclient::Resolver;
use dns_service_client::multiclient::Updater;
use omicron_common::address::{Ipv6Subnet, AZ_PREFIX};
use slog::Logger;
use std::collections::HashMap;
use std::net::IpAddr;

pub(crate) trait DnsInterface: Send + Sync + 'static {
    fn new_resolver(
        &self,
        address: Ipv6Subnet<AZ_PREFIX>,
    ) -> Box<dyn DnsResolverInterface>;

    fn new_updater(
        &self,
        address: Ipv6Subnet<AZ_PREFIX>,
        log: Logger,
    ) -> Box<dyn DnsUpdaterInterface>;
}

pub(crate) struct RealDnsAccess {}

impl DnsInterface for RealDnsAccess {
    fn new_resolver(
        &self,
        address: Ipv6Subnet<AZ_PREFIX>,
    ) -> Box<dyn DnsResolverInterface> {
        Box::new(
            Resolver::new(&address).expect("Failed to create DNS resolver"),
        )
    }

    fn new_updater(
        &self,
        address: Ipv6Subnet<AZ_PREFIX>,
        log: Logger,
    ) -> Box<dyn DnsUpdaterInterface> {
        Box::new(Updater::new(&address, log))
    }
}

#[async_trait::async_trait]
pub(crate) trait DnsResolverInterface: Send + Sync + 'static {
    async fn lookup_ip(
        &self,
        srv: internal_dns_names::SRV,
    ) -> Result<IpAddr, ResolveError>;
}

#[async_trait::async_trait]
impl DnsResolverInterface for Resolver {
    async fn lookup_ip(
        &self,
        srv: internal_dns_names::SRV,
    ) -> Result<IpAddr, ResolveError> {
        self.lookup_ip(srv).await
    }
}

#[async_trait::async_trait]
pub(crate) trait DnsUpdaterInterface: Send + Sync + 'static {
    async fn insert_dns_records(
        &self,
        records: &HashMap<internal_dns_names::SRV, Vec<AAAARecord>>,
    ) -> Result<(), DnsError>;
}

#[async_trait::async_trait]
impl DnsUpdaterInterface for Updater {
    async fn insert_dns_records(
        &self,
        records: &HashMap<internal_dns_names::SRV, Vec<AAAARecord>>,
    ) -> Result<(), DnsError> {
        Updater::insert_dns_records(self, records).await
    }
}
