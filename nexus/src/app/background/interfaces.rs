// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces which can be faked out for testing.

use async_trait::async_trait;
use crate::db::datastore::DataStore;
use crate::Nexus;
use internal_dns_client::{
    multiclient::{
        AAAARecord,
        DnsError,
        Updater as DnsUpdater
    },
    names::SRV,
};
use omicron_common::address::{
    RACK_PREFIX, Ipv6Subnet,
};
use omicron_common::api::external::Error;
use sled_agent_client::types as SledAgentTypes;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

// A trait intended to aid testing.
//
// The non-test implementation should be as simple as possible.
#[async_trait]
pub trait SledClientInterface {
    async fn services_put(&self, body: &SledAgentTypes::ServiceEnsureBody) -> Result<(), Error>;
    async fn filesystem_put(&self, body: &SledAgentTypes::DatasetEnsureBody) -> Result<(), Error>;
}

#[async_trait]
impl SledClientInterface for sled_agent_client::Client {
    async fn services_put(&self, body: &SledAgentTypes::ServiceEnsureBody) -> Result<(), Error> {
        self.services_put(body).await?;
        Ok(())
    }

    async fn filesystem_put(&self, body: &SledAgentTypes::DatasetEnsureBody) -> Result<(), Error> {
        self.filesystem_put(body).await?;
        Ok(())
    }
}

// A trait intended to aid testing.
//
// The non-test implementation should be as simple as possible.
#[async_trait]
pub trait NexusInterface<SledClient: SledClientInterface> {
    fn rack_id(&self) -> Uuid;
    fn rack_subnet(&self) -> Ipv6Subnet<RACK_PREFIX>;
    fn datastore(&self) -> &Arc<DataStore>;
    async fn sled_client(&self, id: &Uuid) -> Result<Arc<SledClient>, Error>;
}

#[async_trait]
impl NexusInterface<sled_agent_client::Client> for Nexus {
    fn rack_id(&self) -> Uuid {
        self.rack_id
    }

    fn rack_subnet(&self) -> Ipv6Subnet<RACK_PREFIX> {
        self.rack_subnet
    }

    fn datastore(&self) -> &Arc<DataStore> {
        self.datastore()
    }

    async fn sled_client(&self, id: &Uuid) -> Result<Arc<sled_agent_client::Client>, Error> {
        self.sled_client(id).await
    }
}

// A trait intended to aid testing.
//
// The non-test implementation should be as simple as possible.
#[async_trait]
pub trait DnsUpdaterInterface {
    async fn insert_dns_records(&self, records: &HashMap<SRV, Vec<AAAARecord>>) -> Result<(), DnsError>;
}

#[async_trait]
impl DnsUpdaterInterface for DnsUpdater {
    async fn insert_dns_records(&self, records: &HashMap<SRV, Vec<AAAARecord>>) -> Result<(), DnsError> {
        self.insert_dns_records(records).await
    }
}
