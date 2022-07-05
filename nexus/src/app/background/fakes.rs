// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test-only implementations of interfaces used by background tasks.

use super::interfaces::{
    DnsUpdaterInterface, NexusInterface, SledClientInterface,
};

use crate::db::datastore::DataStore;
use async_trait::async_trait;
use internal_dns_client::{
    multiclient::{AAAARecord, DnsError},
    names::SRV,
};
use omicron_common::address::{Ipv6Subnet, RACK_PREFIX};
use omicron_common::api::external::Error;
use sled_agent_client::types as SledAgentTypes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// A fake implementation of a Sled Agent client.
///
/// In lieu of any networked requests, stores onto the requested services and
/// datasets for later inspection.
pub struct FakeSledClientInner {
    service_request: Option<SledAgentTypes::ServiceEnsureBody>,
    dataset_requests: Vec<SledAgentTypes::DatasetEnsureBody>,
}

#[derive(Clone)]
pub struct FakeSledClient {
    inner: Arc<Mutex<FakeSledClientInner>>,
}

impl FakeSledClient {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(Mutex::new(FakeSledClientInner {
                service_request: None,
                dataset_requests: vec![],
            })),
        })
    }

    pub fn service_requests(&self) -> Vec<SledAgentTypes::ServiceRequest> {
        self.inner
            .lock()
            .unwrap()
            .service_request
            .as_ref()
            .map(|request| request.services.clone())
            .unwrap_or(vec![])
    }

    pub fn dataset_requests(&self) -> Vec<SledAgentTypes::DatasetEnsureBody> {
        self.inner.lock().unwrap().dataset_requests.clone()
    }
}

#[async_trait]
impl SledClientInterface for FakeSledClient {
    async fn services_put(
        &self,
        body: &SledAgentTypes::ServiceEnsureBody,
    ) -> Result<(), Error> {
        let old =
            self.inner.lock().unwrap().service_request.replace(body.clone());
        assert!(
            old.is_none(),
            "Should only set services once (was {old:?}, inserted {body:?})"
        );
        Ok(())
    }

    async fn filesystem_put(
        &self,
        body: &SledAgentTypes::DatasetEnsureBody,
    ) -> Result<(), Error> {
        self.inner.lock().unwrap().dataset_requests.push(body.clone());
        Ok(())
    }
}

/// Provides an abstraction of Nexus which can be used by tests.
///
/// Wraps a real datastore, but fakes out all networked requests.
#[derive(Clone)]
pub struct FakeNexus {
    datastore: Arc<DataStore>,
    rack_id: Uuid,
    rack_subnet: Ipv6Subnet<RACK_PREFIX>,
    sleds: Arc<Mutex<HashMap<Uuid, Arc<FakeSledClient>>>>,
}

impl FakeNexus {
    pub fn new(
        datastore: Arc<DataStore>,
        rack_subnet: Ipv6Subnet<RACK_PREFIX>,
    ) -> Arc<Self> {
        Arc::new(Self {
            datastore,
            rack_id: Uuid::new_v4(),
            rack_subnet,
            sleds: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl NexusInterface<FakeSledClient> for FakeNexus {
    fn rack_id(&self) -> Uuid {
        self.rack_id
    }

    fn rack_subnet(&self) -> Ipv6Subnet<RACK_PREFIX> {
        self.rack_subnet
    }

    fn datastore(&self) -> &Arc<DataStore> {
        &self.datastore
    }

    async fn sled_client(
        &self,
        id: &Uuid,
    ) -> Result<Arc<FakeSledClient>, Error> {
        let sled = self
            .sleds
            .lock()
            .unwrap()
            .entry(*id)
            .or_insert_with(|| FakeSledClient::new())
            .clone();
        Ok(sled)
    }
}

/// A fake implementation of the DNS updater.
///
/// Avoids all networking, instead storing all outgoing requests for later
/// inspection.
#[derive(Clone)]
pub struct FakeDnsUpdater {
    records: Arc<Mutex<HashMap<SRV, Vec<AAAARecord>>>>,
}

impl FakeDnsUpdater {
    pub fn new() -> Self {
        Self { records: Arc::new(Mutex::new(HashMap::new())) }
    }

    // Get a copy of all records.
    pub fn records(&self) -> HashMap<SRV, Vec<AAAARecord>> {
        self.records.lock().unwrap().clone()
    }
}

#[async_trait]
impl DnsUpdaterInterface for FakeDnsUpdater {
    async fn insert_dns_records(
        &self,
        records: &HashMap<SRV, Vec<AAAARecord>>,
    ) -> Result<(), DnsError> {
        let mut our_records = self.records.lock().unwrap();
        for (k, v) in records {
            let old = our_records.insert(k.clone(), v.clone());
            assert!(
                old.is_none(),
                "Inserted key {k}, but found old value: {old:?}"
            );
        }
        Ok(())
    }
}
