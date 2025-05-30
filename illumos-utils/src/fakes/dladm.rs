// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::dladm::Api;
use crate::dladm::CreateVnicError;
use crate::dladm::DeleteVnicError;
use crate::dladm::FindPhysicalLinkError;
use crate::dladm::VnicSource;
use crate::link::Link;
use omicron_common::api::external::MacAddr;
use omicron_common::vlan::VlanID;
use std::sync::Arc;

/// A fake implementation of [crate::dladm::Dladm].
///
/// This struct implements the [crate::dladm::Api] interface but avoids
/// interacting with the host OS.
pub struct Dladm {}

impl Dladm {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait::async_trait]
impl Api for Dladm {
    async fn create_vnic(
        &self,
        _source: &(dyn VnicSource + 'static),
        _vnic_name: &str,
        _mac: Option<MacAddr>,
        _vlan: Option<VlanID>,
        _mtu: usize,
    ) -> Result<(), CreateVnicError> {
        Ok(())
    }

    async fn verify_link(
        &self,
        link: &str,
    ) -> Result<Link, FindPhysicalLinkError> {
        Ok(Link::wrap_physical(link))
    }

    async fn delete_vnic(&self, _name: &str) -> Result<(), DeleteVnicError> {
        Ok(())
    }
}
