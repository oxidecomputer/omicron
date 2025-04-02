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

pub struct Dladm {}

impl Dladm {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Api for Dladm {
    fn create_vnic(
        &self,
        _source: &(dyn VnicSource + 'static),
        _vnic_name: &str,
        _mac: Option<MacAddr>,
        _vlan: Option<VlanID>,
        _mtu: usize,
    ) -> Result<(), CreateVnicError> {
        Ok(())
    }

    fn verify_link(&self, link: &str) -> Result<Link, FindPhysicalLinkError> {
        Ok(Link::wrap_physical(link))
    }

    fn delete_vnic(&self, _name: &str) -> Result<(), DeleteVnicError> {
        Ok(())
    }
}
