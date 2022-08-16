// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External IP addresses for instances

use crate::authz;
use crate::context::OpContext;
use crate::db::lookup::LookupPath;
use crate::db::model::IpKind;
use crate::db::model::Name;
use crate::external_api::views::ExternalIp;
use omicron_common::api::external::ListResultVec;

impl super::Nexus {
    pub async fn instance_list_external_ips(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> ListResultVec<ExternalIp> {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::Read)
            .await?;
        Ok(self
            .db_datastore
            .instance_lookup_external_ips(opctx, authz_instance.id())
            .await?
            .into_iter()
            .filter_map(|ip| {
                if ip.kind == IpKind::SNat {
                    None
                } else {
                    Some(ip.try_into().unwrap())
                }
            })
            .collect::<Vec<_>>())
    }
}
