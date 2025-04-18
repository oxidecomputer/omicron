// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_auth::authz;
use nexus_auth::context::OpContext;
use nexus_saga_interface::DataStoreImpl;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use uuid::Uuid;

use super::DataStore;

#[async_trait::async_trait]
impl DataStoreImpl for DataStore {
    async fn project_delete_instance(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult {
        self.project_delete_instance(opctx, authz_instance).await
    }

    async fn instance_delete_all_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult {
        self.instance_delete_all_network_interfaces(opctx, authz_instance).await
    }

    async fn deallocate_external_ip_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        self.deallocate_external_ip_by_instance_id(opctx, instance_id).await
    }

    async fn detach_floating_ips_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        self.detach_floating_ips_by_instance_id(opctx, instance_id).await
    }
}
