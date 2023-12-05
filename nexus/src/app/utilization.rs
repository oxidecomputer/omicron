// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Insights into capacity and utilization


impl super::Nexus {
   pub async fn fleet_utilization_view(
       &self,
       opctx: &OpContext,
   ) -> Result<db::model::FleetUtilization, Error> {
       opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
       self.db_datastore.fleet_utilization_view(opctx).await
   }

   pub async fn silo_utilization_view(
       &self,
       opctx: &OpContext,
       silo_lookup: &lookup::Silo<'_>,
   ) -> Result<db::model::SiloUtilization, Error> {
       let (.., authz_silo) =
           silo_lookup.lookup_for(authz::Action::Read).await?;
       self.db_datastore.silo_utilization_view(opctx, &authz_silo).await
   }

   pub async fn silo_utilization_list(
       &self,
       opctx: &OpContext,
       pagparams: &DataPageParams<'_, Uuid>,
   ) -> ListResultVec<db::model::SiloUtilization> {
       opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
       self.db_datastore.silo_utilization_list(opctx, pagparams).await
   }

}