// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack management

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::internal_api::params::ServicePutRequest;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
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
        services: Vec<ServicePutRequest>,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // Convert from parameter -> DB type.
        let services: Vec<_> = services
            .into_iter()
            .map(|svc| {
                db::model::Service::new(
                    svc.service_id,
                    svc.sled_id,
                    svc.address,
                    svc.kind.into(),
                )
            })
            .collect();

        // TODO: Actually supply datasets provided from the sled agent.
        self.db_datastore
            .rack_set_initialized(opctx, rack_id, services, vec![])
            .await?;

        Ok(())
    }
}
