// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack management

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::internal_api::params::ServicePutRequest;
use futures::future::ready;
use futures::StreamExt;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResult;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl super::Nexus {
    pub(crate) fn as_rack(&self) -> db::model::Rack {
        db::model::Rack {
            identity: self.api_rack_identity.clone(),
            initialized: true,
            tuf_base_url: None,
        }
    }

    pub async fn racks_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<db::model::Rack> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        if let Some(marker) = pagparams.marker {
            if *marker >= self.rack_id {
                return Ok(futures::stream::empty().boxed());
            }
        }

        Ok(futures::stream::once(ready(Ok(self.as_rack()))).boxed())
    }

    pub async fn rack_lookup(
        &self,
        opctx: &OpContext,
        rack_id: &Uuid,
    ) -> LookupResult<db::model::Rack> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            *rack_id,
            LookupType::ById(*rack_id),
        );
        opctx.authorize(authz::Action::Read, &authz_rack).await?;

        if *rack_id == self.rack_id {
            Ok(self.as_rack())
        } else {
            Err(Error::not_found_by_id(ResourceType::Rack, rack_id))
        }
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

        let (.., db_rack) = LookupPath::new(opctx, &self.db_datastore)
            .rack_id(rack_id)
            .fetch()
            .await?;

        // TODO-concurrency: Technically, this initialization is not atomic.
        // We check if the rack is initialized, upsert each service, then
        // set the rack to be initialized.
        //
        // However, this should practically be safe for a few reasons:
        //
        // 1. There should only be a single RSS process executing at once.
        // 2. Once the RSS propagates information about services to Nexus,
        // they should be recorded to a local plan that does not change.
        // 3. Once the rack is initialized, it cannot be uninitialized.
        //
        // If any of these invariants change, this sequence of operations
        // should be moved into a transaction.
        if db_rack.initialized {
            return Ok(());
        }

        for svc in &services {
            self.upsert_service(
                opctx,
                svc.service_id,
                svc.sled_id,
                svc.address,
                svc.kind.into(),
            )
            .await?;
        }

        self.db_datastore
            .rack_set_initialized(
                opctx,
                rack_id,
            ).await?;

        Ok(())
    }
}
