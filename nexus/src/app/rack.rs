// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack management

use crate::authz;
use crate::context::OpContext;
use crate::db;
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
}
