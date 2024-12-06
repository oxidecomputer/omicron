// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Access to Support Bundles

use dropshot::Body;
use futures::TryStreamExt;
use http::Response;
use nexus_db_model::SupportBundle;
use nexus_db_model::SupportBundleState;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::SupportBundleGetQueryParams;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use range_requests::PotentialRange;
use uuid::Uuid;

impl super::Nexus {
    pub async fn support_bundle_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SupportBundle> {
        self.db_datastore.support_bundle_list(&opctx, pagparams).await
    }

    pub async fn support_bundle_view(
        &self,
        opctx: &OpContext,
        id: SupportBundleUuid,
    ) -> LookupResult<SupportBundle> {
        self.db_datastore.support_bundle_get(&opctx, id).await
    }

    pub async fn support_bundle_create(
        &self,
        opctx: &OpContext,
        reason: &'static str,
    ) -> CreateResult<SupportBundle> {
        self.db_datastore.support_bundle_create(&opctx, reason, self.id).await
    }

    pub async fn support_bundle_download(
        &self,
        opctx: &OpContext,
        id: SupportBundleUuid,
        query: &SupportBundleGetQueryParams,
        head: bool,
        _range: Option<PotentialRange>,
    ) -> Result<Response<Body>, Error> {
        // Lookup the bundle, confirm it's accessible
        let bundle = self.db_datastore.support_bundle_get(&opctx, id).await?;
        if !matches!(bundle.state, SupportBundleState::Active) {
            return Err(Error::invalid_request(
                "Cannot download bundle in non-active state",
            ));
        }

        // Lookup the sled holding the bundle, forward the request there
        let sled_id = self
            .db_datastore
            .zpool_get_sled(&opctx, bundle.zpool_id.into())
            .await?;
        let client = self.sled_client(&sled_id).await?;

        // TODO: Use "range"?

        let response = if head {
            client
                .support_bundle_head(
                    &ZpoolUuid::from(bundle.zpool_id),
                    &DatasetUuid::from(bundle.dataset_id),
                    &SupportBundleUuid::from(bundle.id),
                    &query,
                )
                .await
        } else {
            client
                .support_bundle_get(
                    &ZpoolUuid::from(bundle.zpool_id),
                    &DatasetUuid::from(bundle.dataset_id),
                    &SupportBundleUuid::from(bundle.id),
                    &query,
                )
                .await
        };

        let response =
            response.map_err(|err| Error::internal_error(&err.to_string()))?;

        // The result from the sled agent a "ResponseValue<ByteStream>", but we
        // need to coerce that type into a "Response<Body>" while preserving the
        // status, headers, and body.
        let mut builder = Response::builder().status(response.status());
        let headers = builder.headers_mut().unwrap();
        headers.extend(
            response.headers().iter().map(|(k, v)| (k.clone(), v.clone())),
        );
        let body = http_body_util::StreamBody::new(
            response
                .into_inner_stream()
                .map_ok(|b| hyper::body::Frame::data(b)),
        );
        Ok(builder.body(Body::wrap(body)).unwrap())
    }

    pub async fn support_bundle_delete(
        &self,
        opctx: &OpContext,
        id: SupportBundleUuid,
    ) -> DeleteResult {
        // NOTE: We can't necessarily delete the support bundle
        // immediately - it might have state that needs cleanup
        // by a background task - so, instead, we mark it deleting.
        //
        // This is a terminal state
        self.db_datastore
            .support_bundle_update(&opctx, id, SupportBundleState::Destroying)
            .await?;
        Ok(())
    }
}
