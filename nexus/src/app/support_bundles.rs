// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Access to Support Bundles

use dropshot::Body;
use futures::TryStreamExt;
use http::Response;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SupportBundle;
use nexus_db_model::SupportBundleState;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;
use range_requests::PotentialRange;
use uuid::Uuid;

/// Describes the type of access to the support bundle
#[derive(Clone, Debug)]
pub enum SupportBundleQueryType {
    /// Access the whole support bundle
    Whole,
    /// Access the names of all files within the support bundle
    Index,
    /// Access a specific file within the support bundle
    Path { file_path: String },
}

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
        let (.., db_bundle) = LookupPath::new(opctx, &self.db_datastore)
            .support_bundle(id)
            .fetch()
            .await?;

        Ok(db_bundle)
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
        query: SupportBundleQueryType,
        head: bool,
        range: Option<PotentialRange>,
    ) -> Result<Response<Body>, Error> {
        // Lookup the bundle, confirm it's accessible
        let (.., bundle) = LookupPath::new(opctx, &self.db_datastore)
            .support_bundle(id)
            .fetch()
            .await?;

        if !matches!(bundle.state, SupportBundleState::Active) {
            return Err(Error::invalid_request(
                "Cannot download bundle in non-active state",
            ));
        }

        // Lookup the sled holding the bundle, forward the request there
        let sled_id = self
            .db_datastore
            .zpool_get_sled_if_in_service(&opctx, bundle.zpool_id.into())
            .await?;

        let short_timeout = std::time::Duration::from_secs(60);
        let long_timeout = std::time::Duration::from_secs(3600);
        let client = nexus_networking::default_reqwest_client_builder()
            // Continuing to read from the sled agent should happen relatively
            // quickly.
            .read_timeout(short_timeout)
            // However, the bundle itself may be large. As long as we're
            // continuing to make progress (see: read_timeout) we should be
            // willing to keep transferring the bundle for a while longer.
            .timeout(long_timeout)
            .build()
            .expect("Failed to build reqwest Client");
        let client = self.sled_client_ext(&sled_id, client).await?;

        let range = if let Some(potential_range) = &range {
            Some(potential_range.try_into_str().map_err(|err| match err {
                range_requests::Error::Parse(_) => Error::invalid_request(
                    "Failed to parse range request header",
                ),
                _ => Error::internal_error("Invalid range request"),
            })?)
        } else {
            None
        };

        let response = match (query, head) {
            (SupportBundleQueryType::Whole, true) => {
                client
                    .support_bundle_head(
                        &ZpoolUuid::from(bundle.zpool_id),
                        &DatasetUuid::from(bundle.dataset_id),
                        &SupportBundleUuid::from(bundle.id),
                    )
                    .await
            }
            (SupportBundleQueryType::Whole, false) => {
                client
                    .support_bundle_download(
                        &ZpoolUuid::from(bundle.zpool_id),
                        &DatasetUuid::from(bundle.dataset_id),
                        &SupportBundleUuid::from(bundle.id),
                        range,
                    )
                    .await
            }
            (SupportBundleQueryType::Index, true) => {
                client
                    .support_bundle_head_index(
                        &ZpoolUuid::from(bundle.zpool_id),
                        &DatasetUuid::from(bundle.dataset_id),
                        &SupportBundleUuid::from(bundle.id),
                    )
                    .await
            }
            (SupportBundleQueryType::Index, false) => {
                client
                    .support_bundle_index(
                        &ZpoolUuid::from(bundle.zpool_id),
                        &DatasetUuid::from(bundle.dataset_id),
                        &SupportBundleUuid::from(bundle.id),
                    )
                    .await
            }
            (SupportBundleQueryType::Path { file_path }, true) => {
                client
                    .support_bundle_head_file(
                        &ZpoolUuid::from(bundle.zpool_id),
                        &DatasetUuid::from(bundle.dataset_id),
                        &SupportBundleUuid::from(bundle.id),
                        &file_path,
                    )
                    .await
            }
            (SupportBundleQueryType::Path { file_path }, false) => {
                client
                    .support_bundle_download_file(
                        &ZpoolUuid::from(bundle.zpool_id),
                        &DatasetUuid::from(bundle.dataset_id),
                        &SupportBundleUuid::from(bundle.id),
                        &file_path,
                    )
                    .await
            }
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
        let (authz_bundle, ..) = LookupPath::new(opctx, &self.db_datastore)
            .support_bundle(id)
            .lookup_for(authz::Action::Delete)
            .await?;

        // NOTE: We can't necessarily delete the support bundle
        // immediately - it might have state that needs cleanup
        // by a background task - so, instead, we mark it deleting.
        //
        // This is a terminal state
        self.db_datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Destroying,
            )
            .await?;
        Ok(())
    }
}
