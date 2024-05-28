// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to Oximeter.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::OximeterInfo;
use crate::db::model::ProducerEndpoint;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Lookup an oximeter instance by its ID.
    pub async fn oximeter_lookup(
        &self,
        opctx: &OpContext,
        id: &Uuid,
    ) -> Result<OximeterInfo, Error> {
        use db::schema::oximeter::dsl;
        dsl::oximeter
            .find(*id)
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Create a record for a new Oximeter instance
    pub async fn oximeter_create(
        &self,
        opctx: &OpContext,
        info: &OximeterInfo,
    ) -> Result<(), Error> {
        use db::schema::oximeter::dsl;

        // If we get a conflict on the Oximeter ID, this means that collector instance was
        // previously registered, and it's re-registering due to something like a service restart.
        // In this case, we update the time modified and the service address, rather than
        // propagating a constraint violation to the caller.
        diesel::insert_into(dsl::oximeter)
            .values(*info)
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::ip.eq(info.ip),
                dsl::port.eq(info.port),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Oximeter,
                        "Oximeter Info",
                    ),
                )
            })?;
        Ok(())
    }

    /// List the oximeter collector instances
    pub async fn oximeter_list(
        &self,
        opctx: &OpContext,
        page_params: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<OximeterInfo> {
        use db::schema::oximeter::dsl;
        paginated(dsl::oximeter, dsl::id, page_params)
            .load_async::<OximeterInfo>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Create a record for a new producer endpoint
    pub async fn producer_endpoint_create(
        &self,
        opctx: &OpContext,
        producer: &ProducerEndpoint,
    ) -> Result<(), Error> {
        use db::schema::metric_producer::dsl;

        // TODO: see https://github.com/oxidecomputer/omicron/issues/323
        diesel::insert_into(dsl::metric_producer)
            .values(producer.clone())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::kind.eq(producer.kind),
                dsl::ip.eq(producer.ip),
                dsl::port.eq(producer.port),
                dsl::interval.eq(producer.interval),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::MetricProducer,
                        "Producer Endpoint",
                    ),
                )
            })?;
        Ok(())
    }

    /// Delete a record for a producer endpoint, by its ID.
    ///
    /// This is idempotent, and deleting a record that is already removed is a
    /// no-op. If the record existed, then the ID of the `oximeter` collector is
    /// returned. If there was no record, `None` is returned.
    pub async fn producer_endpoint_delete(
        &self,
        opctx: &OpContext,
        id: &Uuid,
    ) -> Result<Option<Uuid>, Error> {
        use db::schema::metric_producer::dsl;
        diesel::delete(dsl::metric_producer.find(*id))
            .returning(dsl::oximeter_id)
            .get_result_async::<Uuid>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List the producer endpoint records by the oximeter instance to which they're assigned.
    pub async fn producers_list_by_oximeter_id(
        &self,
        opctx: &OpContext,
        oximeter_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ProducerEndpoint> {
        use db::schema::metric_producer::dsl;
        paginated(dsl::metric_producer, dsl::id, &pagparams)
            .filter(dsl::oximeter_id.eq(oximeter_id))
            .order_by((dsl::oximeter_id, dsl::id))
            .select(ProducerEndpoint::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::MetricProducer,
                        "By Oximeter ID",
                    ),
                )
            })
    }

    /// Fetches a page of the list of producer endpoint records with a
    /// `time_modified` date older than `expiration`
    pub async fn producers_list_expired(
        &self,
        opctx: &OpContext,
        expiration: DateTime<Utc>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ProducerEndpoint> {
        use db::schema::metric_producer::dsl;

        paginated(dsl::metric_producer, dsl::id, pagparams)
            .filter(dsl::time_modified.lt(expiration))
            .order_by((dsl::oximeter_id, dsl::id))
            .select(ProducerEndpoint::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all producer endpoint records with a `time_modified` date older
    /// than `expiration`, making as many queries as needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn producers_list_expired_batched(
        &self,
        opctx: &OpContext,
        expiration: DateTime<Utc>,
    ) -> ListResultVec<ProducerEndpoint> {
        opctx.check_complex_operations_allowed()?;

        let mut producers = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = self
                .producers_list_expired(
                    opctx,
                    expiration,
                    &p.current_pagparams(),
                )
                .await?;
            paginator = p.found_batch(&batch, &|p: &ProducerEndpoint| p.id());
            producers.extend(batch);
        }

        Ok(producers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::datastore::pub_test_utils::datastore_test;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::internal_api::params;
    use omicron_common::api::internal::nexus;
    use omicron_test_utils::dev;
    use std::time::Duration;

    async fn read_time_modified(
        datastore: &DataStore,
        producer_id: Uuid,
    ) -> DateTime<Utc> {
        use db::schema::metric_producer::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        match dsl::metric_producer
            .filter(dsl::id.eq(producer_id))
            .select(dsl::time_modified)
            .first_async(&*conn)
            .await
        {
            Ok(time_modified) => time_modified,
            Err(err) => panic!(
                "failed to read time_modified for producer {producer_id}: \
                {err}"
            ),
        }
    }

    async fn read_expired_producers(
        opctx: &OpContext,
        datastore: &DataStore,
        expiration: DateTime<Utc>,
    ) -> Vec<ProducerEndpoint> {
        let expired_one_page = datastore
            .producers_list_expired(
                opctx,
                expiration,
                &DataPageParams::max_page(),
            )
            .await
            .expect("failed to read max_page of expired producers");
        let expired_batched = datastore
            .producers_list_expired_batched(opctx, expiration)
            .await
            .expect("failed to read batched expired producers");
        assert_eq!(expired_one_page, expired_batched);
        expired_batched
    }

    #[tokio::test]
    async fn test_producers_list_expired() {
        // Setup
        let logctx = dev::test_setup_log("test_producers_list_expired");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            datastore_test(&logctx, &db, Uuid::new_v4()).await;

        // Insert an Oximeter collector
        let collector_info = OximeterInfo::new(&params::OximeterInfo {
            collector_id: Uuid::new_v4(),
            address: "[::1]:0".parse().unwrap(), // unused
        });
        datastore
            .oximeter_create(&opctx, &collector_info)
            .await
            .expect("failed to insert collector");

        // Insert a producer
        let producer = ProducerEndpoint::new(
            &nexus::ProducerEndpoint {
                id: Uuid::new_v4(),
                kind: nexus::ProducerKind::Service,
                address: "[::1]:0".parse().unwrap(), // unused
                interval: Duration::from_secs(0),    // unused
            },
            collector_info.id,
        );
        datastore
            .producer_endpoint_create(&opctx, &producer)
            .await
            .expect("failed to insert producer");

        // Our producer should show up when we list by its collector
        let mut all_producers = datastore
            .producers_list_by_oximeter_id(
                &opctx,
                collector_info.id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("failed to list all producers");
        assert_eq!(all_producers.len(), 1);
        assert_eq!(all_producers[0].id(), producer.id());

        // Steal this producer so we have a database-precision timestamp and can
        // use full equality checks moving forward.
        let producer = all_producers.pop().unwrap();

        let producer_time_modified =
            read_time_modified(&datastore, producer.id()).await;

        // Whether it's expired depends on the expiration date we specify; it
        // should show up if the expiration time is newer than the producer's
        // time_modified...
        let expired_producers = read_expired_producers(
            &opctx,
            &datastore,
            producer_time_modified + Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            expired_producers.as_slice(),
            std::slice::from_ref(&producer)
        );

        // ... but not if the the producer has been modified since the
        // expiration.
        let expired_producers = read_expired_producers(
            &opctx,
            &datastore,
            producer_time_modified - Duration::from_secs(1),
        )
        .await;
        assert_eq!(expired_producers.as_slice(), &[]);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
