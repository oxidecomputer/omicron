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
use crate::db::queries;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

/// Type returned when reassigning producers from a (presumably defunct)
/// Oximeter collector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectorReassignment {
    /// Success: `n` producers were reassigned to other collector(s).
    Complete(usize),
    /// Reassignment could not complete because there are no other collectors
    /// available.
    NoCollectorsAvailable,
}

impl DataStore {
    /// Lookup an oximeter instance by its ID.
    pub async fn oximeter_lookup(
        &self,
        opctx: &OpContext,
        id: &Uuid,
    ) -> Result<OximeterInfo, Error> {
        use db::schema::oximeter::dsl;
        dsl::oximeter
            .filter(dsl::time_deleted.is_null())
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

    /// Mark an Oximeter instance as deleted
    ///
    /// This method is idempotent and has no effect if called with the ID for an
    /// already-deleted Oximeter.
    pub async fn oximeter_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::oximeter::dsl;

        let now = Utc::now();

        diesel::update(dsl::oximeter)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(id))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

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
            .filter(dsl::time_deleted.is_null())
            .load_async::<OximeterInfo>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Reassign all metric producers currently assigned to Oximeter `id`
    ///
    /// The new Oximeter instance for each producer will be randomly selected
    /// from all available Oximeters. On success, returns the number of metric
    /// producers reassigned. Fails if there are no available Oximeter instances
    /// (e.g., all Oximeter instances have been deleted).
    pub async fn oximeter_reassign_all_producers(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<CollectorReassignment, Error> {
        match queries::oximeter::reassign_producers_query(id)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
        {
            Ok(n) => Ok(CollectorReassignment::Complete(n)),
            Err(DieselError::DatabaseError(
                DatabaseErrorKind::NotNullViolation,
                _,
            )) => Ok(CollectorReassignment::NoCollectorsAvailable),
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
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
    async fn test_oximeter_delete() {
        // Setup
        let logctx = dev::test_setup_log("test_oximeter_delete");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            datastore_test(&logctx, &db, Uuid::new_v4()).await;

        // Insert a few Oximeter collectors.
        let mut collector_ids =
            (0..4).map(|_| Uuid::new_v4()).collect::<Vec<_>>();

        // Sort the IDs for easier comparisons later.
        collector_ids.sort();

        for &collector_id in &collector_ids {
            let info = OximeterInfo::new(&params::OximeterInfo {
                collector_id,
                address: "[::1]:0".parse().unwrap(), // unused
            });
            datastore
                .oximeter_create(&opctx, &info)
                .await
                .expect("inserted collector");
        }

        // Ensure all our collectors exist and aren't deleted.
        let mut all_collectors = datastore
            .oximeter_list(&opctx, &DataPageParams::max_page())
            .await
            .expect("listed collectors");
        all_collectors.sort_by_key(|info| info.id);
        assert_eq!(all_collectors.len(), collector_ids.len());
        for (info, &expected_id) in all_collectors.iter().zip(&collector_ids) {
            assert_eq!(info.id, expected_id);
            assert!(info.time_deleted.is_none());
        }

        // Delete the first two of them.
        datastore
            .oximeter_delete(&opctx, collector_ids[0])
            .await
            .expect("deleted collector");
        datastore
            .oximeter_delete(&opctx, collector_ids[1])
            .await
            .expect("deleted collector");

        // Ensure those two were deleted.
        let mut all_collectors = datastore
            .oximeter_list(&opctx, &DataPageParams::max_page())
            .await
            .expect("listed collectors");
        all_collectors.sort_by_key(|info| info.id);
        assert_eq!(all_collectors.len(), collector_ids.len() - 2);
        for (info, &expected_id) in
            all_collectors.iter().zip(&collector_ids[2..])
        {
            assert_eq!(info.id, expected_id);
            assert!(info.time_deleted.is_none());
        }

        // Deletion is idempotent. To test, we'll read the deleted rows
        // directly, delete them again, and confirm the row contents haven't
        // changed.
        let find_oximeter_ignoring_deleted = |id| {
            let datastore = &datastore;
            let opctx = &opctx;
            async move {
                let conn = datastore
                    .pool_connection_authorized(opctx)
                    .await
                    .expect("acquired connection");
                use db::schema::oximeter::dsl;
                let info: OximeterInfo = dsl::oximeter
                    .find(id)
                    .first_async(&*conn)
                    .await
                    .expect("found Oximeter by ID");
                info
            }
        };
        let deleted0a = find_oximeter_ignoring_deleted(collector_ids[0]).await;
        let deleted1a = find_oximeter_ignoring_deleted(collector_ids[1]).await;
        assert!(deleted0a.time_deleted.is_some());
        assert!(deleted1a.time_deleted.is_some());

        datastore
            .oximeter_delete(&opctx, collector_ids[0])
            .await
            .expect("deleted collector");
        datastore
            .oximeter_delete(&opctx, collector_ids[1])
            .await
            .expect("deleted collector");

        let deleted0b = find_oximeter_ignoring_deleted(collector_ids[0]).await;
        let deleted1b = find_oximeter_ignoring_deleted(collector_ids[1]).await;
        assert_eq!(deleted0a, deleted0b);
        assert_eq!(deleted1a, deleted1b);

        // Cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_oximeter_reassigns_randomly() {
        // Setup
        let logctx = dev::test_setup_log("test_oximeter_reassigns_randomly");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            datastore_test(&logctx, &db, Uuid::new_v4()).await;

        // Insert a few Oximeter collectors.
        let collector_ids = (0..4).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        for &collector_id in &collector_ids {
            let info = OximeterInfo::new(&params::OximeterInfo {
                collector_id,
                address: "[::1]:0".parse().unwrap(), // unused
            });
            datastore
                .oximeter_create(&opctx, &info)
                .await
                .expect("inserted collector");
        }

        // Insert 250 metric producers assigned to each collector.
        for &collector_id in &collector_ids {
            for _ in 0..250 {
                let producer = ProducerEndpoint::new(
                    &nexus::ProducerEndpoint {
                        id: Uuid::new_v4(),
                        kind: nexus::ProducerKind::Service,
                        address: "[::1]:0".parse().unwrap(), // unused
                        interval: Duration::from_secs(0),    // unused
                    },
                    collector_id,
                );
                datastore
                    .producer_endpoint_create(&opctx, &producer)
                    .await
                    .expect("created producer");
            }
        }

        // Delete one collector.
        datastore
            .oximeter_delete(&opctx, collector_ids[0])
            .await
            .expect("deleted Oximeter");

        // Reassign producers that belonged to that collector.
        let num_reassigned = datastore
            .oximeter_reassign_all_producers(&opctx, collector_ids[0])
            .await
            .expect("reassigned producers");
        assert_eq!(num_reassigned, CollectorReassignment::Complete(250));

        // Check the distribution of producers for each of the remaining
        // collectors. We don't know the exact count, so we'll check that:
        //
        // * Each of the three remaining collectors gained at least one (the
        //   probability that any of the three collectors gained zero is low
        //   enough that most calculators give up and call it 0)
        // * All 1000 producers are assigned to one of the three collectors
        //
        // to guard against "the reassignment query gave all 250 to exactly one
        // of the remaining collectors", which is an easy failure mode for this
        // kind of SQL query, where the query engine only evaluates the
        // randomness once instead of once for each producer.
        let mut producer_counts = [0; 4];
        for i in 0..4 {
            producer_counts[i] = datastore
                .producers_list_by_oximeter_id(
                    &opctx,
                    collector_ids[i],
                    &DataPageParams::max_page(),
                )
                .await
                .expect("listed producers")
                .len();
        }
        assert_eq!(producer_counts[0], 0); // all reassigned
        assert!(producer_counts[1] > 250); // gained at least one
        assert!(producer_counts[2] > 250); // gained at least one
        assert!(producer_counts[3] > 250); // gained at least one
        assert_eq!(producer_counts[1..].iter().sum::<usize>(), 1000);

        // Cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_oximeter_reassign_fails_if_no_collectors() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_oximeter_reassign_fails_if_no_collectors",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            datastore_test(&logctx, &db, Uuid::new_v4()).await;

        // Insert a few Oximeter collectors.
        let collector_ids = (0..4).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        for &collector_id in &collector_ids {
            let info = OximeterInfo::new(&params::OximeterInfo {
                collector_id,
                address: "[::1]:0".parse().unwrap(), // unused
            });
            datastore
                .oximeter_create(&opctx, &info)
                .await
                .expect("inserted collector");
        }

        // Insert 10 metric producers assigned to each collector.
        for &collector_id in &collector_ids {
            for _ in 0..10 {
                let producer = ProducerEndpoint::new(
                    &nexus::ProducerEndpoint {
                        id: Uuid::new_v4(),
                        kind: nexus::ProducerKind::Service,
                        address: "[::1]:0".parse().unwrap(), // unused
                        interval: Duration::from_secs(0),    // unused
                    },
                    collector_id,
                );
                datastore
                    .producer_endpoint_create(&opctx, &producer)
                    .await
                    .expect("created producer");
            }
        }

        // Delete all four collectors.
        for &collector_id in &collector_ids {
            datastore
                .oximeter_delete(&opctx, collector_id)
                .await
                .expect("deleted Oximeter");
        }

        // Try to reassign producers that belonged to each collector; this
        // should fail, as all collectors have been deleted.
        for &collector_id in &collector_ids {
            let num_reassigned = datastore
                .oximeter_reassign_all_producers(&opctx, collector_id)
                .await
                .expect("reassigned producers");
            assert_eq!(
                num_reassigned,
                CollectorReassignment::NoCollectorsAvailable
            );
        }

        // Now insert a new collector.
        let new_collector_id = Uuid::new_v4();
        datastore
            .oximeter_create(
                &opctx,
                &OximeterInfo::new(&params::OximeterInfo {
                    collector_id: new_collector_id,
                    address: "[::1]:0".parse().unwrap(), // unused
                }),
            )
            .await
            .expect("inserted collector");

        // Reassigning the original four collectors should now all succeed.
        for &collector_id in &collector_ids {
            let num_reassigned = datastore
                .oximeter_reassign_all_producers(&opctx, collector_id)
                .await
                .expect("reassigned producers");
            assert_eq!(num_reassigned, CollectorReassignment::Complete(10));
        }

        // All 40 producers should be assigned to our new collector.
        let nproducers = datastore
            .producers_list_by_oximeter_id(
                &opctx,
                new_collector_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("listed producers")
            .len();
        assert_eq!(nproducers, 40);

        // Cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
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
