// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to Oximeter.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::context::OpContext;
use crate::db::identity::Asset;
use crate::db::model::OximeterInfo;
use crate::db::model::ProducerEndpoint;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::internal;
use uuid::Uuid;

/// Type returned when reassigning producers from an Oximeter collector.
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
    ///
    /// Fails if the instance has been expunged.
    pub async fn oximeter_lookup(
        &self,
        opctx: &OpContext,
        id: &Uuid,
    ) -> Result<OximeterInfo, Error> {
        use nexus_db_schema::schema::oximeter::dsl;
        dsl::oximeter
            .filter(dsl::time_expunged.is_null())
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
        use nexus_db_schema::schema::oximeter::dsl;

        // If we get a conflict on the Oximeter ID, this means that collector
        // instance was previously registered, and it's re-registering due to
        // something like a service restart. In this case, we update the time
        // modified and the service address, rather than propagating a
        // constraint violation to the caller.
        //
        // TODO-completeness - We should return an error if `info.id()` maps to
        // an existing row that has been expunged. We don't expect that to
        // happen in practice (it would mean an expunged Oximeter zone has come
        // back to life and reregistered itself). If it does happen, as written
        // we'll update time_modified/ip/port but leave time_expunged set to
        // whatever it was (which will leave the Oximeter in the "expunged"
        // state).
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

    /// Mark an Oximeter instance as expunged
    ///
    /// This method is idempotent and has no effect if called with the ID for an
    /// already-expunged Oximeter.
    pub async fn oximeter_expunge(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::oximeter::dsl;

        let now = Utc::now();

        diesel::update(dsl::oximeter)
            .filter(dsl::time_expunged.is_null())
            .filter(dsl::id.eq(id))
            .set(dsl::time_expunged.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// List the oximeter collector instances
    ///
    /// Omits expunged instances.
    pub async fn oximeter_list(
        &self,
        opctx: &OpContext,
        page_params: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<OximeterInfo> {
        use nexus_db_schema::schema::oximeter::dsl;
        paginated(dsl::oximeter, dsl::id, page_params)
            .filter(dsl::time_expunged.is_null())
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
    /// (e.g., all Oximeter instances have been expunged).
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

    /// Create or update a record for a producer endpoint
    ///
    /// If the endpoint is being created, a randomly-chosen Oximeter instance
    /// will be assigned. If the endpoint is being updated, it will keep its
    /// existing Oximeter assignment.
    ///
    /// Returns the oximeter ID assigned to this producer (either the
    /// randomly-chosen one, if newly inserted, or the previously-chosen, if
    /// updated).
    pub async fn producer_endpoint_upsert_and_assign(
        &self,
        opctx: &OpContext,
        producer: &internal::nexus::ProducerEndpoint,
    ) -> Result<OximeterInfo, Error> {
        match queries::oximeter::upsert_producer(producer)
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
        {
            Ok(info) => Ok(info),
            Err(DieselError::NotFound) => Err(Error::unavail(
                "no Oximeter instances available for assignment",
            )),
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
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
        use nexus_db_schema::schema::metric_producer::dsl;
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
        use nexus_db_schema::schema::metric_producer::dsl;
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
        use nexus_db_schema::schema::metric_producer::dsl;

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
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
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
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_types::internal_api::params;
    use omicron_common::api::internal::nexus;
    use omicron_test_utils::dev;
    use std::time::Duration;

    async fn read_time_modified(
        datastore: &DataStore,
        producer_id: Uuid,
    ) -> DateTime<Utc> {
        use nexus_db_schema::schema::metric_producer::dsl;

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
    async fn test_oximeter_expunge() {
        // Setup
        let logctx = dev::test_setup_log("test_oximeter_expunge");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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

        // Ensure all our collectors exist and aren't expunged.
        let mut all_collectors = datastore
            .oximeter_list(&opctx, &DataPageParams::max_page())
            .await
            .expect("listed collectors");
        all_collectors.sort_by_key(|info| info.id);
        assert_eq!(all_collectors.len(), collector_ids.len());
        for (info, &expected_id) in all_collectors.iter().zip(&collector_ids) {
            assert_eq!(info.id, expected_id);
            assert!(info.time_expunged.is_none());
        }

        // Delete the first two of them.
        datastore
            .oximeter_expunge(&opctx, collector_ids[0])
            .await
            .expect("expunged collector");
        datastore
            .oximeter_expunge(&opctx, collector_ids[1])
            .await
            .expect("expunged collector");

        // Ensure those two were expunged.
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
            assert!(info.time_expunged.is_none());
        }

        // Deletion is idempotent. To test, we'll read the expunged rows
        // directly, expunge them again, and confirm the row contents haven't
        // changed.
        let find_oximeter_ignoring_expunged = |id| {
            let datastore = &datastore;
            let opctx = &opctx;
            async move {
                let conn = datastore
                    .pool_connection_authorized(opctx)
                    .await
                    .expect("acquired connection");
                use nexus_db_schema::schema::oximeter::dsl;
                let info: OximeterInfo = dsl::oximeter
                    .find(id)
                    .first_async(&*conn)
                    .await
                    .expect("found Oximeter by ID");
                info
            }
        };
        let expunged0a =
            find_oximeter_ignoring_expunged(collector_ids[0]).await;
        let expunged1a =
            find_oximeter_ignoring_expunged(collector_ids[1]).await;
        assert!(expunged0a.time_expunged.is_some());
        assert!(expunged1a.time_expunged.is_some());

        datastore
            .oximeter_expunge(&opctx, collector_ids[0])
            .await
            .expect("expunged collector");
        datastore
            .oximeter_expunge(&opctx, collector_ids[1])
            .await
            .expect("expunged collector");

        let expunged0b =
            find_oximeter_ignoring_expunged(collector_ids[0]).await;
        let expunged1b =
            find_oximeter_ignoring_expunged(collector_ids[1]).await;
        assert_eq!(expunged0a, expunged0b);
        assert_eq!(expunged1a, expunged1b);

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_producer_endpoint_reassigns_if_oximeter_expunged() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_producer_endpoint_reassigns_if_oximeter_expunged",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Insert an Oximeter collector.
        let oximeter1_id = Uuid::new_v4();
        datastore
            .oximeter_create(
                &opctx,
                &OximeterInfo::new(&params::OximeterInfo {
                    collector_id: oximeter1_id,
                    address: "[::1]:0".parse().unwrap(), // unused
                }),
            )
            .await
            .expect("inserted collector");

        // Insert a producer.
        let producer = nexus::ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: nexus::ProducerKind::Service,
            address: "[::1]:0".parse().unwrap(),
            interval: Duration::from_secs(0),
        };
        let chosen_oximeter = datastore
            .producer_endpoint_upsert_and_assign(&opctx, &producer)
            .await
            .expect("inserted producer");
        assert_eq!(chosen_oximeter.id, oximeter1_id);

        // Grab the inserted producer (so we have its time_modified for checks
        // below).
        let producer_info = datastore
            .producers_list_by_oximeter_id(
                &opctx,
                oximeter1_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("listed producers")
            .pop()
            .expect("got producer");
        assert_eq!(producer_info.id(), producer.id);

        // Expunge the oximeter.
        datastore
            .oximeter_expunge(&opctx, oximeter1_id)
            .await
            .expect("expunged oximeter");

        // Attempting to upsert our producer again should fail; our oximeter has
        // been expunged, and our time modified should be unchanged.
        let err = datastore
            .producer_endpoint_upsert_and_assign(&opctx, &producer)
            .await
            .expect_err("producer upsert failed")
            .to_string();
        assert!(
            err.contains("no Oximeter instances available for assignment"),
            "unexpected error: {err}"
        );
        {
            let check_info = datastore
                .producers_list_by_oximeter_id(
                    &opctx,
                    oximeter1_id,
                    &DataPageParams::max_page(),
                )
                .await
                .expect("listed producers")
                .pop()
                .expect("got producer");
            assert_eq!(
                producer_info, check_info,
                "unexpected modification in failed upsert"
            );
        }

        // Add a new, non-expunged Oximeter.
        let oximeter2_id = Uuid::new_v4();
        datastore
            .oximeter_create(
                &opctx,
                &OximeterInfo::new(&params::OximeterInfo {
                    collector_id: oximeter2_id,
                    address: "[::1]:0".parse().unwrap(), // unused
                }),
            )
            .await
            .expect("inserted collector");

        // Retry updating our existing producer; it should get reassigned to a
        // the new Oximeter.
        let chosen_oximeter = datastore
            .producer_endpoint_upsert_and_assign(&opctx, &producer)
            .await
            .expect("inserted producer");
        assert_eq!(chosen_oximeter.id, oximeter2_id);
        {
            let check_info = datastore
                .producers_list_by_oximeter_id(
                    &opctx,
                    oximeter2_id,
                    &DataPageParams::max_page(),
                )
                .await
                .expect("listed producers")
                .pop()
                .expect("got producer");
            assert_eq!(check_info.id(), producer_info.id());
            assert!(
                check_info.time_modified() > producer_info.time_modified(),
                "producer time modified was not advanced"
            );
        }

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_producer_endpoint_upsert_rejects_expunged_oximeters() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_producer_endpoint_upsert_rejects_expunged_oximeters",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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

        // Creating a producer randomly chooses one of our collectors. Create
        // 1000 and check that we saw each collector at least once.
        let mut seen_collector_counts = vec![0; collector_ids.len()];
        for _ in 0..1000 {
            let producer = nexus::ProducerEndpoint {
                id: Uuid::new_v4(),
                kind: nexus::ProducerKind::Service,
                address: "[::1]:0".parse().unwrap(), // unused
                interval: Duration::from_secs(0),    // unused
            };
            let collector_id = datastore
                .producer_endpoint_upsert_and_assign(&opctx, &producer)
                .await
                .expect("inserted producer")
                .id;
            let i = collector_ids
                .iter()
                .position(|id| *id == collector_id)
                .expect("found collector position");
            seen_collector_counts[i] += 1;
        }
        eprintln!("saw collector counts: {seen_collector_counts:?}");
        for count in seen_collector_counts {
            assert_ne!(count, 0);
        }

        // Expunge the first collector.
        datastore
            .oximeter_expunge(&opctx, collector_ids[0])
            .await
            .expect("expunged collector");

        // Repeat the test above; we should never see collector 0 chosen.
        let mut seen_collector_counts = vec![0; collector_ids.len()];
        for _ in 0..1000 {
            let producer = nexus::ProducerEndpoint {
                id: Uuid::new_v4(),
                kind: nexus::ProducerKind::Service,
                address: "[::1]:0".parse().unwrap(), // unused
                interval: Duration::from_secs(0),    // unused
            };
            let collector_id = datastore
                .producer_endpoint_upsert_and_assign(&opctx, &producer)
                .await
                .expect("inserted producer")
                .id;
            let i = collector_ids
                .iter()
                .position(|id| *id == collector_id)
                .expect("found collector position");
            seen_collector_counts[i] += 1;
        }
        eprintln!("saw collector counts: {seen_collector_counts:?}");
        assert_eq!(seen_collector_counts[0], 0);
        for count in seen_collector_counts.into_iter().skip(1) {
            assert_ne!(count, 0);
        }

        // Expunge the remaining collectors; trying to create a producer now
        // should fail.
        for &collector_id in &collector_ids[1..] {
            datastore
                .oximeter_expunge(&opctx, collector_id)
                .await
                .expect("expunged collector");
        }
        let producer = nexus::ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: nexus::ProducerKind::Service,
            address: "[::1]:0".parse().unwrap(), // unused
            interval: Duration::from_secs(0),    // unused
        };
        let err = datastore
            .producer_endpoint_upsert_and_assign(&opctx, &producer)
            .await
            .expect_err("unexpected success - all oximeters expunged")
            .to_string();
        assert!(
            err.contains("no Oximeter instances available for assignment"),
            "unexpected error: {err}"
        );

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_oximeter_reassigns_randomly() {
        // Setup
        let logctx = dev::test_setup_log("test_oximeter_reassigns_randomly");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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

        // Insert 1000 metric producers.
        let mut seen_collector_counts = vec![0; collector_ids.len()];
        for _ in 0..1000 {
            let producer = nexus::ProducerEndpoint {
                id: Uuid::new_v4(),
                kind: nexus::ProducerKind::Service,
                address: "[::1]:0".parse().unwrap(), // unused
                interval: Duration::from_secs(0),    // unused
            };
            let collector_id = datastore
                .producer_endpoint_upsert_and_assign(&opctx, &producer)
                .await
                .expect("inserted producer")
                .id;
            let i = collector_ids
                .iter()
                .position(|id| *id == collector_id)
                .expect("found collector position");
            seen_collector_counts[i] += 1;
        }
        eprintln!("saw collector counts: {seen_collector_counts:?}");
        // Sanity check that we got at least one assignment to collector 0 (so
        // our reassignment below actually does something).
        assert!(
            seen_collector_counts[0] > 0,
            "expected more than 0 assignments to collector 0 (very unlucky?!)"
        );

        // Expunge one collector.
        datastore
            .oximeter_expunge(&opctx, collector_ids[0])
            .await
            .expect("expunged Oximeter");

        // Reassign producers that belonged to that collector.
        let num_reassigned = datastore
            .oximeter_reassign_all_producers(&opctx, collector_ids[0])
            .await
            .expect("reassigned producers");
        assert_eq!(
            num_reassigned,
            CollectorReassignment::Complete(seen_collector_counts[0])
        );

        // Check the distribution of producers for each of the remaining
        // collectors. We don't know the exact count, so we'll check that:
        //
        // * Each of the three remaining collectors gained at least one (the
        //   probability that any of the three collectors gained zero is low
        //   enough that most calculators give up and call it 0)
        // * All 1000 producers are assigned to one of the three collectors
        //
        // to guard against "the reassignment query gave all of collector 0's
        // producers to exactly one of the remaining collectors", which is an
        // easy failure mode for this kind of SQL query, where the query engine
        // only evaluates the randomness once instead of once for each producer.
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

        // each gained at least one
        assert!(producer_counts[1] > seen_collector_counts[1]);
        assert!(producer_counts[2] > seen_collector_counts[2]);
        assert!(producer_counts[3] > seen_collector_counts[3]);

        // all producers are assigned
        assert_eq!(producer_counts[1..].iter().sum::<usize>(), 1000);

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_oximeter_reassign_fails_if_no_collectors() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_oximeter_reassign_fails_if_no_collectors",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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

        // Insert 100 metric producers.
        let mut seen_collector_counts = vec![0; collector_ids.len()];
        for _ in 0..100 {
            let producer = nexus::ProducerEndpoint {
                id: Uuid::new_v4(),
                kind: nexus::ProducerKind::Service,
                address: "[::1]:0".parse().unwrap(), // unused
                interval: Duration::from_secs(0),    // unused
            };
            let collector_id = datastore
                .producer_endpoint_upsert_and_assign(&opctx, &producer)
                .await
                .expect("inserted producer")
                .id;
            let i = collector_ids
                .iter()
                .position(|id| *id == collector_id)
                .expect("found collector position");
            seen_collector_counts[i] += 1;
        }

        // Delete all four collectors.
        for &collector_id in &collector_ids {
            datastore
                .oximeter_expunge(&opctx, collector_id)
                .await
                .expect("expunged Oximeter");
        }

        // Try to reassign producers that belonged to each collector; this
        // should fail, as all collectors have been expunged.
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
        for (i, &collector_id) in collector_ids.iter().enumerate() {
            let num_reassigned = datastore
                .oximeter_reassign_all_producers(&opctx, collector_id)
                .await
                .expect("reassigned producers");
            assert_eq!(
                num_reassigned,
                CollectorReassignment::Complete(seen_collector_counts[i])
            );
        }

        // All 100 producers should be assigned to our new collector.
        let nproducers = datastore
            .producers_list_by_oximeter_id(
                &opctx,
                new_collector_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("listed producers")
            .len();
        assert_eq!(nproducers, 100);

        // Cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_producers_list_expired() {
        // Setup
        let logctx = dev::test_setup_log("test_producers_list_expired");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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
        let producer = nexus::ProducerEndpoint {
            id: Uuid::new_v4(),
            kind: nexus::ProducerKind::Service,
            address: "[::1]:0".parse().unwrap(), // unused
            interval: Duration::from_secs(0),    // unused
        };
        datastore
            .producer_endpoint_upsert_and_assign(&opctx, &producer)
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
        assert_eq!(all_producers[0].id(), producer.id);

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

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
