use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::{Ipv4NatEntry, Ipv4NatValues};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::BigInt;
use nexus_db_model::ExternalIp;
use nexus_db_model::Ipv4NatEntryView;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;

impl DataStore {
    pub async fn ensure_ipv4_nat_entry(
        &self,
        opctx: &OpContext,
        nat_entry: Ipv4NatValues,
    ) -> CreateResult<Ipv4NatEntry> {
        use db::schema::ipv4_nat_entry::dsl;
        use diesel::sql_types;

        // Look up any NAT entries that already have the exact parameters
        // we're trying to INSERT.
        // We want to return any existing entry, but not to mask the UniqueViolation
        // when trying to use an existing IP + port range with a different target.
        let matching_entry_subquery = dsl::ipv4_nat_entry
            .filter(dsl::external_address.eq(nat_entry.external_address))
            .filter(dsl::first_port.eq(nat_entry.first_port))
            .filter(dsl::last_port.eq(nat_entry.last_port))
            .filter(dsl::sled_address.eq(nat_entry.sled_address))
            .filter(dsl::vni.eq(nat_entry.vni))
            .filter(dsl::mac.eq(nat_entry.mac))
            .filter(dsl::version_removed.is_null())
            .select((
                dsl::external_address,
                dsl::first_port,
                dsl::last_port,
                dsl::sled_address,
                dsl::vni,
                dsl::mac,
            ));

        // SELECT exactly the values we're trying to INSERT, but only
        // if it does not already exist.
        let new_entry_subquery = diesel::dsl::select((
            nat_entry.external_address.into_sql::<sql_types::Inet>(),
            nat_entry.first_port.into_sql::<sql_types::Int4>(),
            nat_entry.last_port.into_sql::<sql_types::Int4>(),
            nat_entry.sled_address.into_sql::<sql_types::Inet>(),
            nat_entry.vni.into_sql::<sql_types::Int4>(),
            nat_entry.mac.into_sql::<sql_types::BigInt>(),
        ))
        .filter(diesel::dsl::not(diesel::dsl::exists(matching_entry_subquery)));

        let out = diesel::insert_into(dsl::ipv4_nat_entry)
            .values(new_entry_subquery)
            .into_columns((
                dsl::external_address,
                dsl::first_port,
                dsl::last_port,
                dsl::sled_address,
                dsl::vni,
                dsl::mac,
            ))
            .returning(Ipv4NatEntry::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match out {
            Ok(o) => Ok(o),
            Err(diesel::result::Error::NotFound) => {
                // Idempotent ensure. Annoyingly, we can't easily extract
                // the existing row as part of the insert query:
                // - (SELECT ..) UNION (INSERT INTO .. RETURNING ..) isn't
                //   allowed by crdb.
                // - Can't ON CONFLICT with a partial constraint, so we can't
                //   do a no-op write and return the row that way either.
                // So, we do another lookup.
                self.ipv4_nat_find_by_values(opctx, nat_entry).await
            }
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    pub async fn ipv4_nat_delete(
        &self,
        opctx: &OpContext,
        nat_entry: &Ipv4NatEntry,
    ) -> DeleteResult {
        use db::schema::ipv4_nat_entry::dsl;

        let updated_rows = diesel::update(dsl::ipv4_nat_entry)
            .set((
                dsl::version_removed.eq(ipv4_nat_next_version().nullable()),
                dsl::time_deleted.eq(Utc::now()),
            ))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::version_removed.is_null())
            .filter(dsl::id.eq(nat_entry.id))
            .filter(dsl::version_added.eq(nat_entry.version_added))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if updated_rows == 0 {
            return Err(Error::ObjectNotFound {
                type_name: ResourceType::Ipv4NatEntry,
                lookup_type: LookupType::ByCompositeId(
                    "id, version_added".to_string(),
                ),
            });
        }
        Ok(())
    }

    pub async fn ipv4_nat_find_by_id(
        &self,
        opctx: &OpContext,
        id: uuid::Uuid,
    ) -> LookupResult<Ipv4NatEntry> {
        use db::schema::ipv4_nat_entry::dsl;

        let result = dsl::ipv4_nat_entry
            .filter(dsl::id.eq(id))
            .select(Ipv4NatEntry::as_select())
            .limit(1)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if let Some(nat_entry) = result.first() {
            Ok(nat_entry.clone())
        } else {
            Err(Error::invalid_request("no matching records"))
        }
    }

    pub async fn ipv4_nat_delete_by_external_ip(
        &self,
        opctx: &OpContext,
        external_ip: &ExternalIp,
    ) -> DeleteResult {
        use db::schema::ipv4_nat_entry::dsl;

        let updated_rows = diesel::update(dsl::ipv4_nat_entry)
            .set((
                dsl::version_removed.eq(ipv4_nat_next_version().nullable()),
                dsl::time_deleted.eq(Utc::now()),
            ))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::version_removed.is_null())
            .filter(dsl::external_address.eq(external_ip.ip))
            .filter(dsl::first_port.eq(external_ip.first_port))
            .filter(dsl::last_port.eq(external_ip.last_port))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if updated_rows == 0 {
            return Err(Error::ObjectNotFound {
                type_name: ResourceType::Ipv4NatEntry,
                lookup_type: LookupType::ByCompositeId(
                    "external_ip, first_port, last_port".to_string(),
                ),
            });
        }
        Ok(())
    }

    pub async fn ipv4_nat_find_by_values(
        &self,
        opctx: &OpContext,
        values: Ipv4NatValues,
    ) -> LookupResult<Ipv4NatEntry> {
        use db::schema::ipv4_nat_entry::dsl;
        let result = dsl::ipv4_nat_entry
            .filter(dsl::external_address.eq(values.external_address))
            .filter(dsl::first_port.eq(values.first_port))
            .filter(dsl::last_port.eq(values.last_port))
            .filter(dsl::mac.eq(values.mac))
            .filter(dsl::sled_address.eq(values.sled_address))
            .filter(dsl::vni.eq(values.vni))
            .filter(dsl::time_deleted.is_null())
            .select(Ipv4NatEntry::as_select())
            .limit(1)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if let Some(nat_entry) = result.first() {
            Ok(nat_entry.clone())
        } else {
            Err(Error::invalid_request("no matching records"))
        }
    }

    pub async fn ipv4_nat_list_since_version(
        &self,
        opctx: &OpContext,
        version: i64,
        limit: u32,
    ) -> ListResultVec<Ipv4NatEntry> {
        use db::schema::ipv4_nat_entry::dsl;

        let list = dsl::ipv4_nat_entry
            .filter(
                dsl::version_added
                    .gt(version)
                    .or(dsl::version_removed.gt(version)),
            )
            .limit(limit as i64)
            .select(Ipv4NatEntry::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(list)
    }

    pub async fn ipv4_nat_changeset(
        &self,
        opctx: &OpContext,
        version: i64,
        limit: u32,
    ) -> ListResultVec<Ipv4NatEntryView> {
        let nat_entries =
            self.ipv4_nat_list_since_version(opctx, version, limit).await?;
        let nat_entries: Vec<Ipv4NatEntryView> =
            nat_entries.iter().map(|e| e.clone().into()).collect();
        Ok(nat_entries)
    }

    pub async fn ipv4_nat_current_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<i64> {
        use db::schema::ipv4_nat_version::dsl;

        let latest: Option<i64> = dsl::ipv4_nat_version
            .select(diesel::dsl::max(dsl::last_value))
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        match latest {
            Some(value) => Ok(value),
            None => Err(Error::invalid_request("sequence table is empty!")),
        }
    }

    pub async fn ipv4_nat_cleanup(
        &self,
        opctx: &OpContext,
        version: i64,
        before_timestamp: DateTime<Utc>,
    ) -> DeleteResult {
        use db::schema::ipv4_nat_entry::dsl;

        diesel::delete(dsl::ipv4_nat_entry)
            .filter(dsl::version_removed.lt(version))
            .filter(dsl::time_deleted.lt(before_timestamp))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }
}

fn ipv4_nat_next_version() -> diesel::expression::SqlLiteral<BigInt> {
    diesel::dsl::sql::<BigInt>("nextval('omicron.public.ipv4_nat_version')")
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::db::datastore::datastore_test;
    use chrono::Utc;
    use nexus_db_model::{Ipv4NatEntry, Ipv4NatValues, MacAddr, Vni};
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external;
    use omicron_test_utils::dev;

    // Test our ability to track additions and deletions since a given version number
    #[tokio::test]
    async fn nat_version_tracking() {
        let logctx = dev::test_setup_log("test_nat_version_tracking");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // We should not have any NAT entries at this moment
        let initial_state =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        assert!(initial_state.is_empty());
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            0
        );

        // Each change (creation / deletion) to the NAT table should increment the
        // version number of the row in the NAT table
        let external_address = external::Ipv4Net(
            ipnetwork::Ipv4Network::try_from("10.0.0.100").unwrap(),
        );

        let sled_address = external::Ipv6Net(
            ipnetwork::Ipv6Network::try_from("fd00:1122:3344:104::1").unwrap(),
        );

        // Add a nat entry.
        let nat1 = Ipv4NatValues {
            external_address: external_address.into(),
            first_port: 0.into(),
            last_port: 999.into(),
            sled_address: sled_address.into(),
            vni: Vni(external::Vni::random()),
            mac: MacAddr(
                external::MacAddr::from_str("A8:40:25:F5:EB:2A").unwrap(),
            ),
        };

        datastore.ensure_ipv4_nat_entry(&opctx, nat1.clone()).await.unwrap();
        let first_entry =
            datastore.ipv4_nat_find_by_values(&opctx, nat1).await.unwrap();

        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        // The NAT table has undergone one change. One entry has been added,
        // none deleted, so we should be at version 1.
        assert_eq!(nat_entries.len(), 1);
        assert_eq!(nat_entries.last().unwrap().version_added, 1);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            1
        );

        // Add another nat entry.
        let nat2 = Ipv4NatValues {
            external_address: external_address.into(),
            first_port: 1000.into(),
            last_port: 1999.into(),
            sled_address: sled_address.into(),
            vni: Vni(external::Vni::random()),
            mac: MacAddr(
                external::MacAddr::from_str("A8:40:25:F5:EB:2B").unwrap(),
            ),
        };

        datastore.ensure_ipv4_nat_entry(&opctx, nat2).await.unwrap();

        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        // The NAT table has undergone two changes. Two entries have been
        // added, none deleted, so we should be at version 2.
        let nat_entry =
            nat_entries.iter().find(|e| e.version_added == 2).unwrap();
        assert_eq!(nat_entries.len(), 2);
        assert_eq!(nat_entry.version_added, 2);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            2
        );

        // Test Cleanup logic
        // Cleanup should only perma-delete entries that are older than a
        // specified version number and whose `time_deleted` field is
        // older than a specified age.
        let time_cutoff = Utc::now();
        datastore.ipv4_nat_cleanup(&opctx, 2, time_cutoff).await.unwrap();

        // Nothing should have changed (no records currently marked for deletion)
        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        assert_eq!(nat_entries.len(), 2);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            2
        );

        // Delete the first nat entry. It should show up as a later version number.
        datastore.ipv4_nat_delete(&opctx, &first_entry).await.unwrap();
        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        // The NAT table has undergone three changes. Two entries have been
        // added, one deleted, so we should be at version 3. Since the
        // first entry was marked for deletion (and it was the third change),
        // the first entry's version number should now be 3.
        let nat_entry =
            nat_entries.iter().find(|e| e.version_removed.is_some()).unwrap();
        assert_eq!(nat_entries.len(), 2);
        assert_eq!(nat_entry.version_removed, Some(3));
        assert_eq!(nat_entry.id, first_entry.id);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            3
        );

        // Try cleaning up with the old version and time cutoff values
        datastore.ipv4_nat_cleanup(&opctx, 2, time_cutoff).await.unwrap();

        // Try cleaning up with a greater version and old time cutoff values
        datastore.ipv4_nat_cleanup(&opctx, 6, time_cutoff).await.unwrap();

        // Try cleaning up with a older version and newer time cutoff values
        datastore.ipv4_nat_cleanup(&opctx, 2, Utc::now()).await.unwrap();

        // Both records should still exist (soft deleted record is newer than cutoff
        // values )
        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        assert_eq!(nat_entries.len(), 2);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            3
        );

        // Try cleaning up with a both cutoff values increased
        datastore.ipv4_nat_cleanup(&opctx, 4, Utc::now()).await.unwrap();

        // Soft deleted NAT entry should be removed from the table
        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        assert_eq!(nat_entries.len(), 1);
        // version should be unchanged
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            3
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    /// Table design and queries should only insert one active NAT entry for a given
    /// set of properties, but allow multiple deleted nat entries for the same set
    /// of properties.
    async fn table_allows_unique_active_multiple_deleted() {
        let logctx = dev::test_setup_log("test_nat_version_tracking");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // We should not have any NAT entries at this moment
        let initial_state =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        assert!(initial_state.is_empty());
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            0
        );

        // Each change (creation / deletion) to the NAT table should increment the
        // version number of the row in the NAT table
        let external_address = external::Ipv4Net(
            ipnetwork::Ipv4Network::try_from("10.0.0.100").unwrap(),
        );

        let sled_address = external::Ipv6Net(
            ipnetwork::Ipv6Network::try_from("fd00:1122:3344:104::1").unwrap(),
        );

        // Add a nat entry.
        let nat1 = Ipv4NatValues {
            external_address: external_address.into(),
            first_port: 0.into(),
            last_port: 999.into(),
            sled_address: sled_address.into(),
            vni: Vni(external::Vni::random()),
            mac: MacAddr(
                external::MacAddr::from_str("A8:40:25:F5:EB:2A").unwrap(),
            ),
        };

        datastore.ensure_ipv4_nat_entry(&opctx, nat1.clone()).await.unwrap();

        // Try to add it again. It should still only result in a single entry.
        datastore.ensure_ipv4_nat_entry(&opctx, nat1.clone()).await.unwrap();
        let first_entry = datastore
            .ipv4_nat_find_by_values(&opctx, nat1.clone())
            .await
            .unwrap();

        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        // The NAT table has undergone one change. One entry has been added,
        // none deleted, so we should be at version 1.
        assert_eq!(nat_entries.len(), 1);
        assert_eq!(nat_entries.last().unwrap().version_added, 1);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            1
        );

        datastore.ipv4_nat_delete(&opctx, &first_entry).await.unwrap();

        // The NAT table has undergone two changes. One entry has been added,
        // then deleted, so we should be at version 2.
        let nat_entries = datastore
            .ipv4_nat_list_since_version(&opctx, 0, 10)
            .await
            .unwrap()
            .into_iter();

        let active: Vec<Ipv4NatEntry> = nat_entries
            .clone()
            .filter(|entry| entry.version_removed.is_none())
            .collect();

        let inactive: Vec<Ipv4NatEntry> = nat_entries
            .filter(|entry| entry.version_removed.is_some())
            .collect();

        assert!(active.is_empty());
        assert_eq!(inactive.len(), 1);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            2
        );

        // Add the same entry back. This simulates the behavior we will see
        // when stopping and then restarting an instance.
        datastore.ensure_ipv4_nat_entry(&opctx, nat1.clone()).await.unwrap();

        // The NAT table has undergone three changes.
        let nat_entries = datastore
            .ipv4_nat_list_since_version(&opctx, 0, 10)
            .await
            .unwrap()
            .into_iter();

        let active: Vec<Ipv4NatEntry> = nat_entries
            .clone()
            .filter(|entry| entry.version_removed.is_none())
            .collect();

        let inactive: Vec<Ipv4NatEntry> = nat_entries
            .filter(|entry| entry.version_removed.is_some())
            .collect();

        assert_eq!(active.len(), 1);
        assert_eq!(inactive.len(), 1);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            3
        );

        let second_entry =
            datastore.ipv4_nat_find_by_values(&opctx, nat1).await.unwrap();
        datastore.ipv4_nat_delete(&opctx, &second_entry).await.unwrap();

        // The NAT table has undergone four changes
        let nat_entries = datastore
            .ipv4_nat_list_since_version(&opctx, 0, 10)
            .await
            .unwrap()
            .into_iter();

        let active: Vec<Ipv4NatEntry> = nat_entries
            .clone()
            .filter(|entry| entry.version_removed.is_none())
            .collect();

        let inactive: Vec<Ipv4NatEntry> = nat_entries
            .filter(|entry| entry.version_removed.is_some())
            .collect();

        assert_eq!(active.len(), 0);
        assert_eq!(inactive.len(), 2);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            4
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
