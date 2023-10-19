use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::{Ipv4NatEntry, Ipv4NatValues};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::BigInt;
use diesel::sql_types::Inet;
use diesel::sql_types::Integer;
use diesel::sql_types::Uuid;
use nexus_db_model::ExternalIp;
use nexus_db_model::Ipv4NatEntryView;
use nexus_db_model::SqlU32;
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
    ) -> CreateResult<()> {
        // Ensure that the record with the parameters we want exists in the
        // database. If an entry exists with the same external ip but
        // different target sled / vpc / mac, we will violate a uniqueness
        // constraint (which is desired in this case).
        //
        // It seems that this isn't straightforward to express in diesel
        sql_query(
            "
            INSERT INTO omicron.public.ipv4_nat_entry (
                external_address,
                first_port,
                last_port,
                sled_address,
                vni,
                mac
            )
            SELECT external_address, first_port, last_port, sled_address, vni, mac
            FROM ( VALUES ($1, $2, $3, $4, $5, $6) ) AS data (external_address, first_port, last_port, sled_address, vni, mac)
            WHERE NOT EXISTS (
                SELECT external_address, first_port, last_port, sled_address, vni, mac
                FROM omicron.public.ipv4_nat_entry as entry
                WHERE data.external_address = entry.external_address
                AND data.first_port = entry.first_port
                AND data.last_port = entry.last_port
                AND data.sled_address = entry.sled_address
                AND data.vni = entry.vni
                AND data.mac = entry.mac
            )
            ",
        )
            .bind::<Inet, _>(nat_entry.external_address)
            .bind::<Integer, _>(nat_entry.first_port)
            .bind::<Integer, _>(nat_entry.last_port)
            .bind::<Inet, _>(nat_entry.sled_address)
            .bind::<Integer, _>(nat_entry.vni)
            .bind::<BigInt, _>(nat_entry.mac)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn ipv4_nat_delete(
        &self,
        opctx: &OpContext,
        nat_entry: &Ipv4NatEntry,
    ) -> DeleteResult {
        // We use pure SQL here so we can call nextval() to increment the sequence number in the
        // database.
        let updated_rows = sql_query(
            "
            UPDATE omicron.public.ipv4_nat_entry
            SET
            version_removed = nextval('omicron.public.ipv4_nat_version'),
            time_deleted = now()
            WHERE time_deleted IS NULL AND version_removed IS NULL AND id = $1 AND version_added = $2
            ",
        )
            .bind::<Uuid, _>(nat_entry.id)
            .bind::<BigInt, _>(nat_entry.version_added)
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

    pub async fn ipv4_nat_cancel_delete(
        &self,
        opctx: &OpContext,
        nat_entry: &Ipv4NatEntry,
    ) -> DeleteResult {
        let version_removed =
            nat_entry.version_removed.ok_or(Error::InvalidRequest {
                message: "supplied nat entry does not have version_deleted"
                    .to_string(),
            })?;

        let updated_rows = sql_query(
            "
            UPDATE omicron.public.ipv4_nat_entry
            SET
            version_removed = NULL,
            time_deleted = NULL,
            version_added = nextval('omicron.public.ipv4_nat_version')
            WHERE version_removed IS NOT NULL AND id = $1 AND version_removed = $2
            ",
        )
            .bind::<Uuid, _>(nat_entry.id)
            .bind::<BigInt, _>(version_removed)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if updated_rows == 0 {
            return Err(Error::InvalidRequest {
                message: "no matching records".to_string(),
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
            Err(Error::InvalidRequest {
                message: "no matching records".to_string(),
            })
        }
    }

    pub async fn ipv4_nat_delete_by_external_ip(
        &self,
        opctx: &OpContext,
        external_ip: &ExternalIp,
    ) -> DeleteResult {
        use db::schema::ipv4_nat_entry::dsl;
        let now = Utc::now();
        let updated_rows = diesel::update(dsl::ipv4_nat_entry)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::external_address.eq(external_ip.ip))
            .filter(dsl::first_port.eq(external_ip.first_port))
            .filter(dsl::last_port.eq(external_ip.last_port))
            .set(dsl::time_deleted.eq(now))
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
            Err(Error::InvalidRequest {
                message: "no matching records".to_string(),
            })
        }
    }

    pub async fn ipv4_nat_list_since_version(
        &self,
        opctx: &OpContext,
        version: u32,
        limit: u32,
    ) -> ListResultVec<Ipv4NatEntry> {
        use db::schema::ipv4_nat_entry::dsl;
        let version: SqlU32 = version.into();

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
        version: u32,
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
    ) -> LookupResult<u32> {
        use db::schema::ipv4_nat_version::dsl;

        let latest: Option<SqlU32> = dsl::ipv4_nat_version
            .select(diesel::dsl::max(dsl::last_value))
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        match latest {
            Some(value) => Ok(*value),
            None => Err(Error::InvalidRequest {
                message: "sequence table is empty!".to_string(),
            }),
        }
    }

    pub async fn ipv4_nat_cleanup(
        &self,
        opctx: &OpContext,
        before_version: u32,
        before_timestamp: DateTime<Utc>,
    ) -> DeleteResult {
        use db::schema::ipv4_nat_entry::dsl;
        let version: SqlU32 = before_version.into();

        diesel::delete(dsl::ipv4_nat_entry)
            .filter(dsl::version_removed.lt(version))
            .filter(dsl::time_deleted.lt(before_timestamp))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::db::datastore::datastore_test;
    use chrono::Utc;
    use nexus_db_model::{Ipv4NatValues, MacAddr, Vni};
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
        let external_address =
            ipnetwork::IpNetwork::try_from("10.0.0.100").unwrap();

        let sled_address =
            ipnetwork::IpNetwork::try_from("fd00:1122:3344:104::1").unwrap();

        // Add a nat entry.
        let nat1 = Ipv4NatValues {
            external_address,
            first_port: 0.into(),
            last_port: 999.into(),
            sled_address,
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
        assert_eq!(nat_entries.last().unwrap().version_added(), 1);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            1
        );

        // Add another nat entry.
        let nat2 = Ipv4NatValues {
            external_address,
            first_port: 1000.into(),
            last_port: 1999.into(),
            sled_address,
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
            nat_entries.iter().find(|e| e.version_added() == 2).unwrap();
        assert_eq!(nat_entries.len(), 2);
        assert_eq!(nat_entry.version_added(), 2);
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
            nat_entries.iter().find(|e| e.version_removed().is_some()).unwrap();
        assert_eq!(nat_entries.len(), 2);
        assert_eq!(nat_entry.version_removed(), Some(3));
        assert_eq!(nat_entry.id, first_entry.id);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            3
        );

        // Cancel deletion of NAT entry
        datastore.ipv4_nat_cancel_delete(&opctx, nat_entry).await.unwrap();
        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        // The NAT table has undergone four changes.
        let nat_entry = nat_entries.last().unwrap();
        assert_eq!(nat_entries.len(), 2);
        assert_eq!(nat_entry.version_added(), 4);
        assert_eq!(nat_entry.id, first_entry.id);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            4
        );

        // Test Cleanup logic
        // Cleanup should only perma-delete entries that are older than a
        // specified version number and whose `time_deleted` field is
        // older than a specified age.
        let time_cutoff = Utc::now();
        datastore.ipv4_nat_cleanup(&opctx, 4, time_cutoff).await.unwrap();

        // Nothing should have changed (no records currently marked for deletion)
        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        assert_eq!(nat_entries.len(), 2);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            4
        );

        // Soft delete a record
        let nat_entry = nat_entries.last().unwrap();
        datastore.ipv4_nat_delete(&opctx, nat_entry).await.unwrap();

        // Try cleaning up with the old version and time cutoff values
        datastore.ipv4_nat_cleanup(&opctx, 4, time_cutoff).await.unwrap();

        // Try cleaning up with a greater version and old time cutoff values
        datastore.ipv4_nat_cleanup(&opctx, 6, time_cutoff).await.unwrap();

        // Try cleaning up with a older version and newer time cutoff values
        datastore.ipv4_nat_cleanup(&opctx, 4, Utc::now()).await.unwrap();

        // Both records should still exist (soft deleted record is newer than cutoff
        // values )
        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        assert_eq!(nat_entries.len(), 2);
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            5
        );

        // Try cleaning up with a both cutoff values increased
        datastore.ipv4_nat_cleanup(&opctx, 6, Utc::now()).await.unwrap();

        // Soft deleted NAT entry should be removed from the table
        let nat_entries =
            datastore.ipv4_nat_list_since_version(&opctx, 0, 10).await.unwrap();

        assert_eq!(nat_entries.len(), 1);

        // version should be unchanged
        assert_eq!(
            datastore.ipv4_nat_current_version(&opctx).await.unwrap(),
            5
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
