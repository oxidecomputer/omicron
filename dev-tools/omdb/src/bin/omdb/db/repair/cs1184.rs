// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::repair::CorruptedStateInfo;
use crate::db::repair::DatabaseCorruption;
use crate::db::repair::common;
use crate::helpers::ConfirmationPrompt;
use anyhow::Result;
use anyhow::bail;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use diesel::prelude::*;
use nexus_db_lookup::DataStoreConnection;
use nexus_db_lookup::DbConnection;
use nexus_db_model::Disk;
use nexus_db_model::DiskTypeLocalStorage;
use nexus_db_model::LocalStorageUnencryptedDatasetAllocation;
use nexus_db_model::Sled;
use nexus_db_model::Vmm;
use nexus_db_model::VmmState;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::LocalStorageDisk;
use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external::DiskState;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use propolis_client::instance_spec::Component;
use propolis_client::instance_spec::FileStorageBackend;
use propolis_client::instance_spec::InstanceSpecGetResponse;
use propolis_client::instance_spec::InstanceSpecStatus;
use propolis_client::instance_spec::SpecKey;
use slog::Logger;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use uuid::Uuid;

/// customer-support#1184 resulted from multiple concurrent instance-start sagas
/// running for the same instance, and this would create multiple local storage
/// allocations for the same disk that would overwrite the allocation_id in the
/// disk_type_local_storage table. Two problems result from this:
///
/// 1. Instances that are actually started _before_ this overwriting occurred
///    will get the _old_ local storage allocation details as part of the file
///    backed disk(s) in the instance spec sent to the VMM. When that instance
///    is stopped and started, Nexus will send the _new_ allocation details are
///    sent, resulting in the guest coming up with blank disks instead of what
///    was previously written.
///
/// 2. Multiple local storage allocations can be orphaned from their higher
///    level disk, and Nexus isn't aware of them. They still consume pool space,
///    will cause capacity issues, and because they're orphaned cannot be
///    deleted by the user.
///
/// This command will go through the following steps:
///
/// 1. Look for orphaned local storage allocation records, where an orphaned
///    record does not have a match in `disk_type_local_storage`.
///
/// 2. Determine, as best as possible, if a propolis ever used one of these
///    orphaned allocations.
///
/// 3. Clean up orphaned allocations if it can be safely done, or indicate that
///    manual intervention is required.
///
/// Part of the detection process parses saga node output, and is therefore
/// brittle to that changing!
pub struct OrphanedLocalStorageAllocations {
    datastore: Arc<DataStore>,
    log: Logger,
}

impl OrphanedLocalStorageAllocations {
    pub async fn find_orphaned_local_storage_allocations(
        &self,
    ) -> Result<Vec<Uuid>> {
        let conn = self.datastore.pool_connection_for_tests().await?;

        // Find all non-deleted allocations that do _not_ have a
        // disk_type_local_storage record pointing to them.

        let orphaned_record_ids: Vec<Uuid> = conn.transaction_async(async move |conn| {
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

            use nexus_db_schema::schema::disk_type_local_storage::dsl as dtls_dsl;
            use nexus_db_schema::schema::local_storage_unencrypted_dataset_allocation::dsl as lsuda_dsl;

            lsuda_dsl::local_storage_unencrypted_dataset_allocation
                .left_join(dtls_dsl::disk_type_local_storage.on(
                    dtls_dsl::local_storage_unencrypted_dataset_allocation_id
                        .eq(lsuda_dsl::id.nullable())
                ))
                .filter(dtls_dsl::disk_id.is_null())
                .filter(lsuda_dsl::time_deleted.is_null())
                .select(lsuda_dsl::id)
                .load_async(&conn)
                .await
        })
        .await?;

        Ok(orphaned_record_ids)
    }

    /// Find the instance start saga whose `list_local_storage` node output
    /// includes the argument local storage allocation id
    async fn find_saga_associated_with_allocation_id(
        conn: &DataStoreConnection,
        allocation_id: Uuid,
    ) -> Result<Option<Uuid>> {
        // `sql_query()` requires extracting the results with a struct.
        #[derive(Debug, diesel::QueryableByName)]
        struct QueryResult {
            #[diesel(sql_type = diesel::sql_types::Uuid)]
            saga_id: Uuid,
        }

        let maybe_results: Vec<QueryResult> = conn
            .transaction_async(async move |conn| {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                // Find all sagas that have a `list_local_storage` node, and
                // have the allocation ID in that node's output.
                //
                // This is executed as raw sql because of the data::TEXT where
                // clause. The other filters are for the `list_local_storage`
                // node in the instance start saga.
                //
                // TODO-maybe: the other `saga_dag::TEXT` _should_ be a JSONB
                // query but I couldn't make it work, something like
                //
                //  saga_dag->'graph'->'nodes'[5]->'Action'->'name' == `local_storage_records`

                let query = format!(
                    "select saga_id from \
                saga_node_event \
                  join saga on saga.id = saga_node_event.saga_id \
                where \
                  name = 'instance-start' and \
                  node_id = 5 and
                  event_type = 'succeeded' and
                  saga_dag::TEXT like '%list_local_storage%' and
                  data::TEXT like '%{}%'",
                    allocation_id,
                );

                diesel::sql_query(&query).load_async(&conn).await
            })
            .await?;

        // This author assumes that only one saga will ever be returned by this
        // query - the allocation is assumed to have been orphaned. If that's
        // not true, bail out now!

        if maybe_results.len() > 1 {
            bail!("more than one saga id returned! {maybe_results:?}");
        }

        Ok(maybe_results.into_iter().map(|result| result.saga_id).next())
    }

    async fn vmm_id_for_instance_start_saga(
        conn: &DataStoreConnection,
        saga_id: Uuid,
    ) -> Result<Uuid> {
        // Grab the vmm id from the saga step that generates that random uuid

        let vmm_id: Option<serde_json::Value> = conn
            .transaction_async(async move |conn| {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                use nexus_db_schema::schema::saga_node_event::dsl;

                dsl::saga_node_event
                    .filter(dsl::saga_id.eq(saga_id))
                    .filter(dsl::node_id.eq(0))
                    .filter(dsl::event_type.eq("succeeded"))
                    .select(dsl::data)
                    .get_result_async(&conn)
                    .await
            })
            .await?;

        let Some(vmm_id) = vmm_id else {
            bail!("vmm id is None for {saga_id}!");
        };

        let serde_json::Value::String(vmm_id) = vmm_id else {
            bail!("{vmm_id} not string!");
        };

        Ok(vmm_id.parse()?)
    }

    /// Return the deserialized `list_local_storage` node output for a
    /// particular instance-start saga
    async fn local_storage_list_for_instance_start_saga(
        conn: &DataStoreConnection,
        saga_id: Uuid,
    ) -> Result<Vec<LocalStorageDisk>> {
        // Grab the saga node raw output

        let list_local_storage_output: Option<serde_json::Value> = conn
            .transaction_async(async move |conn| {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                use nexus_db_schema::schema::saga_node_event::dsl;

                dsl::saga_node_event
                    .filter(dsl::saga_id.eq(saga_id))
                    .filter(dsl::node_id.eq(5))
                    .filter(dsl::event_type.eq("succeeded"))
                    .select(dsl::data)
                    .get_result_async(&conn)
                    .await
            })
            .await?;

        let Some(list_local_storage_output) = list_local_storage_output else {
            bail!(
                "no successful `list_local_storage` output in saga {saga_id}"
            );
        };

        // Then deserialize from serde_json::Value into LocalStorageDisk

        let list_local_storage_output: Vec<LocalStorageDisk> =
            match list_local_storage_output {
                serde_json::Value::Array(_) => {
                    match serde_json::from_value(list_local_storage_output) {
                        Ok(v) => v,
                        Err(e) => {
                            bail!(
                                "`list_local_storage` output deserialize \
                                failed for saga {saga_id}: {e}"
                            );
                        }
                    }
                }

                _ => {
                    bail!(
                        "wrong serde_value variant \
                        {list_local_storage_output:?} for `list_local_storage` \
                        output in saga {saga_id}"
                    );
                }
            };

        Ok(list_local_storage_output)
    }

    /// Automatically garbage collect all orphaned allocations that were never
    /// used by a VMM
    pub async fn garbage_collect_never_used_allocations(
        &self,
        _: DestructiveOperationToken,
    ) -> Result<()> {
        let conn = self.datastore.pool_connection_for_tests().await?;

        let orphaned_record_ids =
            self.find_orphaned_local_storage_allocations().await?;

        for orphaned_record_id in orphaned_record_ids {
            // Grab the allocation, partially to ensure it's not deleted

            let allocation = {
                use nexus_db_schema::schema::local_storage_unencrypted_dataset_allocation::dsl;

                dsl::local_storage_unencrypted_dataset_allocation
                    .filter(dsl::id.eq(orphaned_record_id))
                    .select(
                        LocalStorageUnencryptedDatasetAllocation::as_select(),
                    )
                    .get_result_async(&*conn)
                    .await?
            };

            if allocation.time_deleted.is_some() {
                println!("allocation {orphaned_record_id} already deleted");
                continue;
            }

            let maybe_saga_id = Self::find_saga_associated_with_allocation_id(
                &conn,
                orphaned_record_id,
            )
            .await?;

            // Find if a disk backed by this allocation was ever in the list of
            // local storage disks output by the instance-start saga. This
            // tool's job is a _lot_ easier if it never was :)

            let maybe_disk_id: Option<Uuid> = if let Some(saga_id) =
                maybe_saga_id
            {
                info!(self.log, "found affected saga {saga_id}");

                let list_local_storage_output =
                    Self::local_storage_list_for_instance_start_saga(
                        &conn, saga_id,
                    )
                    .await?;

                let disk_id = list_local_storage_output
                    .into_iter()
                    .find(|local_storage_disk| {
                        orphaned_record_id == local_storage_disk
                            .disk_type_local_storage
                            .local_storage_unencrypted_dataset_allocation_id()
                            .expect(
                                "at this point in the saga all disks are allocated"
                            )
                            .into_untyped_uuid()
                    })
                    .map(|local_storage_disk: LocalStorageDisk| local_storage_disk.id())
                    .expect(
                        "this should return something, otherwise \
                        `find_saga_associated_with_allocation_id` would not have \
                        returned this saga id"
                    );

                Some(disk_id)
            } else {
                info!(self.log, "no affected saga found");
                None
            };

            if maybe_disk_id.is_none() {
                // No instance-start saga output that listed local storage disks
                // using this allocation ID was found, so it's ok to delete: the
                // instance-start saga will only ensure local storage exists
                // based on the output of the `list_local_storage` node, and if
                // this allocation ID does not appear there then it was never
                // ensured.  Even if an instance spec was created that used a
                // zvol path based on this allocation, the propolis would fail
                // to boot due to the zvol having never been created.

                println!(
                    "allocation {orphaned_record_id} could never have been \
                    ensured, deleting",
                );

                DataStore::delete_local_storage_unencrypted_dataset_allocation_in_txn(
                    &conn,
                    allocation.id(),
                )
                .await?;
            }

            // If disk_id is Some(_), then the other sub-command has to be used
        }

        Ok(())
    }

    /// Change the local storage pointed to by `disk_id` to be backed by the
    /// argument `replacement_allocation`. If the disk is attached to an
    /// instance with a running VMM, or there was a previously running VMM that
    /// is now destroyed, then the `replacement` allocation sled _must_ match
    /// this running VMM's sled.
    async fn change_disk_allocation(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        disk_id: Uuid,
        replacement_allocation: &LocalStorageUnencryptedDatasetAllocation,
        vmm_sled_id: Option<SledUuid>,
    ) -> Result<()> {
        let replacement_allocation_id = replacement_allocation.id();
        let replacement_allocation_sled_id = replacement_allocation.sled_id();

        let existing_allocation_id: DatasetUuid = {
            use nexus_db_schema::schema::disk_type_local_storage::dsl;

            dsl::disk_type_local_storage
                .filter(dsl::disk_id.eq(disk_id))
                .select(DiskTypeLocalStorage::as_select())
                .get_result_async(conn)
                .await?
                .local_storage_unencrypted_dataset_allocation_id()
                .expect("if there is a VMM, this should be Some")
        };

        println!(
            "setting disk_id {disk_id} allocation id from \
            {existing_allocation_id} to {replacement_allocation_id}"
        );

        // If there is a currently running VMM for the instance where the disk
        // is attached, or there was a running VMM that is now detroyed, then
        // make sure the replacement allocation's sled id matches it - otherwise
        // when the instance is stopped and started, then the current local
        // storage allocation (which is not orphaned) is on a different sled.
        //
        // Note the booted VMM's allocation would not have succeeded unless the
        // affinity / anti-affinity rules were respected, so there's no worries
        // there: we just don't want to create an unbootable instance whose
        // local storage spans multiple sleds.

        if let Some(vmm_sled_id) = vmm_sled_id {
            if vmm_sled_id != replacement_allocation_sled_id {
                bail!(
                    "replacement allocation's sled {} does not match supplied \
                    VMM sled id {}",
                    replacement_allocation_sled_id,
                    vmm_sled_id,
                );
            }
        }

        // Past here, change the allocation id

        let rows_updated = {
            use nexus_db_schema::schema::disk_type_local_storage::dsl;

            diesel::update(dsl::disk_type_local_storage)
                .filter(dsl::disk_id.eq(disk_id))
                .filter(
                    dsl::local_storage_unencrypted_dataset_allocation_id
                        .eq(to_db_typed_uuid(existing_allocation_id)),
                )
                .set(
                    dsl::local_storage_unencrypted_dataset_allocation_id
                        .eq(to_db_typed_uuid(replacement_allocation_id)),
                )
                .execute_async(conn)
                .await?
        };

        if rows_updated != 1 {
            bail!("rows_updated was {rows_updated}, not 1!");
        }

        println!(
            "disk {disk_id} is now using allocation {}",
            replacement_allocation.id(),
        );

        Ok(())
    }

    pub async fn garbage_collect_allocation(
        &self,
        _: &DestructiveOperationToken,
        allocation_id: Uuid,
        interactive: bool,
    ) -> Result<()> {
        let conn = self.datastore.pool_connection_for_tests().await?;

        // Grab the allocation, partially to ensure it's not deleted

        let allocation = {
            use nexus_db_schema::schema::local_storage_unencrypted_dataset_allocation::dsl;

            dsl::local_storage_unencrypted_dataset_allocation
                .filter(dsl::id.eq(allocation_id))
                .select(LocalStorageUnencryptedDatasetAllocation::as_select())
                .get_result_async(&*conn)
                .await?
        };

        if allocation.time_deleted.is_some() {
            println!("allocation {allocation_id} already deleted");
            return Ok(());
        }

        // Ensure that the allocation_id is actually orphaned

        let maybe_associated_disk: Option<DiskTypeLocalStorage> = conn
            .transaction_async(async move |conn| {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                use nexus_db_schema::schema::disk_type_local_storage::dsl;

                dsl::disk_type_local_storage
                    .filter(
                        dsl::local_storage_unencrypted_dataset_allocation_id
                            .eq(allocation_id),
                    )
                    .select(DiskTypeLocalStorage::as_select())
                    .get_result_async(&conn)
                    .await
                    .optional()
            })
            .await?;

        if let Some(associated_disk) = maybe_associated_disk {
            bail!(
                "allocation id {} not orphaned, it's associated with disk {}",
                allocation_id,
                associated_disk.disk_id(),
            );
        }

        // For the warning message, try to find the associated disk this
        // orphaned allocation was made for

        let maybe_saga_id =
            Self::find_saga_associated_with_allocation_id(&conn, allocation_id)
                .await?;

        // Find if a disk backed by this allocation was ever in the list of
        // local storage disks output by the instance-start saga. This tool's
        // job is a _lot_ easier if it never was :)

        let maybe_disk_id: Option<Uuid> = if let Some(saga_id) = maybe_saga_id {
            info!(self.log, "found affected saga {saga_id}");

            let list_local_storage_output =
                Self::local_storage_list_for_instance_start_saga(
                    &conn, saga_id,
                )
                .await?;

            let disk_id = list_local_storage_output
                .into_iter()
                .find(|local_storage_disk| {
                    allocation_id == local_storage_disk
                        .disk_type_local_storage
                        .local_storage_unencrypted_dataset_allocation_id()
                        .expect(
                            "at this point in the saga all disks are allocated"
                        )
                        .into_untyped_uuid()
                })
                .map(|local_storage_disk: LocalStorageDisk| {
                    local_storage_disk.id()
                })
                .expect(
                    "this should return something, otherwise \
                    `find_saga_associated_with_allocation_id` would not have \
                    returned this saga id",
                );

            Some(disk_id)
        } else {
            info!(self.log, "no affected saga found");
            None
        };

        let Some(disk_id) = maybe_disk_id else {
            // No instance-start saga output that listed local storage disks
            // using this allocation ID was found, so it's ok to delete: the
            // instance-start saga will only ensure local storage exists based
            // on the output of the `list_local_storage` node, and if this
            // allocation ID does not appear there then it was never ensured.
            // Even if an instance spec was created that used a zvol path based
            // on this allocation, the propolis would fail to boot due to the
            // zvol having never been created.

            println!(
                "allocation {allocation_id} could never have been ensured, \
                deleting",
            );

            DataStore::delete_local_storage_unencrypted_dataset_allocation_in_txn(
                &conn,
                allocation.id(),
            )
            .await?;

            return Ok(());
        };

        // Did the customer delete the disk?

        let disk = {
            use nexus_db_schema::schema::disk::dsl;

            dsl::disk
                .filter(dsl::id.eq(disk_id))
                .select(Disk::as_select())
                .get_result_async(&*conn)
                .await
                .unwrap()
        };

        if *disk.state().state() == DiskState::Destroyed {
            println!(
                r#"
allocation {allocation_id} _may_ have been used by a VMM, but the higher level
disk {} (id {}) was deleted by the customer, so omdb can freely garbage collect
all allocations made for this disk"#,
                disk.name(),
                disk.id(),
            );

            common::remove_local_storage_allocation(
                &self.log,
                &conn,
                &allocation,
            )
            .await?;

            return Ok(());
        }

        if *disk.state().state() == DiskState::Faulted {
            println!(
                r#"
allocation {allocation_id} _may_ have been used by a VMM, but the higher level
disk {} (id {}) is faulted, so omdb can freely garbage collect all allocations
made for this disk"#,
                disk.name(),
                disk.id(),
            );

            common::remove_local_storage_allocation(
                &self.log,
                &conn,
                &allocation,
            )
            .await?;

            return Ok(());
        }

        // Try to determine if this allocation was ever used by a VMM: it may
        // have been used by a propolis, _then_ orphaned in the database.

        let vmm_id =
            Self::vmm_id_for_instance_start_saga(&conn, maybe_saga_id.unwrap())
                .await?;

        // Based on VMM state, reach out to the propolis to check, or print a
        // warning message if omdb can't figure it out.

        let vmm = {
            use nexus_db_schema::schema::vmm::dsl;

            dsl::vmm
                .filter(dsl::id.eq(vmm_id))
                .select(Vmm::as_select())
                .get_result_async(&*conn)
                .await
                .unwrap()
        };

        if vmm.state == VmmState::Running {
            info!(self.log, "vmm {vmm_id} is running");

            // If the VMM is on a sled, we don't have to guess if it used an
            // allocation or not: ask Propolis for the instance spec so we can
            // compare the allocation ids to what is currently in the database.

            let url = format!(
                "http://[{}]:{}",
                vmm.propolis_ip.ip(),
                vmm.propolis_port,
            );

            let client = propolis_client::Client::new(&url);

            let spec = match client.instance_spec_get().send().await {
                Err(e) => {
                    bail!(
                        "could not get instance spec from {url}: {}",
                        InlineErrorChain::new(&e)
                    );
                }

                Ok(response) => {
                    let InstanceSpecGetResponse {
                        properties: _,
                        state: _,
                        spec: InstanceSpecStatus::Present(spec),
                    } = response.into_inner()
                    else {
                        bail!(
                            "vmm {vmm_id} instance spec at {url} not \
                            present"
                        );
                    };

                    info!(
                        self.log,
                        "vmm {vmm_id} instance spec retrieved from {url} ok"
                    );

                    spec
                }
            };

            // Check that instance spec for file backed storage, and see if
            // it's using the zvol for this allocation id

            let mut vmm_is_using_orphaned_allocation = false;

            for (spec_key, component) in &spec.components {
                // Look to the spec key for the disk id, then check that
                // disk's allocation

                let SpecKey::Uuid(spec_id) = spec_key else {
                    continue;
                };

                // Does the spec disk id match the one we're looking for?

                if *spec_id != disk_id {
                    continue;
                }

                match component {
                    Component::FileStorageBackend(FileStorageBackend {
                        path,
                        readonly: _,
                        block_size: _,
                        workers: _,
                    }) => {
                        // Check the path to see if it matches the pattern
                        // that local storage uses. An example, with offsets
                        // into the split string:
                        //
                        // 0: /
                        // 1: dev/
                        // 2: zvol/
                        // 3: rdsk/
                        // 4: oxp_6aa87b66-0281-4c7a-941c-c9ec6faac9bb/
                        // 5: local_storage_unencrypted/
                        // 6: 0822c0be-fcd7-4311-88bb-e3ed422b6ec5/
                        // 7: vol

                        let parts: Vec<_> = path.split("/").collect();

                        if parts.len() >= 8
                            && parts[1] == "dev"
                            && parts[2] == "zvol"
                            && parts[3] == "rdsk"
                            && parts[5] == "local_storage_unencrypted"
                            && parts[7] == "vol"
                        {
                            // this matches what is expected of unencrypted
                            // local storage paths
                        } else {
                            warn!(
                                self.log,
                                "path {path} does not match local storage \
                                pattern"
                            );

                            continue;
                        }

                        let pool_part = parts[4];
                        let pool_id_parts: Vec<_> =
                            pool_part.split("_").collect();

                        if pool_id_parts.len() < 2 {
                            warn!(
                                self.log,
                                "could not parse {pool_part} for pool_id"
                            );

                            continue;
                        }

                        if pool_id_parts[0] != "oxp" {
                            warn!(
                                self.log,
                                "{pool_part} does not begin with oxp_"
                            );

                            continue;
                        }

                        let Ok(_pool_id) = pool_id_parts[1].parse::<Uuid>()
                        else {
                            warn!(
                                self.log,
                                "could not parse {pool_part} for pool_id"
                            );

                            continue;
                        };

                        let Ok(observed_id) = parts[6].parse::<Uuid>() else {
                            warn!(
                                self.log,
                                "could not parse {pool_part} for \
                                allocation_id"
                            );

                            continue;
                        };

                        // does the orphaned allocation id we're trying to
                        // garbage collect match this?

                        if observed_id == allocation_id {
                            vmm_is_using_orphaned_allocation = true;
                            break;
                        }
                    }

                    _ => {
                        // skip any other components
                    }
                }
            }

            if vmm_is_using_orphaned_allocation {
                // Cannot delete it! Make this orphaned allocation the one the
                // disk uses instead.

                println!(
                    r#"
vmm {vmm_id} is _currently_ using allocation {allocation_id} for disk {disk_id},
cannot garbage collect it!. Because this allocation is orphaned, when the
instance is incarnated as another VMM, that VMM will get a different
allocation for the disk, and it will appear like the previous data is lost.

omdb will make this orphaned allocation back the disk: this will orphan the one
currently set in the database (which is _not_ being used by the VMM), which will
then show up as never being used and can be garbage collected.
"#
                );

                // The VMM is running, so pass the sled id in

                Self::change_disk_allocation(
                    &conn,
                    disk_id,
                    &allocation,
                    Some(vmm.sled_id()),
                )
                .await?;
            } else {
                println!(
                    r#"
vmm {vmm_id} is not currently using allocation
{allocation_id} for disk {disk_id}, this is unexpected."#
                );

                // The VMM, uniquely identified by its UUID, is running, and we
                // extracted the allocation ID it would have been sent from the
                // instance-start saga. Not sure how we get here!

                bail!("unexpected state");
            }
        } else if vmm.state == VmmState::Destroyed {
            info!(self.log, "vmm {vmm_id} is destroyed");

            // If the VMM is `destroyed`, it was previously running! Omdb
            // can't figure out if the instance spec included this orphaned
            // allocation or not, it has to be done manually using logs.

            let pool_id = allocation.pool_id();
            let sled_id = allocation.sled_id();

            let sled = {
                use nexus_db_schema::schema::sled::dsl;

                dsl::sled
                    .filter(dsl::id.eq(to_db_typed_uuid(sled_id)))
                    .select(Sled::as_select())
                    .get_result_async(&*conn)
                    .await
                    .unwrap()
            };

            println!(
                r#"
WARNING: Deleting an orphaned allocation can result in customer data loss _if_ a
VMM started with the local storage details of the orphaned allocation instead of
the one currently assigned to the disk.

You should only proceed if

- This is a development system whose state can be wiped

- You have confirmed that the customer does not care about the data on the
  associated disk {} (id {}) which is currently in state {:?}.

If they do, at one point this disk was backed by this orphaned allocation, and
the VMM associated with the instance start saga could have been sent an
instance spec using said allocation instead of the one that the disk is using
now. Vmm {vmm_id} was destroyed, so we can't know for sure by asking it for
its instance spec.

To determine if this happened, search the logs for the propolis zone {vmm_id}
to check if an instance spec was sent that included a zvol path based on this
allocation. The sled hosting this zone is {} (id {sled_id}).

On there, run:

  /opt/oxide/oxlog/oxlog logs --current --archived \
    oxz_propolis-server_{vmm_id} | \
    xargs grep -h {allocation_id} | \
    grep 'Creating file disk backend' | \
    looker

If you see this message:

  INFO propolis-server (vm_state_driver): Creating file disk backend
    path = /dev/zvol/rdsk/oxp_{pool_id}/local_storage_unencrypted/{allocation_id}/vol

Then that VMM was using the orphaned allocation's zvol.
"#,
                disk.name(),
                disk.id(),
                disk.state(),
                sled.serial_number(),
            );

            if !interactive {
                bail!("bailing, not interactive command");
            }

            let mut prompt = ConfirmationPrompt::new();
            let answer = prompt.read("Do you see this? y/n: ")?;

            match answer.to_lowercase().as_str() {
                "y" => {
                    println!(
                        "Then the orphaned allocation was used for at least \
                        one VMM. Checking if this instance had any other VMMs \
                        now...",
                    );
                }

                "n" => {
                    println!(
                        r#"
This could be due to no propolis logs existing. Check if there are any stored
propolis logs:

  /opt/oxide/oxlog/oxlog logs --current --archived \
    oxz_propolis-server_{vmm_id}
"#
                    );

                    if !interactive {
                        bail!("bailing, not interactive command");
                    }

                    let mut prompt = ConfirmationPrompt::new();
                    let answer =
                        prompt.read("Do you see propolis logs? y/n: ")?;

                    match answer.to_lowercase().as_str() {
                        "y" => {
                            println!(
                                r#"
"Then this allocation was never used and can be deleted. Deleting now!"#
                            );

                            common::remove_local_storage_allocation(
                                &self.log,
                                &conn,
                                &allocation,
                            )
                            .await?;

                            return Ok(());
                        }

                        "n" => {
                            println!(
                                r#"
Then we can't be sure if the VMM used this disk, and you'll have to reach out to
the customer to decide what to do next."#
                            );

                            bail!("manual steps required");
                        }

                        _ => {
                            bail!(
                                "Aborting, input did not match expected value"
                            );
                        }
                    }
                }

                _ => {
                    bail!("Aborting, input did not match expected value");
                }
            }

            // How many other VMMs may have used this allocation? None, because
            // only one saga expected to be returned for the "find
            // instance-start sagas affected" query.
            //
            // But: if other VMMs were booted for this instance then the disk
            // would have appeared to lose data. Check that now:

            let other_vmms = {
                use nexus_db_schema::schema::vmm::dsl;

                dsl::vmm
                    .filter(dsl::time_created.gt(vmm.time_created))
                    .filter(dsl::instance_id.eq(vmm.instance_id))
                    // XXX more filters here?
                    .filter(dsl::state.ne(VmmState::SagaUnwound))
                    .select(Vmm::as_select())
                    .load_async(&*conn)
                    .await
                    .unwrap()
            };

            for other_vmm in &other_vmms {
                print!(
                    "found vmm {} in state {}, created {}",
                    other_vmm.id, other_vmm.state, other_vmm.time_created,
                );

                if let Some(time_deleted) = &other_vmm.time_deleted {
                    println!(" and deleted {}", time_deleted);
                } else {
                    println!();
                }
            }

            if !other_vmms.is_empty() {
                println!(
                    r#"
The associated instance had one or more new VMMs, and therefore a different
allocation will have been used for that local storage disk. For successively
booted VMMs, that disk will have been all blank, appearing to have lost data on
that disk.

The guest may have already written data to the current allocation. You'll have
to reach out to the customer to decide what to do next.
                    "#
                );

                bail!("manual steps required");
            } else {
                println!(
                    r#"
The associated instance has not had any other VMMs. omdb will change the local
storage disk's allocation so that it will match what was previously used, and
therefore will _not_ appear to have lost data. This orphans the other
allocation, which never will be used and can be cleaned up."#
                );

                if allocation.sled_id() != vmm.sled_id() {
                    println!(
                        r#"
Error: the orphaned allocation's sled, which should match the destroyed vmm's
sled id, does not. This is odd as the VMM was previously running, and we saw the
matching "Creating file disk backend" message! This is unexpected so omdb is
bailing out here."#
                    );

                    bail!(
                        "unexpected sled id mismatch: {} != {}",
                        allocation.sled_id(),
                        vmm.sled_id(),
                    );
                }

                // Update the local storage disk so that it points to this
                // orphaned allocation.

                Self::change_disk_allocation(
                    &conn,
                    disk_id,
                    &allocation,
                    Some(vmm.sled_id()),
                )
                .await?;
            }
        } else if vmm.state == VmmState::Failed {
            info!(self.log, "vmm {vmm_id} is failed");

            info!(
                self.log,
                "checking if instance failed to boot due to this issue"
            );

            // If the sled for this allocation does not match the VMM sled, it
            // will likely never have booted.

            if allocation.sled_id() != vmm.sled_id() {
                println!(
                    r#"
VMM {vmm_id} is in state failed. The orphaned allocation's sled does not match
the VMM sled {}. It's likely that this caused propolis to crash without running
the guest OS, meaning this allocation was never used."#,
                    vmm.sled_id(),
                );
            }

            // Make sure by searching the sled-agent logs

            let sled = {
                use nexus_db_schema::schema::sled::dsl;

                dsl::sled
                    .filter(dsl::id.eq(vmm.sled_id))
                    .select(Sled::as_select())
                    .get_result_async(&*conn)
                    .await
                    .unwrap()
            };

            println!(
                r#"
VMM {vmm_id} is in state failed. If propolis crashed without running the guest
OS, this orphaned allocation was never used, and can be deleted.

Check sled {} (id {}) relevant sled-agent log message to determine what
happened:

  /opt/oxide/oxlog/oxlog logs --current --archived global sled-agent | \
    xargs cat | \
    looker -c 'r.msg.contains("result of instance_ensure call is") && r.propolis_id == "{vmm_id}"'

If you see a message and that message contains:

    result of instance_ensure call is Err(Error Response: status: 500 Internal Server Error

Then the VMM never ran the guest.
"#,
                sled.serial_number(),
                sled.id(),
            );

            if !interactive {
                bail!("bailing, not interactive command");
            }

            let mut prompt = ConfirmationPrompt::new();
            let answer = prompt.read("Do you see this? y/n: ")?;

            match answer.to_lowercase().as_str() {
                "y" => {
                    println!(
                        "Then this allocation was never used by the guest OS, \
                        and can be deleted",
                    );
                }

                "n" => {
                    println!(
                        r#"
This could be due to failing to set up the propolis zone. Check for this:

  /opt/oxide/oxlog/oxlog logs --current --archived global sled-agent | \
    xargs cat | \
    looker -c 'r.msg.contains("failed to set up Propolis zone") && r.propolis_id == "{vmm_id}"'
"#
                    );

                    if !interactive {
                        bail!("bailing, not interactive command");
                    }

                    let mut prompt = ConfirmationPrompt::new();
                    let answer =
                        prompt.read("Do you see a failure message? y/n: ")?;

                    match answer.to_lowercase().as_str() {
                        "y" => {
                            println!(
                                "If the propolis zone was never set up, then this allocation can be deleted."
                            );

                            // fall through to cleaning up the allocation
                        }

                        "n" => {
                            // XXX find propolis logs, look for "VM successfully started"?

                            println!(
                                r#"
Then we can't be sure if the VMM used this disk, and you'll have to reach out to
the customer to decide what to do next."#
                            );

                            bail!("manual steps required");
                        }

                        _ => {
                            bail!(
                                "Aborting, input did not match expected value"
                            );
                        }
                    }
                }

                _ => {
                    bail!("Aborting, input did not match expected value");
                }
            }

            // Clean up the allocation
            common::remove_local_storage_allocation(
                &self.log,
                &conn,
                &allocation,
            )
            .await?;
        } else if vmm.state == VmmState::SagaUnwound {
            info!(self.log, "vmm {vmm_id} is saga_unwound");

            // This means the VMM never booted, and never could have used the
            // orphaned allocation. Remove it.

            println!(
                r#"
vmm {vmm_id} is in state SagaUnwound and therefore was never booted."#
            );

            common::remove_local_storage_allocation(
                &self.log,
                &conn,
                &allocation,
            )
            .await?;
        } else {
            bail!("unsure what to do about vmm in state {:?}", vmm.state);
        }

        println!("done!");

        Ok(())
    }
}

#[async_trait::async_trait]
impl DatabaseCorruption for OrphanedLocalStorageAllocations {
    async fn new(datastore: Arc<DataStore>, log: &Logger) -> Self {
        OrphanedLocalStorageAllocations {
            datastore,
            log: log.new(o!("check" => "orphaned_local_storage_allocations")),
        }
    }

    async fn detect(&self) -> Result<Vec<CorruptedStateInfo>> {
        let orphaned_record_ids =
            self.find_orphaned_local_storage_allocations().await?;

        Ok(orphaned_record_ids
            .into_iter()
            .map(|orphaned_record_id| CorruptedStateInfo {
                message: format!(
                    "unencrypted local storage allocation {} is an orphan",
                    orphaned_record_id,
                ),
            })
            .collect())
    }
}
