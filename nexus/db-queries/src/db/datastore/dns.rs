// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model::DnsGroup;
use crate::db::model::DnsName;
use crate::db::model::DnsVersion;
use crate::db::model::DnsZone;
use crate::db::model::Generation;
use crate::db::model::InitialDnsGroup;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_types::internal_api::params::DnsConfigParams;
use nexus_types::internal_api::params::DnsConfigZone;
use nexus_types::internal_api::params::DnsRecord;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::bail_unless;
use slog::debug;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::num::NonZeroU32;
use uuid::Uuid;

// This restriction could be removed by just implementing paginated reads.
const NMAX_DNS_ZONES: u32 = 10;

impl DataStore {
    /// List all DNS zones in a DNS group (paginated)
    pub async fn dns_zones_list(
        &self,
        opctx: &OpContext,
        dns_group: DnsGroup,
        pagparams: &DataPageParams<'_, String>,
    ) -> ListResultVec<DnsZone> {
        opctx.authorize(authz::Action::Read, &authz::DNS_CONFIG).await?;
        use nexus_db_schema::schema::dns_zone::dsl;
        paginated(dsl::dns_zone, dsl::zone_name, pagparams)
            .filter(dsl::dns_group.eq(dns_group))
            .select(DnsZone::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all DNS zones in a DNS group without pagination
    ///
    /// We do not generally expect there to be more than 1-2 DNS zones in a
    /// group (and nothing today creates more than one).
    pub async fn dns_zones_list_all(
        &self,
        opctx: &OpContext,
        dns_group: DnsGroup,
    ) -> ListResultVec<DnsZone> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Ok(self
            .dns_zones_list_all_on_connection(opctx, &conn, dns_group)
            .await?)
    }

    /// Variant of [`Self::dns_zones_list_all`] which may be called from a
    /// transaction context.
    pub(crate) async fn dns_zones_list_all_on_connection(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        dns_group: DnsGroup,
    ) -> Result<Vec<DnsZone>, TransactionError<Error>> {
        use nexus_db_schema::schema::dns_zone::dsl;
        const LIMIT: usize = 5;

        opctx.authorize(authz::Action::Read, &authz::DNS_CONFIG).await?;
        let list = dsl::dns_zone
            .filter(dsl::dns_group.eq(dns_group))
            .order(dsl::zone_name.asc())
            .limit(i64::try_from(LIMIT).unwrap())
            .select(DnsZone::as_select())
            .load_async(conn)
            .await?;

        bail_unless!(
            list.len() < LIMIT,
            "unexpectedly at least {} zones in DNS group {}",
            LIMIT,
            dns_group
        );
        Ok(list)
    }

    /// Get the latest version for a given DNS group
    pub async fn dns_group_latest_version(
        &self,
        opctx: &OpContext,
        dns_group: DnsGroup,
    ) -> LookupResult<DnsVersion> {
        let version = self
            .dns_group_latest_version_conn(
                opctx,
                &*self.pool_connection_authorized(opctx).await?,
                dns_group,
            )
            .await?;
        Ok(version)
    }

    pub async fn dns_group_latest_version_conn(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        dns_group: DnsGroup,
    ) -> Result<DnsVersion, TransactionError<Error>> {
        opctx.authorize(authz::Action::Read, &authz::DNS_CONFIG).await?;
        use nexus_db_schema::schema::dns_version::dsl;
        let versions = dsl::dns_version
            .filter(dsl::dns_group.eq(dns_group))
            .order_by(dsl::version.desc())
            .limit(1)
            .select(DnsVersion::as_select())
            .load_async(conn)
            .await?;

        bail_unless!(
            versions.len() == 1,
            "expected exactly one latest version for DNS group {:?}, found {}",
            dns_group,
            versions.len()
        );

        Ok(versions.into_iter().next().unwrap())
    }

    /// List all DNS names in the given DNS zone at the given group version
    /// (paginated)
    pub async fn dns_names_list(
        &self,
        opctx: &OpContext,
        dns_zone_id: Uuid,
        version: Generation,
        pagparams: &DataPageParams<'_, String>,
    ) -> ListResultVec<(String, Vec<DnsRecord>)> {
        opctx.authorize(authz::Action::Read, &authz::DNS_CONFIG).await?;
        use nexus_db_schema::schema::dns_name::dsl;
        Ok(paginated(dsl::dns_name, dsl::name, pagparams)
            .filter(dsl::dns_zone_id.eq(dns_zone_id))
            .filter(dsl::version_added.le(version))
            .filter(
                dsl::version_removed
                    .is_null()
                    .or(dsl::version_removed.gt(version)),
            )
            .select(DnsName::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .into_iter()
            .filter_map(|n: DnsName| match n.records() {
                Ok(records) => Some((n.name, records)),
                Err(error) => {
                    warn!(
                        opctx.log,
                        "failed to deserialize dns_name records: {:#}",
                        error;
                        "dns_zone_id" => n.dns_zone_id.to_string(),
                        "name" => n.name.to_string(),
                        "version_added" => n.version_added.to_string(),
                    );
                    None
                }
            })
            .collect())
    }

    /// Read the latest complete DNS configuration for a DNS group
    pub async fn dns_config_read(
        &self,
        opctx: &OpContext,
        dns_group: DnsGroup,
    ) -> Result<DnsConfigParams, Error> {
        let log = opctx.log.new(o!("dns_group" => dns_group.to_string()));

        debug!(log, "reading DNS version");
        let version = self.dns_group_latest_version(opctx, dns_group).await?;
        debug!(log, "found DNS version";
            "version" => version.version.to_string()
        );
        assert_eq!(version.dns_group, dns_group);

        self.dns_config_read_version(
            opctx,
            &log,
            NonZeroU32::new(100).unwrap(),
            &version,
        )
        .await
    }

    /// Private helper for reading a specific version of a group's DNS config
    async fn dns_config_read_version(
        &self,
        opctx: &OpContext,
        log: &slog::Logger,
        batch_size: NonZeroU32,
        version: &DnsVersion,
    ) -> Result<DnsConfigParams, Error> {
        debug!(log, "reading DNS config");
        debug!(log, "reading DNS zones");
        let dns_group = version.dns_group;
        let dns_zones = self
            .dns_zones_list(
                opctx,
                dns_group,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: NonZeroU32::try_from(NMAX_DNS_ZONES).unwrap(),
                },
            )
            .await
            .with_internal_context(|| {
                format!("listing internal zones for DNS group {:?}", dns_group)
            })?;
        debug!(log, "found zones"; "count" => dns_zones.len());

        bail_unless!(
            dns_zones.len() < usize::try_from(NMAX_DNS_ZONES).unwrap()
        );

        let mut zones = Vec::with_capacity(dns_zones.len());
        for zone in dns_zones {
            let mut zone_records = Vec::new();
            let mut paginator = Paginator::new(batch_size);
            while let Some(p) = paginator.next() {
                debug!(log, "listing DNS names for zone";
                    "dns_zone_id" => zone.id.to_string(),
                    "dns_zone_name" => &zone.zone_name,
                    "version" => i64::from(&version.version.0),
                    "found_so_far" => zone_records.len(),
                    "batch_size" => batch_size.get(),
                );
                let names_batch = self
                    .dns_names_list(
                        opctx,
                        zone.id,
                        version.version,
                        &p.current_pagparams(),
                    )
                    .await?;
                paginator = p.found_batch(&names_batch, &|(n, _)| n.clone());
                zone_records.extend(names_batch.into_iter());
            }

            debug!(log, "found all DNS names for zone";
                "dns_zone_id" => zone.id.to_string(),
                "dns_zone_name" => &zone.zone_name,
                "version" => i64::from(&version.version.0),
                "found_so_far" => zone_records.len(),
            );

            if !zone_records.is_empty() {
                zones.push(DnsConfigZone {
                    zone_name: zone.zone_name,
                    records: zone_records.into_iter().collect(),
                });
            }
        }

        debug!(log, "read DNS config";
            "version" => i64::from(&version.version.0),
            "nzones" => zones.len()
        );

        Ok(DnsConfigParams {
            generation: version.version.0,
            time_created: version.time_created,
            zones,
        })
    }

    /// Load initial data for a DNS group into the database
    pub async fn load_dns_data(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        dns: InitialDnsGroup,
    ) -> Result<(), Error> {
        {
            use nexus_db_schema::schema::dns_zone::dsl;
            diesel::insert_into(dsl::dns_zone)
                .values(dns.row_for_zone())
                .on_conflict((dsl::dns_group, dsl::zone_name))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        }

        {
            use nexus_db_schema::schema::dns_version::dsl;
            diesel::insert_into(dsl::dns_version)
                .values(dns.row_for_version())
                .on_conflict((dsl::dns_group, dsl::version))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        }

        {
            use nexus_db_schema::schema::dns_name::dsl;
            diesel::insert_into(dsl::dns_name)
                .values(dns.rows_for_names()?)
                .on_conflict((dsl::dns_zone_id, dsl::version_added, dsl::name))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        }

        Ok(())
    }

    /// Update the configuration of a DNS zone as specified in `update`,
    /// conditional on the _current_ DNS version being `old_version`.
    ///
    /// Unlike `dns_update_incremental()`, this function assumes the caller has
    /// already constructed `update` based on a specific DNS version
    /// (`old_version`) and only wants to apply these changes if the DNS version
    /// in the database has not changed.
    ///
    /// Also unlike `dns_update_incremental()`, this function creates its own
    /// transaction to apply the update.
    ///
    /// Like `dns_update_incremental()`, **callers almost certainly want to wake
    /// up the corresponding Nexus background task to cause these changes to be
    /// propagated to the corresponding DNS servers.**
    pub async fn dns_update_from_version(
        &self,
        opctx: &OpContext,
        update: DnsVersionUpdateBuilder,
        old_version: Generation,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::DNS_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("dns_update_from_version")
            .transaction(&conn, |c| {
                let err = err.clone();
                let update = update.clone();
                async move {
                    let zones = self
                        .dns_zones_list_all_on_connection(opctx, &c, update.dns_group)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    // This looks like a time-of-check-to-time-of-use race, but this
                    // approach works because we're inside a transaction and the
                    // isolation level is SERIALIZABLE.
                    let version = self
                        .dns_group_latest_version_conn(opctx, &c, update.dns_group)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    if version.version != old_version {
                        return Err(err.bail(TransactionError::CustomError(Error::conflict(
                            format!(
                                "expected current DNS version to be {}, found {}",
                                *old_version, *version.version,
                            ),
                        ))));
                    }

                    self.dns_write_version_internal(
                        &c,
                        update,
                        zones,
                        Generation(old_version.next()),
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// Update the configuration of a DNS zone as specified in `update`
    ///
    /// Unlike `dns_update_from_version()`, this function assumes that the
    /// caller's changes are valid regardless of the current DNS configuration.
    /// This function fetches the latest version and always writes the updated
    /// config as the next version.  This is appropriate if the caller is adding
    /// something wholly new or removing something that it knows should be
    /// present (as in the case when we add or remove DNS names for a Silo,
    /// since we control exactly when that happens).  This is _not_ appropriate
    /// if the caller is making arbitrary changes that might conflict with a
    /// concurrent caller.  For that, you probably want
    /// `dns_update_from_version()`.
    ///
    /// This function runs the body inside a transaction (if no transaction is
    /// open) or a nested transaction (savepoint, if a transaction is already
    /// open).  Generally, the caller should invoke this function while already
    /// inside a transaction so that the DNS changes happen if and only if the
    /// rest of the transaction also happens.  The caller's transaction should
    /// be aborted if this function fails, lest the operation succeed with DNS
    /// not updated.
    ///
    /// It's recommended to put this step last in any transaction because the
    /// more time elapses between running this function and attempting to commit
    /// the transaction, the greater the chance of either transaction failure
    /// due to a conflict error (if some other caller attempts to update the
    /// same DNS group) or another client blocking (for the same reason).
    ///
    /// **Callers almost certainly want to wake up the corresponding Nexus
    /// background task to cause these changes to be propagated to the
    /// corresponding DNS servers.**
    pub async fn dns_update_incremental(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        update: DnsVersionUpdateBuilder,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::DNS_CONFIG).await?;

        let zones = self
            .dns_zones_list_all_on_connection(opctx, conn, update.dns_group)
            .await?;

        // This method is used in nested transactions, which are not supported
        // with retryable transactions.
        self.transaction_non_retry_wrapper("dns_update_incremental")
            .transaction(&conn, |c| async move {
                let version = self
                    .dns_group_latest_version_conn(
                        opctx,
                        conn,
                        update.dns_group,
                    )
                    .await?;
                self.dns_write_version_internal(
                    &c,
                    update,
                    zones,
                    Generation(version.version.next()),
                )
                .await
            })
            .await
    }

    // This must only be used inside a transaction.  Otherwise, it may make
    // invalid changes to the database state.  Use one of the `dns_update_*()`
    // functions instead.
    //
    // The caller should already have checked (in the same transaction) that
    // their version number is the correct next version.
    async fn dns_write_version_internal(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        update: DnsVersionUpdateBuilder,
        zones: Vec<DnsZone>,
        new_version_num: Generation,
    ) -> Result<(), TransactionError<Error>> {
        // TODO-scalability TODO-performance This would be much better as a CTE
        // for all the usual reasons described in RFD 192.  Using an interactive
        // transaction here means that either we wind up holding database locks
        // while executing code on the client (resulting in latency bubbles for
        // other clients) or else the database invalidates our transaction if
        // there's a conflict (which increases the likelihood that these
        // operations fail spuriously as far as the client is concerned).  We
        // expect these problems to be small or unlikely at small scale but
        // significant as the system scales up.
        let new_version = DnsVersion {
            dns_group: update.dns_group,
            version: new_version_num,
            time_created: chrono::Utc::now(),
            creator: update.creator,
            comment: update.comment,
        };

        let dns_zone_ids: Vec<_> = zones.iter().map(|z| z.id).collect();
        let new_names = update
            .names_added
            .into_iter()
            .flat_map(|(name, records)| {
                dns_zone_ids.iter().map(move |dns_zone_id| {
                    DnsName::new(
                        *dns_zone_id,
                        name.clone(),
                        new_version_num,
                        None,
                        records.clone(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ntoadd = new_names.len();

        {
            use nexus_db_schema::schema::dns_version::dsl;
            diesel::insert_into(dsl::dns_version)
                .values(new_version)
                .execute_async(conn)
                .await?;
        }

        {
            use nexus_db_schema::schema::dns_name::dsl;

            // Remove any names that we're removing first.  This is important,
            // as the database will enforce a constraint that the same name not
            // appear live (i.e., with version_removed IS NULL) in two different
            // versions.  If someone is adding *and* removing a name in this
            // update, we would (temporarily) violate that constraint if we did
            // this in the other order.
            let to_remove = update.names_removed;
            let ntoremove = to_remove.len() * dns_zone_ids.len();
            let nremoved = diesel::update(
                dsl::dns_name
                    .filter(dsl::dns_zone_id.eq_any(dns_zone_ids))
                    .filter(dsl::name.eq_any(to_remove))
                    .filter(dsl::version_removed.is_null()),
            )
            .set(dsl::version_removed.eq(new_version_num))
            .execute_async(conn)
            .await?;

            bail_unless!(
                nremoved == ntoremove,
                "updated wrong number of dns_name records: expected {}, \
                actually marked {} for removal",
                ntoremove,
                nremoved
            );

            // Now add any names being added.
            let nadded = diesel::insert_into(dsl::dns_name)
                .values(new_names)
                .execute_async(conn)
                .await?;

            bail_unless!(
                nadded == ntoadd,
                "inserted wrong number of dns_name records: expected {}, \
                actually inserted {}",
                ntoadd,
                nadded
            );
        }

        Ok(())
    }
}

/// Helper for changing the configuration of all the DNS zone in a DNS group
///
/// A DNS zone's configuration consists of the DNS names in the zone and their
/// associated DNS records.  Any change to the zone configuration consists of a
/// set of names added (with associated records) and a set of names removed
/// (which removes all records for the corresponding names).  If you want to
/// _change_ the records associated with a name, you first remove what's there
/// and add the name back with different records.
///
/// You use this object to build up a _description_ of the changes to the DNS
/// zone's configuration.  Then you call [`DataStore::dns_update_incremental()`]
/// or [`DataStore::dns_update_from_version()`] to apply these changes
/// transactionally to the database.  The changes are then propagated
/// asynchronously to the DNS servers.  No changes are made (to either the
/// database or the DNS servers) while you modify this object.
///
/// This object changes all of the zones associated with a particular DNS group
/// because the assumption right now is that they're equivalent.  (In practice,
/// we should only ever have one zone in each group right now.)
#[derive(Clone, Debug)]
pub struct DnsVersionUpdateBuilder {
    dns_group: DnsGroup,
    comment: String,
    creator: String,
    names_added: HashMap<String, Vec<DnsRecord>>,
    names_removed: HashSet<String>,
}

impl DnsVersionUpdateBuilder {
    /// Begin describing a new change to the given DNS group
    ///
    /// `comment` is a short text summary of why this change is being made,
    /// aimed at people looking through the change history while debugging.
    /// Example: `"silo create: 'my-silo'"` tells us that the DNS change is
    /// being made because a new Silo called `my-silo` is being created.
    ///
    /// "creator" describes the component making the change. This is generally
    /// the current Nexus instance's uuid.
    pub fn new(
        dns_group: DnsGroup,
        comment: String,
        creator: String,
    ) -> DnsVersionUpdateBuilder {
        DnsVersionUpdateBuilder {
            dns_group,
            comment,
            creator,
            names_added: HashMap::new(),
            names_removed: HashSet::new(),
        }
    }

    /// Record that the DNS name `name` is being added to the zone with the
    /// corresponding set of records
    ///
    /// `name` must not already exist in the zone unless you're also calling
    /// `remove_name()` with the same name (meaning that you're replacing the
    /// records for that `name`).  It's expected that callers know precisely
    /// which names need to be added when making a change.  For example, when
    /// creating a Silo, we expect that the Silo's DNS name does not already
    /// exist.
    ///
    /// `name` should not contain the suffix of the DNS zone itself.  For
    /// example, if the DNS zone is `control-plane.oxide.internal` and we want
    /// to have `nexus.control-plane.oxide.internal` show up in DNS, the name
    /// here would just be `nexus`.
    ///
    /// `name` can contain multiple labels (e.g., `foo.bar`).
    pub fn add_name(
        &mut self,
        name: String,
        records: Vec<DnsRecord>,
    ) -> Result<(), Error> {
        match self.names_added.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(records);
                Ok(())
            }
            Entry::Occupied(entry) => Err(Error::internal_error(&format!(
                "DNS update ({:?}) attempted to add name {:?} multiple times",
                self.comment,
                entry.key()
            ))),
        }
    }

    /// Record that the DNS name `name` and all of its associated records are
    /// being removed from the zone
    ///
    /// `name` must already be in the zone.  It's expected that callers know
    /// precisely which names need to be removed when making a change.  For
    /// example, when deleting a Silo, we expect that the Silo's DNS name is
    /// present.
    ///
    /// See `add_name(name)` for more about other expectations about `name`,
    /// like that it may contain multiple labels and that it should not include
    /// the DNS zone suffix.
    pub fn remove_name(&mut self, name: String) -> Result<(), Error> {
        if self.names_removed.contains(&name) {
            Err(Error::internal_error(&format!(
                "DNS update ({:?}) attempted to remove name {:?} \
                multiple times",
                self.comment, &name,
            )))
        } else {
            assert!(self.names_removed.insert(name));
            Ok(())
        }
    }

    pub fn names_removed(&self) -> impl Iterator<Item = &str> {
        self.names_removed.iter().map(AsRef::as_ref)
    }

    pub fn names_added(&self) -> impl Iterator<Item = (&str, &[DnsRecord])> {
        self.names_added
            .iter()
            .map(|(name, list)| (name.as_ref(), list.as_ref()))
    }
}

/// Extra interfaces that are not intended for use in Nexus, but useful for
/// testing and `omdb`
pub trait DataStoreDnsTest: Send + Sync {
    /// Fetch the DNS configuration for a specific group and version
    fn dns_config_read_version<'a>(
        &'a self,
        opctx: &'a OpContext,
        dns_group: DnsGroup,
        version: omicron_common::api::external::Generation,
    ) -> BoxFuture<'a, Result<DnsConfigParams, Error>>;
}

impl DataStoreDnsTest for DataStore {
    fn dns_config_read_version<'a>(
        &'a self,
        opctx: &'a OpContext,
        dns_group: DnsGroup,
        version: omicron_common::api::external::Generation,
    ) -> BoxFuture<'a, Result<DnsConfigParams, Error>> {
        async move {
            use nexus_db_schema::schema::dns_version::dsl;
            let dns_version = dsl::dns_version
                .filter(dsl::dns_group.eq(dns_group))
                .filter(dsl::version.eq(Generation::from(version)))
                .select(DnsVersion::as_select())
                .first_async(&*self.pool_connection_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    // Technically, we could produce a `NotFound` error here.
                    // But since this is only for testing, it's okay to produce
                    // an InternalError.
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            self.dns_config_read_version(
                opctx,
                &opctx.log,
                SQL_BATCH_SIZE,
                &dns_version,
            )
            .await
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::db::DataStore;
    use crate::db::datastore::DnsVersionUpdateBuilder;
    use crate::db::pub_test_utils::TestDatabase;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncConnection;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use futures::FutureExt;
    use nexus_db_errors::TransactionError;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::DnsName;
    use nexus_db_model::DnsVersion;
    use nexus_db_model::DnsZone;
    use nexus_db_model::Generation;
    use nexus_db_model::InitialDnsGroup;
    use nexus_types::internal_api::params::DnsRecord;
    use nexus_types::internal_api::params::Srv;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use std::collections::HashMap;
    use std::net::Ipv6Addr;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    // Tests reading various uninitialized or partially-initialized DNS data
    #[tokio::test]
    async fn test_read_dns_config_uninitialized() {
        let logctx = dev::test_setup_log("test_read_dns_config_uninitialized");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // If we attempt to load the config when literally nothing related to
        // DNS has been initialized, we will get an InternalError because we
        // cannot tell what version we're supposed to be at.
        let error = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect_err(
                "unexpectedly succeeding reading uninitialized DNS config",
            );
        println!("found error: {:?}", error);
        assert_matches!(
            error,
            Error::InternalError { internal_message, .. } if
                internal_message == "expected exactly one latest \
                    version for DNS group Internal, found 0"
        );

        // Now insert a version with no zones.  This shouldn't happen for real,
        // but it's worth testing that we do something reasonable here.
        let now = Utc::now();
        {
            use crate::db::model::DnsVersion;
            use nexus_db_schema::schema::dns_version::dsl;
            use omicron_common::api::external::Generation;

            diesel::insert_into(dsl::dns_version)
                .values(DnsVersion {
                    dns_group: DnsGroup::Internal,
                    version: Generation::new().into(),
                    time_created: now,
                    creator: "test suite".to_string(),
                    comment: "test suite".to_string(),
                })
                .execute_async(
                    &*datastore
                        .pool_connection_for_tests()
                        .await
                        .expect("failed to get datastore connection"),
                )
                .await
                .expect("failed to insert initial version");
        }
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("failed to read DNS config");
        println!("found config: {:?}", dns_config);
        assert_eq!(u64::from(dns_config.generation), 1);
        // A round-trip through the database reduces the precision of the
        // "time_created" value.
        assert_eq!(
            dns_config.time_created.signed_duration_since(now).num_seconds(),
            0
        );
        assert_eq!(dns_config.zones.len(), 0);

        // Note that the version we just created and tested was specific to the
        // "Internal" DNS group.  If we read the config for the "External" DNS
        // group, we should get the same error as above.
        let error = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect_err(
                "unexpectedly succeeding reading uninitialized DNS config",
            );
        println!("found error: {:?}", error);
        assert_matches!(
            error,
            Error::InternalError { internal_message, .. } if
                internal_message == "expected exactly one latest \
                    version for DNS group External, found 0"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests a very simple configuration of DNS data
    #[tokio::test]
    async fn test_read_dns_config_basic() {
        let logctx = dev::test_setup_log("test_read_dns_config_basic");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create exactly one zone with no names in it.
        // This will not show up in the read config.
        let before = Utc::now();
        let initial = InitialDnsGroup::new(
            DnsGroup::External,
            "dummy.oxide.test",
            "test suite",
            "test suite",
            HashMap::new(),
        );
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            DataStore::load_dns_data(&conn, initial)
                .await
                .expect("failed to load initial DNS zone");
        }

        let after = Utc::now();
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect("failed to read DNS config");
        println!("found config: {:?}", dns_config);
        assert_eq!(u64::from(dns_config.generation), 1);
        assert!(dns_config.time_created >= before);
        assert!(dns_config.time_created <= after);
        assert_eq!(dns_config.zones.len(), 0);

        // Create a zone with a few names in it.
        let before = Utc::now();
        let wendell_records = vec![
            DnsRecord::Aaaa("fe80::2:3".parse().unwrap()),
            DnsRecord::Aaaa("fe80::2:4".parse().unwrap()),
        ];
        let krabappel_records = vec![DnsRecord::Srv(Srv {
            weight: 0,
            prio: 0,
            port: 12345,
            target: "wendell.dummy.oxide.internal".to_string(),
        })];
        let initial = InitialDnsGroup::new(
            DnsGroup::Internal,
            "dummy.oxide.internal",
            "test suite",
            "test suite",
            HashMap::from([
                ("wendell".to_string(), wendell_records.clone()),
                ("krabappel".to_string(), krabappel_records.clone()),
            ]),
        );
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            DataStore::load_dns_data(&conn, initial)
                .await
                .expect("failed to load initial DNS zone");
        }

        let after = Utc::now();
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("failed to read DNS config");
        println!("found config: {:?}", dns_config);
        assert_eq!(u64::from(dns_config.generation), 1);
        assert!(dns_config.time_created >= before);
        assert!(dns_config.time_created <= after);
        assert_eq!(dns_config.zones.len(), 1);
        assert_eq!(dns_config.zones[0].zone_name, "dummy.oxide.internal");
        assert_eq!(
            dns_config.zones[0].records,
            HashMap::from([
                ("krabappel".to_string(), krabappel_records),
                ("wendell".to_string(), wendell_records)
            ])
        );

        // Do this again, but controlling the batch size to make sure pagination
        // works right.
        let dns_config_batch_1 = datastore
            .dns_config_read_version(
                &opctx,
                &opctx.log.clone(),
                NonZeroU32::new(1).unwrap(),
                &DnsVersion {
                    dns_group: DnsGroup::Internal,
                    version: Generation(1u32.try_into().unwrap()),
                    time_created: dns_config.time_created,
                    creator: "unused".to_string(),
                    comment: "unused".to_string(),
                },
            )
            .await
            .expect("failed to read DNS config with batch size 1");
        assert_eq!(dns_config_batch_1, dns_config);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests a complex configuration of DNS data (see comment below)
    #[tokio::test]
    async fn test_read_dns_config_complex() {
        let logctx = dev::test_setup_log("test_read_dns_config_complex");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let batch_size = NonZeroU32::new(10).unwrap();
        let now = Utc::now();
        let log = &logctx.log;

        // Construct a more complex configuration:
        //
        // - both DNS groups in use
        // - multiple zones in each DNS group
        // - multiple versions for each DNS group
        // - the multiple versions add and remove some zones
        // - the multiple versions add, remove, and change names in some zones
        //
        // Here's the broad plan:
        //
        // "external" group:
        // - zone "z1.foo" (versions 1 and 2) [tests removing a zone]
        //   - v1: name "n1", name "n2"
        //   - v2: name "n1", "n3" [tests adding and removing a record]
        // - zone "z2.foo" (versions 2 and 3) [tests adding a zone]
        //   - v2: name "n1" with one record
        //   - v3: name "n1" with a different record [tests changing one rec]
        // - zone "z3.bar" (versions 1-3)
        //   - v1: name "n1" with one record
        //   - v2: name "n1" with a second record [tests change a record]
        //         name "n2" with a record [tests adding a record]
        //   - v3: name "n2" with same record [tests removal of a record]
        // "internal" group:
        // - version 1 has no zones
        // - version 2 has zone "z1.foo" with name "z1n1" with a different
        //   record than in the other DNS group
        //   [tests that the two groups' zones are truly separate]
        //
        // Reusing the names across zones and reusing the zone names across
        // groups tests that these containers are really treated separately.
        //
        // It's important that we load all of the data up front and then fetch
        // each version's contents (rather than load one version's data, fetch
        // the latest contents, move on to the next version, etc.).  That
        // ensures that the underlying queries present a correct picture of a
        // given version even when a newer version is being written.

        let z1_id = Uuid::new_v4();
        let z2_id = Uuid::new_v4();
        let z3_id = Uuid::new_v4();
        let zinternal_id = Uuid::new_v4();
        let g1 = Generation(1u32.try_into().unwrap());
        let g2 = Generation(2u32.try_into().unwrap());
        let g3 = Generation(3u32.try_into().unwrap());
        let v1 = DnsVersion {
            dns_group: DnsGroup::External,
            version: g1,
            time_created: now,
            creator: "test suite".to_string(),
            comment: "test suite".to_string(),
        };
        let v2 = DnsVersion {
            dns_group: DnsGroup::External,
            version: g2,
            time_created: now,
            creator: "test suite".to_string(),
            comment: "test suite".to_string(),
        };
        let v3 = DnsVersion {
            dns_group: DnsGroup::External,
            version: g3,
            time_created: now,
            creator: "test suite".to_string(),
            comment: "test suite".to_string(),
        };
        let vi1 = DnsVersion {
            dns_group: DnsGroup::Internal,
            version: g1,
            time_created: now,
            creator: "test suite".to_string(),
            comment: "test suite".to_string(),
        };
        let vi2 = DnsVersion {
            dns_group: DnsGroup::Internal,
            version: g2,
            time_created: now,
            creator: "test suite".to_string(),
            comment: "test suite".to_string(),
        };

        let r1 = DnsRecord::Aaaa("fe80::1:2:3:4".parse().unwrap());
        let r2 = DnsRecord::Aaaa("fe80::1:1:1:1".parse().unwrap());
        let records_r1 = vec![r1.clone()];
        let records_r2 = vec![r2.clone()];
        let records_r1r2 = vec![r1, r2];

        // Set up the database state exactly as we want it.
        // First, insert the DNS zones.
        {
            use nexus_db_schema::schema::dns_zone::dsl;
            diesel::insert_into(dsl::dns_zone)
                .values(vec![
                    DnsZone {
                        id: z1_id,
                        time_created: now,
                        dns_group: DnsGroup::External,
                        zone_name: "z1.foo".to_string(),
                    },
                    DnsZone {
                        id: z2_id,
                        time_created: now,
                        dns_group: DnsGroup::External,
                        zone_name: "z2.foo".to_string(),
                    },
                    DnsZone {
                        id: z3_id,
                        time_created: now,
                        dns_group: DnsGroup::External,
                        zone_name: "z3.bar".to_string(),
                    },
                    DnsZone {
                        id: zinternal_id,
                        time_created: now,
                        dns_group: DnsGroup::Internal,
                        // Zone name deliberately overlaps one in External group
                        zone_name: "z1.foo".to_string(),
                    },
                ])
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap();
        }

        // Next, insert the DNS versions.
        {
            use nexus_db_schema::schema::dns_version::dsl;
            diesel::insert_into(dsl::dns_version)
                .values(vec![
                    v1.clone(),
                    v2.clone(),
                    v3.clone(),
                    vi1.clone(),
                    vi2.clone(),
                ])
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap();
        }

        // Finally, insert all DNS names for all versions of all zones.
        {
            use nexus_db_schema::schema::dns_name::dsl;
            diesel::insert_into(dsl::dns_name)
                .values(vec![
                    // External zone "z1" records test that:
                    // - name "n1" lasts multiple generations
                    // - name "n2" removed between generations
                    // - name "n3" added between generations
                    // - a zone is removed between generations
                    DnsName::new(
                        z1_id,
                        "n1".to_string(),
                        g1,
                        Some(g3),
                        records_r1.clone(),
                    )
                    .unwrap(),
                    DnsName::new(
                        z1_id,
                        "n2".to_string(),
                        g1,
                        Some(g2),
                        records_r1.clone(),
                    )
                    .unwrap(),
                    DnsName::new(
                        z1_id,
                        "n3".to_string(),
                        g2,
                        Some(g3),
                        records_r1.clone(),
                    )
                    .unwrap(),
                    // External zone "z2" records test that:
                    // - we add a zone between generation
                    // - a record ("n1") changes between gen 2 and gen 3
                    DnsName::new(
                        z2_id,
                        "n1".to_string(),
                        g2,
                        Some(g3),
                        records_r1.clone(),
                    )
                    .unwrap(),
                    DnsName::new(
                        z2_id,
                        "n1".to_string(),
                        g3,
                        None,
                        records_r2.clone(),
                    )
                    .unwrap(),
                    // External zone "z3" records test that:
                    // - a zone exists in all generations
                    // - a record ("n1") changes between generations
                    // - a record ("n2") is added between generations
                    // - a record ("n1") is removed between generations
                    // Using the same names in different zones ensures these are
                    // treated separately.
                    DnsName::new(
                        z3_id,
                        "n1".to_string(),
                        g1,
                        Some(g2),
                        records_r2.clone(),
                    )
                    .unwrap(),
                    DnsName::new(
                        z3_id,
                        "n1".to_string(),
                        g2,
                        Some(g3),
                        records_r1r2.clone(),
                    )
                    .unwrap(),
                    DnsName::new(
                        z3_id,
                        "n2".to_string(),
                        g2,
                        None,
                        records_r2.clone(),
                    )
                    .unwrap(),
                    // Internal zone records test that the namespaces are
                    // orthogonal across different DNS groups.
                    DnsName::new(
                        zinternal_id,
                        "n1".to_string(),
                        g2,
                        None,
                        records_r2.clone(),
                    )
                    .unwrap(),
                ])
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap();
        }

        // Now, read back the state (using the function we're testing) for each
        // version of each DNS group that we wrote.

        // Verify external version 1.
        let dns_config_v1 = datastore
            .dns_config_read_version(&opctx, log, batch_size, &v1)
            .await
            .unwrap();
        println!("dns_config_v1: {:?}", dns_config_v1);
        assert_eq!(u64::from(dns_config_v1.generation), 1);
        assert_eq!(dns_config_v1.zones.len(), 2);
        assert_eq!(dns_config_v1.zones[0].zone_name, "z1.foo");
        assert_eq!(
            dns_config_v1.zones[0].records,
            HashMap::from([
                ("n1".to_string(), records_r1.clone()),
                ("n2".to_string(), records_r1.clone())
            ])
        );
        assert_eq!(dns_config_v1.zones[1].zone_name, "z3.bar");
        assert_eq!(
            dns_config_v1.zones[1].records,
            HashMap::from([("n1".to_string(), records_r2.clone())])
        );

        // Verify external version 2.
        let dns_config_v2 = datastore
            .dns_config_read_version(&opctx, log, batch_size, &v2)
            .await
            .unwrap();
        println!("dns_config_v2: {:?}", dns_config_v2);
        assert_eq!(u64::from(dns_config_v2.generation), 2);
        assert_eq!(dns_config_v2.zones.len(), 3);
        assert_eq!(dns_config_v2.zones[0].zone_name, "z1.foo");
        assert_eq!(
            dns_config_v2.zones[0].records,
            HashMap::from([
                ("n1".to_string(), records_r1.clone()),
                ("n3".to_string(), records_r1.clone())
            ])
        );

        assert_eq!(dns_config_v2.zones[1].zone_name, "z2.foo");
        assert_eq!(dns_config_v2.zones[1].records.len(), 1);
        assert_eq!(
            dns_config_v2.zones[1].records,
            HashMap::from([("n1".to_string(), records_r1.clone())])
        );

        assert_eq!(dns_config_v2.zones[2].zone_name, "z3.bar");
        assert_eq!(
            dns_config_v2.zones[2].records,
            HashMap::from([
                ("n1".to_string(), records_r1r2.clone()),
                ("n2".to_string(), records_r2.clone())
            ])
        );

        // Verify external version 3
        let dns_config_v3 = datastore
            .dns_config_read_version(&opctx, log, batch_size, &v3)
            .await
            .unwrap();
        println!("dns_config_v3: {:?}", dns_config_v3);
        assert_eq!(u64::from(dns_config_v3.generation), 3);
        assert_eq!(dns_config_v3.zones.len(), 2);
        assert_eq!(dns_config_v3.zones[0].zone_name, "z2.foo");
        assert_eq!(
            dns_config_v3.zones[0].records,
            HashMap::from([("n1".to_string(), records_r2.clone())])
        );
        assert_eq!(dns_config_v3.zones[1].zone_name, "z3.bar");
        assert_eq!(
            dns_config_v3.zones[1].records,
            HashMap::from([("n2".to_string(), records_r2.clone())])
        );

        // Without specifying a version, we should get v3.
        let dns_config_latest = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        // Note that the time_created doesn't quite match up here because in
        // `dns_config_latest`, we took a round-trip through the database, which
        // loses some precision.
        assert_eq!(dns_config_v3.generation, dns_config_latest.generation);
        assert_eq!(dns_config_v3.zones, dns_config_latest.zones);

        // Verify internal version 1.
        let internal_dns_config_v1 = datastore
            .dns_config_read_version(&opctx, log, batch_size, &vi1)
            .await
            .unwrap();
        println!("internal dns_config_v1: {:?}", internal_dns_config_v1);
        assert_eq!(u64::from(internal_dns_config_v1.generation), 1);
        assert_eq!(internal_dns_config_v1.zones.len(), 0);

        // Verify internal version 2.
        let internal_dns_config_v2 = datastore
            .dns_config_read_version(&opctx, log, batch_size, &vi2)
            .await
            .unwrap();
        println!("internal dns_config_v2: {:?}", internal_dns_config_v2);
        assert_eq!(u64::from(internal_dns_config_v2.generation), 2);
        assert_eq!(internal_dns_config_v2.zones.len(), 1);
        assert_eq!(internal_dns_config_v2.zones[0].zone_name, "z1.foo");
        assert_eq!(
            internal_dns_config_v2.zones[0].records,
            HashMap::from([("n1".to_string(), records_r2.clone())])
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests the unique indexes enforced by the database.
    #[tokio::test]
    async fn test_dns_uniqueness() {
        let logctx = dev::test_setup_log("test_dns_uniqueness");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let now = Utc::now();

        // There cannot be two DNS zones in the same group with the same name.
        {
            use nexus_db_schema::schema::dns_zone::dsl;
            let error = diesel::insert_into(dsl::dns_zone)
                .values(vec![
                    DnsZone {
                        id: Uuid::new_v4(),
                        time_created: now,
                        dns_group: DnsGroup::External,
                        zone_name: "z1.foo".to_string(),
                    },
                    DnsZone {
                        id: Uuid::new_v4(),
                        time_created: now,
                        dns_group: DnsGroup::External,
                        zone_name: "z1.foo".to_string(),
                    },
                ])
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("duplicate key value violates unique constraint")
            );
        }

        // There cannot be two DNS version records with the same group and
        // version number.
        {
            use nexus_db_schema::schema::dns_version::dsl;
            let error = diesel::insert_into(dsl::dns_version)
                .values(vec![
                    DnsVersion {
                        dns_group: DnsGroup::Internal,
                        version: Generation::new(),
                        time_created: now,
                        creator: "test suite 1".to_string(),
                        comment: "test suite 2".to_string(),
                    },
                    DnsVersion {
                        dns_group: DnsGroup::Internal,
                        version: Generation::new(),
                        time_created: now,
                        creator: "test suite 3".to_string(),
                        comment: "test suite 4".to_string(),
                    },
                ])
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("duplicate key value violates unique constraint")
            );
        }

        // There cannot be two DNS names in the same zone with the same name
        // created in the same generation.
        {
            use nexus_db_schema::schema::dns_name::dsl;
            let dns_zone_id = Uuid::new_v4();
            let name = "n1".to_string();
            let g1 = Generation(1u32.try_into().unwrap());
            let g2 = Generation(2u32.try_into().unwrap());
            let r1 = DnsRecord::Aaaa("fe80::1:2:3:4".parse().unwrap());
            let r2 = DnsRecord::Aaaa("fe80::1:1:1:1".parse().unwrap());

            let error = diesel::insert_into(dsl::dns_name)
                .values(vec![
                    DnsName::new(dns_zone_id, name.clone(), g1, None, vec![r1])
                        .unwrap(),
                    DnsName::new(
                        dns_zone_id,
                        name.clone(),
                        g1,
                        Some(g2),
                        vec![r2],
                    )
                    .unwrap(),
                ])
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("duplicate key value violates unique constraint")
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_dns_builder_basic() {
        let mut dns_update = DnsVersionUpdateBuilder::new(
            DnsGroup::External,
            String::from("basic test"),
            String::from("the basic test"),
        );
        assert_eq!(dns_update.dns_group, DnsGroup::External);
        assert_eq!(dns_update.comment, "basic test");
        assert_eq!(dns_update.creator, "the basic test");
        assert!(dns_update.names_added.is_empty());
        assert!(dns_update.names_removed.is_empty());

        let aaaa_record1 = DnsRecord::Aaaa(Ipv6Addr::LOCALHOST);
        let aaaa_record2 = DnsRecord::Aaaa("fe80::1".parse().unwrap());
        let records1 = vec![aaaa_record1];
        let records2 = vec![aaaa_record2];

        // Basic case: add a name
        dns_update.add_name(String::from("test1"), records1.clone()).unwrap();
        assert_eq!(dns_update.names_added.get("test1").unwrap(), &records1);

        // Cannot add a name that's already been added.  After the error, the
        // state is unchanged.
        let error = dns_update
            .add_name(String::from("test1"), records2.clone())
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Internal Error: DNS update (\"basic test\") attempted to add \
            name \"test1\" multiple times"
        );
        assert_eq!(dns_update.names_added.get("test1").unwrap(), &records1);
        assert_eq!(dns_update.names_added.len(), 1);

        // Neither of these operations changed `names_removed`.
        assert!(dns_update.names_removed.is_empty());

        // Basic case: remove a name.
        dns_update.remove_name(String::from("test2")).unwrap();
        assert!(dns_update.names_removed.contains("test2"));

        // Cannot remove a name that's already been removed.  After the error,
        // the state is unchanged.
        let error = dns_update.remove_name(String::from("test2")).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Internal Error: DNS update (\"basic test\") attempted to remove \
            name \"test2\" multiple times"
        );
        assert!(dns_update.names_removed.contains("test2"));
        assert_eq!(dns_update.names_removed.len(), 1);

        // Neither of these operations changed `names_added`.
        assert_eq!(dns_update.names_added.get("test1").unwrap(), &records1);
        assert_eq!(dns_update.names_added.len(), 1);

        // It's fine to remove and add the same name.  The order of add and
        // remove does not matter.
        dns_update.remove_name(String::from("test1")).unwrap();
        dns_update.add_name(String::from("test2"), records2.clone()).unwrap();
        assert_eq!(dns_update.names_removed.len(), 2);
        assert!(dns_update.names_removed.contains("test1"));
        assert!(dns_update.names_removed.contains("test2"));
        assert_eq!(dns_update.names_added.len(), 2);
        assert_eq!(dns_update.names_added.get("test1").unwrap(), &records1);
        assert_eq!(dns_update.names_added.get("test2").unwrap(), &records2);
    }

    #[tokio::test]
    async fn test_dns_update_incremental() {
        let logctx = dev::test_setup_log("test_dns_update_incremental");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let now = Utc::now();

        // Create three DNS zones for testing:
        //
        // dns_zone1: the zone we're going to do most of our testing with
        // dns_zone2: another zone in the same DNS group
        // dns_zone3: a zone in a different DNS group
        let dns_zone1 = DnsZone {
            id: Uuid::new_v4(),
            time_created: now,
            dns_group: DnsGroup::External,
            zone_name: String::from("oxide1.test"),
        };
        let dns_zone2 = DnsZone {
            id: Uuid::new_v4(),
            time_created: now,
            dns_group: DnsGroup::External,
            zone_name: String::from("oxide2.test"),
        };
        let dns_zone3 = DnsZone {
            id: Uuid::new_v4(),
            time_created: now,
            dns_group: DnsGroup::Internal,
            zone_name: String::from("oxide3.test"),
        };

        {
            // Create those initial zones.
            use nexus_db_schema::schema::dns_zone::dsl;
            diesel::insert_into(dsl::dns_zone)
                .values(vec![
                    dns_zone1.clone(),
                    dns_zone2.clone(),
                    dns_zone3.clone(),
                ])
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap();
        }
        {
            // Create initial versions of each DNS group.
            use nexus_db_schema::schema::dns_version::dsl;
            diesel::insert_into(dsl::dns_version)
                .values(vec![
                    DnsVersion {
                        dns_group: DnsGroup::Internal,
                        version: Generation::new(),
                        time_created: now,
                        creator: "test suite 5".to_string(),
                        comment: "test suite 6".to_string(),
                    },
                    DnsVersion {
                        dns_group: DnsGroup::External,
                        version: Generation::new(),
                        time_created: now,
                        creator: "test suite 7".to_string(),
                        comment: "test suite 8".to_string(),
                    },
                ])
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap();
        }

        // The configuration starts off empty.
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 1);
        assert_eq!(dns_config.zones.len(), 0);

        // Add a few DNS names.
        let aaaa_record1 = DnsRecord::Aaaa(Ipv6Addr::LOCALHOST);
        let aaaa_record2 = DnsRecord::Aaaa("fe80::1".parse().unwrap());
        let records1 = vec![aaaa_record1.clone()];
        let records2 = vec![aaaa_record2.clone()];
        let records12 = vec![aaaa_record1, aaaa_record2];

        {
            let mut update = DnsVersionUpdateBuilder::new(
                DnsGroup::External,
                String::from("update 1: set up zone1/zone2"),
                String::from("the test suite"),
            );
            update.add_name(String::from("n1"), records1.clone()).unwrap();
            update.add_name(String::from("n2"), records2.clone()).unwrap();

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            datastore
                .dns_update_incremental(&opctx, &conn, update)
                .await
                .unwrap();
        }

        // Verify the new config.
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 2);
        assert_eq!(dns_config.zones.len(), 2);
        assert_eq!(dns_config.zones[0].zone_name, "oxide1.test");
        assert_eq!(
            dns_config.zones[0].records,
            HashMap::from([
                ("n1".to_string(), records1.clone()),
                ("n2".to_string(), records2.clone()),
            ])
        );
        assert_eq!(dns_config.zones[1].zone_name, "oxide2.test");
        assert_eq!(dns_config.zones[0].records, dns_config.zones[1].records,);

        // Now change "n1" in the group by removing it and adding it again.
        // This should work and affect both zones.
        {
            let mut update = DnsVersionUpdateBuilder::new(
                DnsGroup::External,
                String::from("update 2: change n1 in zone1/zone2"),
                String::from("the test suite"),
            );
            update.remove_name(String::from("n1")).unwrap();
            update.add_name(String::from("n1"), records12.clone()).unwrap();

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            datastore
                .dns_update_incremental(&opctx, &conn, update)
                .await
                .unwrap();
        }

        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 3);
        assert_eq!(dns_config.zones.len(), 2);
        assert_eq!(dns_config.zones[0].zone_name, "oxide1.test");
        assert_eq!(
            dns_config.zones[0].records,
            HashMap::from([
                ("n1".to_string(), records12.clone()),
                ("n2".to_string(), records2.clone()),
            ])
        );
        assert_eq!(dns_config.zones[1].zone_name, "oxide2.test");
        assert_eq!(dns_config.zones[0].records, dns_config.zones[1].records,);

        // Now just remove "n1" in this group altogether.
        {
            let mut update = DnsVersionUpdateBuilder::new(
                DnsGroup::External,
                String::from("update 3: remove n1 in zone1/zone2"),
                String::from("the test suite"),
            );
            update.remove_name(String::from("n1")).unwrap();

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            datastore
                .dns_update_incremental(&opctx, &conn, update)
                .await
                .unwrap();
        }

        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 4);
        assert_eq!(dns_config.zones.len(), 2);
        assert_eq!(dns_config.zones[0].zone_name, "oxide1.test");
        assert_eq!(
            dns_config.zones[0].records,
            HashMap::from([("n2".to_string(), records2.clone()),])
        );
        assert_eq!(dns_config.zones[1].zone_name, "oxide2.test");
        assert_eq!(dns_config.zones[0].records, dns_config.zones[1].records,);

        // Now add "n1" back -- again.
        {
            let mut update = DnsVersionUpdateBuilder::new(
                DnsGroup::External,
                String::from("update 4: add n1 in zone1/zone2"),
                String::from("the test suite"),
            );
            update.add_name(String::from("n1"), records2.clone()).unwrap();

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            datastore
                .dns_update_incremental(&opctx, &conn, update)
                .await
                .unwrap();
        }

        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 5);
        assert_eq!(dns_config.zones.len(), 2);
        assert_eq!(dns_config.zones[0].zone_name, "oxide1.test");
        assert_eq!(
            dns_config.zones[0].records,
            HashMap::from([
                ("n1".to_string(), records2.clone()),
                ("n2".to_string(), records2.clone()),
            ])
        );
        assert_eq!(dns_config.zones[1].zone_name, "oxide2.test");
        assert_eq!(dns_config.zones[0].records, dns_config.zones[1].records,);

        // Now, try concurrent updates to different DNS groups.  Both should
        // succeed.
        {
            let mut update1 = DnsVersionUpdateBuilder::new(
                DnsGroup::External,
                String::from("update: concurrent part 1"),
                String::from("the test suite"),
            );
            update1.remove_name(String::from("n1")).unwrap();

            let conn1 = datastore.pool_connection_for_tests().await.unwrap();
            let conn2 = datastore.pool_connection_for_tests().await.unwrap();
            let (wait1_tx, wait1_rx) = tokio::sync::oneshot::channel();
            let (wait2_tx, wait2_rx) = tokio::sync::oneshot::channel();

            let cds = datastore.clone();
            let copctx = opctx.child(std::collections::BTreeMap::new());

            #[allow(clippy::disallowed_methods)]
            let mut fut = conn1
                .transaction_async(|c1| async move {
                    cds.dns_update_incremental(&copctx, &c1, update1)
                        .await
                        .unwrap();
                    // Let the outside scope know we've done the update, but we
                    // haven't committed the transaction yet.  Wait for them to
                    // tell us to proceed.
                    wait1_tx.send(()).unwrap();
                    let _ = wait2_rx.await.unwrap();
                    Ok::<(), TransactionError<()>>(())
                })
                .fuse();

            // Wait for that transaction to get far enough to have done the
            // update.  We also have to wait for the transaction itself or it
            // won't run.
            let mut wait1_rx = wait1_rx.fuse();
            futures::select! {
                _ = fut => (),
                r = wait1_rx => r.unwrap(),
            };

            // Now start another transaction that updates the same DNS group and
            // have it complete before the first one does.
            let mut update2 = DnsVersionUpdateBuilder::new(
                DnsGroup::Internal,
                String::from("update: concurrent part 2"),
                String::from("the test suite"),
            );
            update2.add_name(String::from("n1"), records1.clone()).unwrap();
            datastore
                .dns_update_incremental(&opctx, &conn2, update2)
                .await
                .unwrap();

            // Now let the first one finish.
            wait2_tx.send(()).unwrap();
            // Make sure it completed successfully.
            fut.await.unwrap();
        }

        // Verify the result of the concurrent update.
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 6);
        assert_eq!(dns_config.zones.len(), 2);
        assert_eq!(dns_config.zones[0].zone_name, "oxide1.test");
        assert_eq!(
            dns_config.zones[0].records,
            HashMap::from([("n2".to_string(), records2.clone()),])
        );
        assert_eq!(dns_config.zones[1].zone_name, "oxide2.test");
        assert_eq!(dns_config.zones[0].records, dns_config.zones[1].records,);
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 2);
        assert_eq!(dns_config.zones.len(), 1);
        assert_eq!(dns_config.zones[0].zone_name, "oxide3.test");
        assert_eq!(
            dns_config.zones[0].records,
            HashMap::from([("n1".to_string(), records1.clone()),])
        );

        // Failure case: cannot remove a name that didn't exist.
        {
            let mut update = DnsVersionUpdateBuilder::new(
                DnsGroup::External,
                String::from("bad update: remove non-existent name"),
                String::from("the test suite"),
            );
            update.remove_name(String::from("n4")).unwrap();

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let error = datastore
                .dns_update_incremental(&opctx, &conn, update)
                .await
                .unwrap_err();
            let error = match error {
                TransactionError::CustomError(err) => err,
                _ => panic!("Unexpected error: {:?}", error),
            };
            assert_eq!(
                error.to_string(),
                "Internal Error: updated wrong number of dns_name \
                records: expected 2, actually marked 0 for removal"
            );
        }

        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 6);

        // Failure case: cannot add a name that already exists.
        {
            let mut update = DnsVersionUpdateBuilder::new(
                DnsGroup::External,
                String::from("bad update: remove non-existent name"),
                String::from("the test suite"),
            );
            update.add_name(String::from("n2"), records1.clone()).unwrap();

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let error = Error::from(
                datastore
                    .dns_update_incremental(&opctx, &conn, update)
                    .await
                    .unwrap_err(),
            );
            let msg = error.to_string();
            assert!(msg.starts_with("Internal Error: "), "Message: {msg:}");
            assert!(
                msg.contains("violates unique constraint"),
                "Message: {msg:}"
            );
        }

        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .unwrap();
        assert_eq!(u64::from(dns_config.generation), 6);
        assert_eq!(dns_config.zones.len(), 2);
        assert_eq!(dns_config.zones[0].zone_name, "oxide1.test");
        assert_eq!(
            dns_config.zones[0].records,
            HashMap::from([("n2".to_string(), records2.clone()),])
        );
        assert_eq!(dns_config.zones[1].zone_name, "oxide2.test");
        assert_eq!(dns_config.zones[0].records, dns_config.zones[1].records,);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_dns_update_from_version() {
        let logctx = dev::test_setup_log("test_dns_update_from_version");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // The guts of `dns_update_from_version()` are shared with
        // `dns_update_incremental()`.  The main cases worth testing here are
        // (1) quick check that the happy path works, plus (2) make sure it
        // fails when the precondition fails (current version doesn't match
        // what's expected).
        //
        // Start by loading some initial data.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let initial_data = InitialDnsGroup::new(
            DnsGroup::Internal,
            "my-zone",
            "test-suite",
            "test-suite",
            HashMap::from([
                (
                    "wendell".to_string(),
                    vec![DnsRecord::Aaaa(Ipv6Addr::LOCALHOST)],
                ),
                (
                    "krabappel".to_string(),
                    vec![DnsRecord::Aaaa(Ipv6Addr::LOCALHOST)],
                ),
            ]),
        );
        DataStore::load_dns_data(&conn, initial_data)
            .await
            .expect("failed to insert initial data");

        // Construct an update and apply it conditional on the current
        // generation matching the initial one.  This should succeed.
        let mut update1 = DnsVersionUpdateBuilder::new(
            DnsGroup::Internal,
            String::from("test-suite-1"),
            String::from("test-suite-1"),
        );
        update1.remove_name(String::from("wendell")).unwrap();
        update1
            .add_name(
                String::from("nelson"),
                vec![DnsRecord::Aaaa(Ipv6Addr::LOCALHOST)],
            )
            .unwrap();
        let gen1 = Generation::new();
        datastore
            .dns_update_from_version(&opctx, update1, gen1)
            .await
            .expect("failed to update from first generation");

        // Now construct another update based on the _first_ version and try to
        // apply that.  It should not work because the version has changed from
        // under us.
        let mut update2 = DnsVersionUpdateBuilder::new(
            DnsGroup::Internal,
            String::from("test-suite-2"),
            String::from("test-suite-2"),
        );
        update2.remove_name(String::from("krabappel")).unwrap();
        update2
            .add_name(
                String::from("hoover"),
                vec![DnsRecord::Aaaa(Ipv6Addr::LOCALHOST)],
            )
            .unwrap();
        let error = datastore
            .dns_update_from_version(&opctx, update2.clone(), gen1)
            .await
            .expect_err("update unexpectedly succeeded");
        assert!(
            error
                .to_string()
                .contains("expected current DNS version to be 1, found 2")
        );

        // At this point, the database state should reflect the first update but
        // not the second.
        let config = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("failed to read config");
        let gen2 = nexus_db_model::Generation(gen1.next());
        assert_eq!(gen2.0, config.generation);
        assert_eq!(1, config.zones.len());
        let records = &config.zones[0].records;
        assert!(records.contains_key("nelson"));
        assert!(!records.contains_key("wendell"));
        assert!(records.contains_key("krabappel"));

        // We can apply the second update, as long as we say it's conditional on
        // the current generation.
        datastore
            .dns_update_from_version(&opctx, update2, gen2)
            .await
            .expect("failed to update from first generation");
        let config = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("failed to read config");
        assert_eq!(gen2.next(), config.generation);
        assert_eq!(1, config.zones.len());
        let records = &config.zones[0].records;
        assert!(records.contains_key("nelson"));
        assert!(!records.contains_key("wendell"));
        assert!(!records.contains_key("krabappel"));
        assert!(records.contains_key("hoover"));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
