// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::DnsGroup;
use crate::db::model::DnsName;
use crate::db::model::DnsVersion;
use crate::db::model::DnsZone;
use crate::db::model::Generation;
use crate::db::model::InitialDnsGroup;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::PoolError;
use diesel::prelude::*;
use nexus_types::internal_api::params::DnsConfigParams;
use nexus_types::internal_api::params::DnsConfigZone;
use nexus_types::internal_api::params::DnsKv;
use nexus_types::internal_api::params::DnsRecordKey;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::bail_unless;
use slog::debug;
use std::num::NonZeroU32;
use uuid::Uuid;

// This restriction could be removed by just implementing paginated reads.
const NMAX_DNS_ZONES: u32 = 10;

impl DataStore {
    async fn dns_zones_list(
        &self,
        opctx: &OpContext,
        dns_group: DnsGroup,
        pagparams: &DataPageParams<'_, String>,
    ) -> ListResultVec<DnsZone> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::dns_zone::dsl;
        paginated(dsl::dns_zone, dsl::zone_name, pagparams)
            .filter(dsl::dns_group.eq(dns_group))
            .select(DnsZone::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    async fn dns_group_latest_version(
        &self,
        opctx: &OpContext,
        dns_group: DnsGroup,
    ) -> LookupResult<DnsVersion> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::dns_version::dsl;
        let versions = dsl::dns_version
            .filter(dsl::dns_group.eq(dns_group))
            .order_by(dsl::version.desc())
            .limit(1)
            .select(DnsVersion::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;

        bail_unless!(
            versions.len() == 1,
            "expected exactly one latest version for DNS group {:?}, found {}",
            dns_group,
            versions.len()
        );

        Ok(versions.into_iter().next().unwrap())
    }

    async fn dns_names_list(
        &self,
        opctx: &OpContext,
        dns_zone_id: Uuid,
        version: Generation,
        pagparams: &DataPageParams<'_, String>,
    ) -> ListResultVec<DnsKv> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::dns_name::dsl;
        Ok(paginated(dsl::dns_name, dsl::name, pagparams)
            .filter(dsl::dns_zone_id.eq(dns_zone_id))
            .filter(dsl::version_added.le(version))
            .filter(
                dsl::version_removed
                    .is_null()
                    .or(dsl::version_removed.gt(version)),
            )
            .select(DnsName::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?
            .into_iter()
            .filter_map(|n: DnsName| {
                match serde_json::from_value(n.dns_record_data) {
                    Ok(records) => Some(DnsKv {
                        key: DnsRecordKey { name: n.name },
                        records,
                    }),
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
                }
            })
            .collect())
    }

    /// Read the latest complete DNS configuration for a particular DNS group
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

    #[cfg(test)]
    pub async fn dns_config_read_version_test(
        &self,
        opctx: &OpContext,
        log: &slog::Logger,
        batch_size: NonZeroU32,
        version: &DnsVersion,
    ) -> Result<DnsConfigParams, Error> {
        self.dns_config_read_version(opctx, log, batch_size, version).await
    }

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
            let mut total_found = 0;
            let mut zone_records = Vec::new();
            let mut marker = None;

            loop {
                debug!(log, "listing DNS names for zone";
                    "dns_zone_id" => zone.id.to_string(),
                    "dns_zone_name" => &zone.zone_name,
                    "version" => i64::from(&version.version.0),
                    "found_so_far" => total_found,
                    "batch_size" => batch_size.get(),
                );
                let pagparams = DataPageParams {
                    marker: marker.as_ref(),
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: batch_size,
                };
                let names_batch = self
                    .dns_names_list(opctx, zone.id, version.version, &pagparams)
                    .await?;
                let nfound = names_batch.len();
                if nfound == 0 {
                    break;
                }

                total_found += nfound;
                let last = &names_batch[nfound - 1];
                marker = Some(last.key.name.clone());

                zone_records.extend(names_batch.into_iter());

                if nfound < usize::try_from(batch_size.get()).unwrap() {
                    break;
                }
            }

            debug!(log, "found all DNS names for zone";
                "dns_zone_id" => zone.id.to_string(),
                "dns_zone_name" => &zone.zone_name,
                "version" => i64::from(&version.version.0),
                "found_so_far" => total_found,
            );

            if !zone_records.is_empty() {
                zones.push(DnsConfigZone {
                    zone_name: zone.zone_name,
                    records: zone_records,
                });
            }
        }

        let generation =
            u64::try_from(i64::from(&version.version.0)).map_err(|e| {
                Error::internal_error(&format!(
                    "unsupported generation number: {:#}",
                    e
                ))
            })?;

        debug!(log, "read DNS config";
            "version" => i64::from(&version.version.0),
            "nzones" => zones.len()
        );

        Ok(DnsConfigParams {
            generation,
            time_created: version.time_created,
            zones,
        })
    }

    /// Load initial data for a DNS group into the database
    pub async fn load_dns_data<ConnErr>(
        conn: &(impl async_bb8_diesel::AsyncConnection<
            crate::db::pool::DbConnection,
            ConnErr,
        > + Sync),
        dns: InitialDnsGroup,
    ) -> Result<(), Error>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        ConnErr: Into<PoolError>,
    {
        {
            use db::schema::dns_zone::dsl;
            diesel::insert_into(dsl::dns_zone)
                .values(dns.row_for_zone())
                .on_conflict((dsl::dns_group, dsl::zone_name))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        e.into(),
                        ErrorHandler::Server,
                    )
                })?;
        }

        {
            use db::schema::dns_version::dsl;
            diesel::insert_into(dsl::dns_version)
                .values(dns.row_for_version())
                .on_conflict((dsl::dns_group, dsl::version))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        e.into(),
                        ErrorHandler::Server,
                    )
                })?;
        }

        {
            use db::schema::dns_name::dsl;
            diesel::insert_into(dsl::dns_name)
                .values(dns.rows_for_names().map_err(|error| {
                    Error::internal_error(&format!(
                        "error serializing initial DNS data: {:#}",
                        error
                    ))
                })?)
                .on_conflict((dsl::dns_zone_id, dsl::version_added, dsl::name))
                .do_nothing()
                .execute_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        e.into(),
                        ErrorHandler::Server,
                    )
                })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::db::datastore::datastore_test;
    use crate::db::DataStore;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::DnsName;
    use nexus_db_model::DnsVersion;
    use nexus_db_model::DnsZone;
    use nexus_db_model::Generation;
    use nexus_db_model::InitialDnsGroup;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::internal_api::params::DnsKv;
    use nexus_types::internal_api::params::DnsRecord;
    use nexus_types::internal_api::params::DnsRecordKey;
    use nexus_types::internal_api::params::Srv;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    // Tests reading various uninitialized or partially-initialized DNS data
    #[tokio::test]
    async fn test_read_dns_config_uninitialized() {
        let logctx = dev::test_setup_log("test_read_dns_config_uninitialized");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

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
            use crate::db::schema::dns_version::dsl;
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
                    datastore
                        .pool_for_tests()
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
        assert_eq!(dns_config.generation, 1);
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

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Tests a very simple configuration of DNS data
    #[tokio::test]
    async fn test_read_dns_config_basic() {
        let logctx = dev::test_setup_log("test_read_dns_config_basic");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create exactly one zone with no names in it.
        // This will not show up in the read config.
        let before = Utc::now();
        let initial = InitialDnsGroup::new(
            DnsGroup::External,
            "dummy.oxide.test",
            "test suite",
            "test suite",
            vec![],
        );
        {
            let conn = datastore.pool_for_tests().await.unwrap();
            DataStore::load_dns_data(conn, initial)
                .await
                .expect("failed to load initial DNS zone");
        }

        let after = Utc::now();
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect("failed to read DNS config");
        println!("found config: {:?}", dns_config);
        assert_eq!(dns_config.generation, 1);
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
            vec![
                DnsKv {
                    key: DnsRecordKey { name: "wendell".to_string() },
                    records: wendell_records.clone(),
                },
                DnsKv {
                    key: DnsRecordKey { name: "krabappel".to_string() },
                    records: krabappel_records.clone(),
                },
            ],
        );
        {
            let conn = datastore.pool_for_tests().await.unwrap();
            DataStore::load_dns_data(conn, initial)
                .await
                .expect("failed to load initial DNS zone");
        }

        let after = Utc::now();
        let dns_config = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("failed to read DNS config");
        println!("found config: {:?}", dns_config);
        assert_eq!(dns_config.generation, 1);
        assert!(dns_config.time_created >= before);
        assert!(dns_config.time_created <= after);
        assert_eq!(dns_config.zones.len(), 1);
        assert_eq!(dns_config.zones[0].zone_name, "dummy.oxide.internal");
        assert_eq!(dns_config.zones[0].records.len(), 2);
        assert_eq!(dns_config.zones[0].records[0].key.name, "krabappel");
        assert_eq!(dns_config.zones[0].records[0].records, krabappel_records);
        assert_eq!(dns_config.zones[0].records[1].key.name, "wendell");
        assert_eq!(dns_config.zones[0].records[1].records, wendell_records);

        // Do this again, but controlling the batch size to make sure pagination
        // works right.
        let dns_config_batch_1 = datastore
            .dns_config_read_version_test(
                &opctx,
                &opctx.log.clone(),
                NonZeroU32::new(1).unwrap(),
                &DnsVersion {
                    dns_group: DnsGroup::Internal,
                    version: Generation(1.try_into().unwrap()),
                    time_created: dns_config.time_created,
                    creator: "unused".to_string(),
                    comment: "unused".to_string(),
                },
            )
            .await
            .expect("failed to read DNS config with batch size 1");
        assert_eq!(dns_config_batch_1, dns_config);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Tests a complex configuration of DNS data (see comment below)
    #[tokio::test]
    async fn test_read_dns_config_complex() {
        let logctx = dev::test_setup_log("test_read_dns_config_complex");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
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
        let g1 = Generation(1.try_into().unwrap());
        let g2 = Generation(2.try_into().unwrap());
        let g3 = Generation(3.try_into().unwrap());
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
        let json_r1 = serde_json::to_value(&records_r1).unwrap();
        let json_r2 = serde_json::to_value(&records_r2).unwrap();
        let json_r1r2 = serde_json::to_value(&records_r1r2).unwrap();

        // Set up the database state exactly as we want it.
        // First, insert the DNS zones.
        {
            use crate::db::schema::dns_zone::dsl;
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
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap();
        }

        // Next, insert the DNS versions.
        {
            use crate::db::schema::dns_version::dsl;
            diesel::insert_into(dsl::dns_version)
                .values(vec![
                    v1.clone(),
                    v2.clone(),
                    v3.clone(),
                    vi1.clone(),
                    vi2.clone(),
                ])
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap();
        }

        // Finally, insert all DNS names for all versions of all zones.
        {
            use crate::db::schema::dns_name::dsl;
            diesel::insert_into(dsl::dns_name)
                .values(vec![
                    // External zone "z1" records test that:
                    // - name "n1" lasts multiple generations
                    // - name "n2" removed between generations
                    // - name "n3" added between generations
                    // - a zone is removed between generations
                    DnsName {
                        dns_zone_id: z1_id,
                        name: "n1".to_string(),
                        version_added: g1,
                        version_removed: Some(g3),
                        dns_record_data: json_r1.clone(),
                    },
                    DnsName {
                        dns_zone_id: z1_id,
                        name: "n2".to_string(),
                        version_added: g1,
                        version_removed: Some(g2),
                        dns_record_data: json_r1.clone(),
                    },
                    DnsName {
                        dns_zone_id: z1_id,
                        name: "n3".to_string(),
                        version_added: g2,
                        version_removed: Some(g3),
                        dns_record_data: json_r1.clone(),
                    },
                    // External zone "z2" records test that:
                    // - we add a zone between generation
                    // - a record ("n1") changes between gen 2 and gen 3
                    DnsName {
                        dns_zone_id: z2_id,
                        name: "n1".to_string(),
                        version_added: g2,
                        version_removed: Some(g3),
                        dns_record_data: json_r1.clone(),
                    },
                    DnsName {
                        dns_zone_id: z2_id,
                        name: "n1".to_string(),
                        version_added: g3,
                        version_removed: None,
                        dns_record_data: json_r2.clone(),
                    },
                    // External zone "z3" records test that:
                    // - a zone exists in all generations
                    // - a record ("n1") changes between generations
                    // - a record ("n2") is added between generations
                    // - a record ("n1") is removed between generations
                    // Using the same names in different zones ensures these are
                    // treated separately.
                    DnsName {
                        dns_zone_id: z3_id,
                        name: "n1".to_string(),
                        version_added: g1,
                        version_removed: Some(g2),
                        dns_record_data: json_r2.clone(),
                    },
                    DnsName {
                        dns_zone_id: z3_id,
                        name: "n1".to_string(),
                        version_added: g2,
                        version_removed: Some(g3),
                        dns_record_data: json_r1r2.clone(),
                    },
                    DnsName {
                        dns_zone_id: z3_id,
                        name: "n2".to_string(),
                        version_added: g2,
                        version_removed: None,
                        dns_record_data: json_r2.clone(),
                    },
                    // Internal zone records test that the namespaces are
                    // orthogonal across different DNS groups.
                    DnsName {
                        dns_zone_id: zinternal_id,
                        name: "n1".to_string(),
                        version_added: g2,
                        version_removed: None,
                        dns_record_data: json_r2.clone(),
                    },
                ])
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap();
        }

        // Now, read back the state (using the function we're testing) for each
        // version of each DNS group that we wrote.

        // Verify external version 1.
        let dns_config_v1 = datastore
            .dns_config_read_version_test(&opctx, log, batch_size, &v1)
            .await
            .unwrap();
        println!("dns_config_v1: {:?}", dns_config_v1);
        assert_eq!(dns_config_v1.generation, 1);
        assert_eq!(dns_config_v1.zones.len(), 2);
        assert_eq!(dns_config_v1.zones[0].zone_name, "z1.foo");
        assert_eq!(dns_config_v1.zones[0].records.len(), 2);
        assert_eq!(dns_config_v1.zones[0].records[0].key.name, "n1");
        assert_eq!(dns_config_v1.zones[0].records[0].records, records_r1);
        assert_eq!(dns_config_v1.zones[0].records[1].key.name, "n2");
        assert_eq!(dns_config_v1.zones[0].records[1].records, records_r1);
        assert_eq!(dns_config_v1.zones[1].zone_name, "z3.bar");
        assert_eq!(dns_config_v1.zones[1].records.len(), 1);
        assert_eq!(dns_config_v1.zones[1].records[0].key.name, "n1");
        assert_eq!(dns_config_v1.zones[1].records[0].records, records_r2);

        // Verify external version 2.
        let dns_config_v2 = datastore
            .dns_config_read_version_test(&opctx, log, batch_size, &v2)
            .await
            .unwrap();
        println!("dns_config_v2: {:?}", dns_config_v2);
        assert_eq!(dns_config_v2.generation, 2);
        assert_eq!(dns_config_v2.zones.len(), 3);
        assert_eq!(dns_config_v2.zones[0].zone_name, "z1.foo");
        assert_eq!(dns_config_v2.zones[0].records.len(), 2);
        assert_eq!(dns_config_v2.zones[0].records[0].key.name, "n1");
        assert_eq!(dns_config_v2.zones[0].records[0].records, records_r1);
        assert_eq!(dns_config_v2.zones[0].records[1].key.name, "n3");
        assert_eq!(dns_config_v2.zones[0].records[1].records, records_r1);

        assert_eq!(dns_config_v2.zones[1].zone_name, "z2.foo");
        assert_eq!(dns_config_v2.zones[1].records.len(), 1);
        assert_eq!(dns_config_v2.zones[1].records[0].key.name, "n1");
        assert_eq!(dns_config_v2.zones[1].records[0].records, records_r1);

        assert_eq!(dns_config_v2.zones[2].zone_name, "z3.bar");
        assert_eq!(dns_config_v2.zones[2].records.len(), 2);
        assert_eq!(dns_config_v2.zones[2].records[0].key.name, "n1");
        assert_eq!(dns_config_v2.zones[2].records[0].records, records_r1r2);
        assert_eq!(dns_config_v2.zones[2].records[1].key.name, "n2");
        assert_eq!(dns_config_v2.zones[2].records[1].records, records_r2);

        // Verify external version 3
        let dns_config_v3 = datastore
            .dns_config_read_version_test(&opctx, log, batch_size, &v3)
            .await
            .unwrap();
        println!("dns_config_v3: {:?}", dns_config_v3);
        assert_eq!(dns_config_v3.generation, 3);
        assert_eq!(dns_config_v3.zones.len(), 2);
        assert_eq!(dns_config_v3.zones[0].zone_name, "z2.foo");
        assert_eq!(dns_config_v3.zones[0].records.len(), 1);
        assert_eq!(dns_config_v3.zones[0].records[0].key.name, "n1");
        assert_eq!(dns_config_v3.zones[0].records[0].records, records_r2);
        assert_eq!(dns_config_v3.zones[1].zone_name, "z3.bar");
        assert_eq!(dns_config_v3.zones[1].records.len(), 1);
        assert_eq!(dns_config_v3.zones[1].records[0].key.name, "n2");
        assert_eq!(dns_config_v3.zones[1].records[0].records, records_r2);

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
            .dns_config_read_version_test(&opctx, log, batch_size, &vi1)
            .await
            .unwrap();
        println!("internal dns_config_v1: {:?}", internal_dns_config_v1);
        assert_eq!(internal_dns_config_v1.generation, 1);
        assert_eq!(internal_dns_config_v1.zones.len(), 0);

        // Verify internal version 2.
        let internal_dns_config_v2 = datastore
            .dns_config_read_version_test(&opctx, log, batch_size, &vi2)
            .await
            .unwrap();
        println!("internal dns_config_v2: {:?}", internal_dns_config_v2);
        assert_eq!(internal_dns_config_v2.generation, 2);
        assert_eq!(internal_dns_config_v2.zones.len(), 1);
        assert_eq!(internal_dns_config_v2.zones[0].zone_name, "z1.foo");
        assert_eq!(internal_dns_config_v2.zones[0].records.len(), 1);
        assert_eq!(internal_dns_config_v2.zones[0].records[0].key.name, "n1");
        assert_eq!(
            internal_dns_config_v2.zones[0].records[0].records,
            records_r2
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Tests the unique indexes enforced by the database.
    #[tokio::test]
    async fn test_dns_uniqueness() {
        let logctx = dev::test_setup_log("test_dns_uniqueness");
        let db = test_setup_database(&logctx.log).await;
        let (_opctx, datastore) = datastore_test(&logctx, &db).await;
        let now = Utc::now();

        // There cannot be two DNS zones in the same group with the same name.
        {
            use crate::db::schema::dns_zone::dsl;
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
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("duplicate key value violates unique constraint"));
        }

        // There cannot be two DNS version records with the same group and
        // version number.
        {
            use crate::db::schema::dns_version::dsl;
            let error = diesel::insert_into(dsl::dns_version)
                .values(vec![
                    DnsVersion {
                        dns_group: DnsGroup::Internal,
                        version: Generation::new().into(),
                        time_created: now,
                        creator: "test suite 1".to_string(),
                        comment: "test suite 2".to_string(),
                    },
                    DnsVersion {
                        dns_group: DnsGroup::Internal,
                        version: Generation::new().into(),
                        time_created: now,
                        creator: "test suite 3".to_string(),
                        comment: "test suite 4".to_string(),
                    },
                ])
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("duplicate key value violates unique constraint"));
        }

        // There cannot be two DNS names in the same zone with the same name
        // created in the same generation.
        {
            use crate::db::schema::dns_name::dsl;
            let dns_zone_id = Uuid::new_v4();
            let name = "n1".to_string();
            let g1 = Generation(1.try_into().unwrap());
            let g2 = Generation(2.try_into().unwrap());
            let r1 = DnsRecord::Aaaa("fe80::1:2:3:4".parse().unwrap());
            let r2 = DnsRecord::Aaaa("fe80::1:1:1:1".parse().unwrap());
            let json_r1 = serde_json::to_value(&[r1]).unwrap();
            let json_r2 = serde_json::to_value(&[r2]).unwrap();

            let error = diesel::insert_into(dsl::dns_name)
                .values(vec![
                    DnsName {
                        dns_zone_id,
                        name: name.clone(),
                        version_added: g1,
                        version_removed: None,
                        dns_record_data: json_r1,
                    },
                    DnsName {
                        dns_zone_id,
                        name: name.clone(),
                        version_added: g1,
                        version_removed: Some(g2),
                        dns_record_data: json_r2,
                    },
                ])
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("duplicate key value violates unique constraint"));
        }
    }
}
