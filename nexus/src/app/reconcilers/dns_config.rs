// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler for maintaining internal copy of DNS configuration

use dns_service_client::types::DnsConfigParams;
use dns_service_client::types::DnsConfigZone;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::bail_unless;
use slog::{debug, info};
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;

// This restriction could be removed by just implementing paginated reads.
const NMAX_DNS_ZONES: u32 = 10;

pub struct DnsConfigReconciler {
    config_tx: watch::Sender<Option<DnsConfigParams>>,
    config_rx: watch::Receiver<Option<DnsConfigParams>>,
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
}

// XXX-dap some thoughts on the generalizeable interface
// accept in constructor a channel on which it receives messages to "go"
// that message contains a channel it can use to send reports back (e.g.,
// progress)

impl DnsConfigReconciler {
    pub fn new(
        datastore: Arc<DataStore>,
        dns_group: DnsGroup,
    ) -> DnsConfigReconciler {
        let (config_tx, config_rx) = watch::channel(None);
        DnsConfigReconciler { config_tx, config_rx, datastore, dns_group }
    }
}

async fn read_dns_config_from_database(
    opctx: &OpContext,
    datastore: &DataStore,
    dns_group: DnsGroup,
    batch_size: NonZeroU32,
) -> Result<DnsConfigParams, Error> {
    let log = opctx.log.new(o!("dns_group" => dns_group.to_string()));

    debug!(log, "reading DNS config");
    debug!(log, "reading DNS zones");
    let dns_zones = datastore
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

    bail_unless!(dns_zones.len() < usize::try_from(NMAX_DNS_ZONES).unwrap());

    debug!(log, "reading DNS version");
    let version = datastore.dns_group_latest_version(opctx, dns_group).await?;
    debug!(log, "found DNS version"; "version" => version.version.to_string());

    let mut marker = None;
    let mut total_found = 0;
    let mut zones = Vec::with_capacity(dns_zones.len());
    for zone in dns_zones {
        let mut zone_records = Vec::new();
        loop {
            debug!(log, "listing DNS names for zone";
                "dns_zone_id" => zone.id.to_string(),
                "dns_zone_name" => &zone.zone_name,
                "found_so_far" => total_found,
                "batch_size" => batch_size.get(),
            );
            let pagparams = DataPageParams {
                marker: marker.as_ref(),
                direction: dropshot::PaginationOrder::Ascending,
                limit: batch_size,
            };
            let names_batch = datastore
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
            "found_so_far" => total_found,
        );

        zones.push(DnsConfigZone {
            zone_name: zone.zone_name,
            records: zone_records,
        });
    }

    let generation =
        u64::try_from(i64::from(&version.version.0)).map_err(|e| {
            Error::internal_error(&format!(
                "unsupported generation number: {:#}",
                e
            ))
        })?;
    Ok(DnsConfigParams {
        generation,
        time_created: version.time_created,
        zones,
    })
}

#[cfg(test)]
mod test {
    use super::read_dns_config_from_database;
    use assert_matches::assert_matches;
    use chrono::Utc;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::InitialDnsGroup;
    use nexus_db_queries::db::DataStore;
    use nexus_test_utils::db::datastore_test;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::internal_api::params::DnsKv;
    use nexus_types::internal_api::params::DnsRecord;
    use nexus_types::internal_api::params::DnsRecordKey;
    use nexus_types::internal_api::params::Srv;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;

    #[tokio::test]
    async fn test_read_dns_config_uninitialized() {
        let logctx = dev::test_setup_log("test_read_dns_config_uninitialized");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let batch_size = NonZeroU32::new(10).unwrap();

        // If we attempt to load the config when literally nothing related to
        // DNS has been initialized, we will get an InternalError because we
        // cannot tell what version we're supposed to be at.
        let error = read_dns_config_from_database(
            &opctx,
            &datastore,
            DnsGroup::Internal,
            batch_size,
        )
        .await
        .expect_err("unexpectedly succeeding reading uninitialized DNS config");
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
            use async_bb8_diesel::AsyncRunQueryDsl;
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
        let dns_config = read_dns_config_from_database(
            &opctx,
            &datastore,
            DnsGroup::Internal,
            batch_size,
        )
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
        let error = read_dns_config_from_database(
            &opctx,
            &datastore,
            DnsGroup::External,
            batch_size,
        )
        .await
        .expect_err("unexpectedly succeeding reading uninitialized DNS config");
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

    #[tokio::test]
    async fn test_read_dns_config_basic() {
        let logctx = dev::test_setup_log("test_read_dns_config_basic");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let batch_size = NonZeroU32::new(10).unwrap();

        // Create exactly one zone with no names in it.
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
        let dns_config = read_dns_config_from_database(
            &opctx,
            &datastore,
            DnsGroup::External,
            batch_size,
        )
        .await
        .expect("failed to read DNS config");
        println!("found config: {:?}", dns_config);
        assert_eq!(dns_config.generation, 1);
        assert!(dns_config.time_created >= before);
        assert!(dns_config.time_created <= after);
        assert_eq!(dns_config.zones.len(), 1);
        assert_eq!(dns_config.zones[0].zone_name, "dummy.oxide.test");
        assert_eq!(dns_config.zones[0].records.len(), 0);

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
        let dns_config = read_dns_config_from_database(
            &opctx,
            &datastore,
            DnsGroup::Internal,
            batch_size,
        )
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
        let dns_config_batch_1 = read_dns_config_from_database(
            &opctx,
            &datastore,
            DnsGroup::Internal,
            NonZeroU32::new(1).unwrap(),
        )
        .await
        .expect("failed to read DNS config with batch size 1");
        assert_eq!(dns_config_batch_1, dns_config);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_read_dns_config_complex() {
        let logctx = dev::test_setup_log("test_read_dns_config_basic");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Construct a more complex configuration:
        //
        // - multiple zones in each DNS group
        // - multiple versions for each DNS group
        // - the multiple versions add and remove some zones
        // - the multiple versions add and remove names in some zones
        //
        // We'll want to construct configs for various versions.
        // XXX-dap need a version of the function that parametrizes the version
        // itself?
        //
        // XXX-dap plan:
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
        // [all: tests that overlapping names are treated separately]
        // "internal" group:
        // - version 1 has no zones
        // - version 2 has zone "z1.foo" with name "z1n1" with a different
        //   record than in the other DNS group
        //   [tests that the two groups' zones are truly separate]
        //
        // In the end: fetch each version of each group and make sure we have
        // the zones and records that we expect.
        //
        // XXX-dap will we want a way to write a new DnsConfig generation to the
        // database?
        todo!();

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
