// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Data migration test for update that normalized SNAT service IPs.

use crate::integration_tests::schema::DataMigrationFns;
use crate::integration_tests::schema::MigrationContext;
use chrono::DateTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::TimeZone as _;
use chrono::Utc;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use nexus_db_model::ExternalIp;
use nexus_db_model::IpAttachState;
use nexus_db_model::IpKind;
use nexus_db_model::Name;
use nexus_db_model::SqlU16;
use oxnet::Ipv4Net;
use rand::seq::IndexedRandom as _;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use tokio_postgres::Row;
use uuid::Uuid;

const N_UNTOUCHED_INSTANCE_RECORDS: usize = 10;
const N_UNTOUCHED_SERVICE_RECORDS: usize = 10;
const N_SNAT_SERVICE_RECORDS_TO_UPDATE: usize = 10;
const N_TOTAL_RECORDS: usize = N_UNTOUCHED_INSTANCE_RECORDS
    + N_UNTOUCHED_SERVICE_RECORDS
    + N_SNAT_SERVICE_RECORDS_TO_UPDATE;

pub(super) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    before_impl(ctx).boxed()
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    // clean up from any previous test
    ctx.client
        .batch_execute("DELETE FROM omicron.public.external_ip WHERE 1=1;")
        .await
        .expect("deleted old entries");

    // Insert both instance records and service records for _non_ boundary NTP
    // services, which have their names / descriptions already filled out.
    let untouched = create_untouched_records();

    // Create records that we'll need to update.
    let to_update = create_records_to_update();

    let stmt = ctx
        .client
        .prepare(
            "INSERT INTO omicron.public.external_ip ( \
                id, name, description, time_created, time_modified, \
                time_deleted, ip_pool_id, ip_pool_range_id, is_service, \
                parent_id, kind, ip, first_port, last_port, project_id, \
                state, is_probe \
            ) VALUES (\
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,\
                $11::text::omicron.public.ip_kind,\
                $12,$13,$14,$15,\
                $16::text::omicron.public.ip_attach_state,\
                $17)\
        ",
        )
        .await
        .expect("prepared insert query");

    for each in untouched.chain(to_update) {
        // This is a little goofy, but we have to convert these to strings for
        // the `tokio-postgres` client, because these don't implement its
        // `ToSql` trait.
        //
        // But note that we _also_ have to "doule cast" in the `VALUES` clause
        // above. The first, `$11::text` forces `tokio-postgres` to infer that
        // the prepared statement accepts text, which is what we're providing
        // here. The second one tells the _server_ to cast that to `ip_kind`
        // when inserting.
        let kind = match each.kind {
            IpKind::SNat => "snat",
            IpKind::Ephemeral => "ephemeral",
            IpKind::Floating => "floating",
        };
        let state = match each.state {
            IpAttachState::Detached => "detached",
            IpAttachState::Attached => "attached",
            IpAttachState::Detaching => "detaching",
            IpAttachState::Attaching => "attaching",
        };
        ctx.client
            .execute(
                &stmt,
                &[
                    &each.id,
                    &each.name.as_ref().map(|n| n.0.as_str()),
                    &each.description,
                    &each.time_created,
                    &each.time_modified,
                    &each.time_deleted,
                    &each.ip_pool_id,
                    &each.ip_pool_range_id,
                    &each.is_service,
                    &each.parent_id,
                    &kind,
                    &each.ip.ip(),
                    &i32::from(each.first_port.0),
                    &i32::from(each.last_port.0),
                    &each.project_id,
                    &state,
                    &each.is_probe,
                ],
            )
            .await
            .expect("successful insert");
    }
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    after_impl(ctx).boxed()
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let records = ctx
        .client
        .query(
            "SELECT \
                name, \
                description, \
                time_created, \
                time_modified, \
                time_deleted, \
                is_service, \
                CAST(kind AS TEXT) AS kind, \
                parent_id \
            FROM omicron.public.external_ip",
            &[],
        )
        .await
        .expect("successful select *");
    assert_eq!(records.len(), N_TOTAL_RECORDS);
    let mut n_instance_records = 0;
    let mut n_non_snat_service_records = 0;
    let mut n_snat_service_records = 0;
    for record in records {
        let kind: String = record.get("kind");
        if record.get("is_service") {
            check_service_record(record);
            if kind == "snat" {
                n_snat_service_records += 1;
            } else {
                n_non_snat_service_records += 1;
            }
        } else {
            check_instance_record_unchanged(record);
            n_instance_records += 1;
        }
    }

    assert_eq!(n_snat_service_records, N_SNAT_SERVICE_RECORDS_TO_UPDATE);
    assert_eq!(n_non_snat_service_records, N_UNTOUCHED_SERVICE_RECORDS);
    assert_eq!(n_instance_records, N_UNTOUCHED_INSTANCE_RECORDS);
}

fn check_instance_record_unchanged(record: Row) {
    let name: Option<String> = record.get("name");
    let description: Option<String> = record.get("description");
    let time_modified: DateTime<Utc> = record.get("time_modified");
    let time_deleted: Option<DateTime<Utc>> = record.get("time_deleted");
    let is_service: bool = record.get("is_service");
    let kind: String = record.get("kind");
    assert_eq!(
        time_modified,
        start_time(),
        "Should not have updated any Instance IP at all"
    );
    let is_floating = kind == "floating";
    assert_eq!(name.is_some(), is_floating);
    assert_eq!(description.is_some(), is_floating);
    assert!(time_deleted.is_none());
    assert!(!is_service);
}

fn check_service_record(record: Row) {
    let kind: String = record.get("kind");
    let name: Option<String> = record.get("name");
    let name = name.expect("all service records should have names now");
    let description: Option<String> = record.get("description");
    let description =
        description.expect("all service records should have descriptions now");
    let time_modified: DateTime<Utc> = record.get("time_modified");
    let time_deleted: Option<DateTime<Utc>> = record.get("time_deleted");
    let is_service: bool = record.get("is_service");
    let parent_id: Option<Uuid> = record.get("parent_id");
    let parent_id = parent_id.expect("services should have parent IDs");
    assert!(time_deleted.is_none());
    assert!(is_service);
    if kind == "snat" {
        assert_eq!(name, format!("ntp-{}", parent_id));
        assert_eq!(description, "boundary_ntp");
        assert!(
            time_modified > start_time(),
            "Should have updated time_modified"
        );
    } else if kind == "floating" {
        assert_eq!(
            time_modified,
            start_time(),
            "Should not have updated Floating IP service record"
        );
    } else {
        panic!("Unexpected service IP kind: {kind}");
    }
}

// Sentinel timestamp that we insert for every record in the before impl, for
// both time_created and time_modified. This is what we use to assert that the
// records haven't been touched by the migration.
fn start_time() -> DateTime<Utc> {
    let date = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let time = NaiveTime::from_hms_opt(12, 12, 12).unwrap();
    Utc.from_local_datetime(&NaiveDateTime::new(date, time)).unwrap()
}

fn create_untouched_records() -> impl Iterator<Item = ExternalIp> {
    create_untouched_instance_records()
        .into_iter()
        .chain(create_untouched_service_records())
}

fn random_instance_ip_kind() -> IpKind {
    const KINDS: &[IpKind] =
        &[IpKind::SNat, IpKind::Ephemeral, IpKind::Floating];
    *(KINDS.choose(&mut rand::rng()).unwrap())
}

fn port_for_kind(kind: IpKind) -> u16 {
    match kind {
        IpKind::SNat => u16::MAX / 4,
        IpKind::Ephemeral => u16::MAX,
        IpKind::Floating => u16::MAX,
    }
}

fn create_untouched_instance_records() -> Vec<ExternalIp> {
    let subnet = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap();
    let mut addrs = subnet.addr_iter();
    (0..N_UNTOUCHED_INSTANCE_RECORDS)
        .map(|i| {
            let kind = random_instance_ip_kind();
            let last_port = SqlU16(port_for_kind(kind));
            let (name, description, project_id) =
                if matches!(kind, IpKind::Floating) {
                    (
                        Some(Name(format!("inst-{i}").parse().unwrap())),
                        Some(String::new()),
                        Some(Uuid::new_v4()),
                    )
                } else {
                    (None, None, None)
                };
            ExternalIp {
                id: Uuid::new_v4(),
                name,
                description,
                time_created: start_time(),
                time_modified: start_time(),
                time_deleted: None,
                ip_pool_id: Uuid::new_v4(),
                ip_pool_range_id: Uuid::new_v4(),
                is_service: false,
                parent_id: Some(Uuid::new_v4()),
                kind,
                ip: IpAddr::V4(addrs.next().unwrap()).into(),
                first_port: SqlU16(0),
                last_port,
                project_id,
                state: IpAttachState::Attached,
                is_probe: false,
            }
        })
        .collect()
}

fn create_untouched_service_records() -> Vec<ExternalIp> {
    let subnet = Ipv4Net::new(Ipv4Addr::new(10, 1, 0, 0), 24).unwrap();
    let mut addrs = subnet.addr_iter();
    (0..N_UNTOUCHED_SERVICE_RECORDS)
        .map(|i| {
            ExternalIp {
                id: Uuid::new_v4(),
                name: Some(Name(format!("svc-{i}").parse().unwrap())),
                description: Some(String::new()),
                time_created: start_time(),
                time_modified: start_time(),
                time_deleted: None,
                ip_pool_id: Uuid::new_v4(),
                ip_pool_range_id: Uuid::new_v4(),
                is_service: true,
                parent_id: Some(Uuid::new_v4()),
                kind: IpKind::Floating, // Only one supported
                ip: IpAddr::V4(addrs.next().unwrap()).into(),
                first_port: SqlU16(0),
                last_port: SqlU16(u16::MAX),
                project_id: None,
                state: IpAttachState::Attached,
                is_probe: false,
            }
        })
        .collect()
}

fn create_records_to_update() -> impl Iterator<Item = ExternalIp> {
    create_snat_service_records_to_update().into_iter()
}

fn create_snat_service_records_to_update() -> Vec<ExternalIp> {
    let subnet = Ipv4Net::new(Ipv4Addr::new(10, 2, 0, 0), 24).unwrap();
    let mut addrs = subnet.addr_iter();
    (0..N_SNAT_SERVICE_RECORDS_TO_UPDATE)
        .map(|_| {
            ExternalIp {
                id: Uuid::new_v4(),
                // These had no name / description, and we want them to.
                name: None,
                description: None,
                time_created: start_time(),
                time_modified: start_time(),
                time_deleted: None,
                ip_pool_id: Uuid::new_v4(),
                ip_pool_range_id: Uuid::new_v4(),
                is_service: true,
                parent_id: Some(Uuid::new_v4()),
                kind: IpKind::SNat,
                ip: IpAddr::V4(addrs.next().unwrap()).into(),
                first_port: SqlU16(0),
                last_port: SqlU16(u16::MAX / 4),
                project_id: None,
                state: IpAttachState::Attached,
                is_probe: false,
            }
        })
        .collect()
}
