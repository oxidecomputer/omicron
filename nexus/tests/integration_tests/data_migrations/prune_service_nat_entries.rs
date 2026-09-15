// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use chrono::{DateTime, Utc};
use futures::{FutureExt, future::BoxFuture};
use omicron_common::api::external::Vni;
use pretty_assertions::assert_eq;
use std::fmt::Debug;
use std::net::IpAddr;
use uuid::Uuid;

// Quasi-random samples from the dogfood rack at the time of this writing:
//
// 1. All 7 active service zone NAT entries
// 2. 10 randomly-selected removed service zone NAT entries
// 3. 10 randomly-selected active instance NAT entries
// 4. 10 randomly-selected removed instance NAT entries
//
// Column order is:
// id,external_address,first_port,last_port,sled_address,vni,mac,version_added,version_removed,time_created,time_deleted
//
// This migration should delete the 17 service zone NAT entries and leave the 20
// instance entries untouched.
const DOGFOOD_NAT_ENTRIES: &str = r#"
58b18a55-5bad-4959-9589-74becfd2b278,172.20.26.1,0,65535,fd00:1122:3344:129::1,100,184993468874755,397346,NULL,2026-07-29 01:19:39.689495+00,NULL
a6679649-36eb-4c74-80d4-7ac5b24bbc8d,172.20.26.6,0,65535,fd00:1122:3344:102::1,100,184993468874756,397351,NULL,2026-07-29 02:42:40.000905+00,NULL
6c57aadc-169d-4caa-84e9-e7b5d241ad32,172.20.26.8,0,65535,fd00:1122:3344:108::1,100,184993468874757,397350,NULL,2026-07-29 02:42:39.991114+00,NULL
0eb2eec2-03e6-4068-b9ed-d277474f8c98,172.20.26.7,0,16383,fd00:1122:3344:103::1,100,184993468874752,397348,NULL,2026-07-29 01:51:26.180296+00,NULL
38094741-0c75-44b5-bf3a-7432a92853b4,172.20.26.9,0,65535,fd00:1122:3344:101::1,100,184993468874758,397482,NULL,2026-07-29 07:35:18.382512+00,NULL
d8cb2af0-70f8-42d8-aa5b-a0e7f7c33353,172.20.26.7,16384,32767,fd00:1122:3344:101::1,100,184993468874754,397478,NULL,2026-07-29 07:33:48.379127+00,NULL
d056e9b7-d8aa-4038-8099-c8ba0a222a6e,172.20.26.2,0,65535,fd00:1122:3344:105::1,100,184993468874753,397481,NULL,2026-07-29 07:34:26.092184+00,NULL
28d94d5c-a11f-43e7-99fe-606ae1c5d020,172.20.26.7,0,16383,fd00:1122:3344:127::1,100,184993468874752,356305,363891,2026-07-21 06:17:16.484579+00,2026-07-21 23:21:13.874508+00
e9637899-35e7-42e7-8240-cad82da025cf,172.20.26.4,0,65535,fd00:1122:3344:10b::1,100,184993468874760,395780,396366,2026-07-26 21:34:43.455073+00,2026-07-27 21:35:52.870946+00
e999d9ae-9c04-496d-9d31-24867b7e6dde,172.20.26.7,0,16383,fd00:1122:3344:127::1,100,184993468874752,397003,397347,2026-07-28 22:31:53.908885+00,2026-07-29 01:51:26.159576+00
88a5517f-942a-4a83-9f20-923a8c21b6d0,172.20.26.9,0,65535,fd00:1122:3344:129::1,100,184993468874758,355689,355727,2026-07-20 19:46:57.61687+00,2026-07-20 21:59:18.465995+00
3cfc3112-4ce7-49fc-8516-5479637c243e,172.20.26.1,0,65535,fd00:1122:3344:103::1,100,184993468874755,396369,396928,2026-07-27 21:37:52.700546+00,2026-07-28 21:20:53.465649+00
eb847abe-44f1-48f5-a5bc-4143063ccbc9,172.20.26.5,0,65535,fd00:1122:3344:109::1,100,184993468874761,397153,397354,2026-07-28 23:41:43.252402+00,2026-07-29 02:44:48.947877+00
66ba2853-ab30-4dbe-96fc-d99b8d54075b,172.20.26.7,0,16383,fd00:1122:3344:104::1,100,184993468874752,353164,354460,2026-07-15 06:22:50.5407+00,2026-07-17 06:00:59.645668+00
eb9affba-0378-4152-ab9d-34cfd696eab3,172.20.26.6,0,65535,fd00:1122:3344:102::1,100,184993468874756,355600,355729,2026-07-20 18:54:44.569782+00,2026-07-20 21:59:18.484987+00
b9bc6f88-fa25-40eb-9cf1-debb78e18098,172.20.26.5,0,65535,fd00:1122:3344:109::1,100,184993468874761,395831,396368,2026-07-26 22:03:28.595575+00,2026-07-27 21:37:52.635038+00
9d03f45a-d77f-4653-ac25-a1201612441e,172.20.26.7,16384,32767,fd00:1122:3344:129::1,100,184993468874754,355690,355720,2026-07-20 19:46:57.632214+00,2026-07-20 20:49:16.30952+00
f900b449-be8b-42a5-82f4-13a876a45822,172.20.52.132,0,65535,fd00:1122:3344:129::1,7547150,184993467858944,380664,380668,2026-07-24 22:31:56.578037+00,2026-07-24 22:32:19.405371+00
88b14885-6fc9-492a-b0fd-1ad07a6ab1fe,172.20.26.155,32768,49151,fd00:1122:3344:101::1,6033221,184993467858945,378457,378466,2026-07-24 18:25:12.629124+00,2026-07-24 18:25:51.677006+00
0b140ef2-1fca-4a0f-a448-103bb86b17f6,172.20.52.138,49152,65535,fd00:1122:3344:127::1,6033221,184993467858964,361546,361623,2026-07-21 18:33:13.57453+00,2026-07-21 19:01:05.788486+00
a048c559-51a9-4c8a-b490-5155a7a41779,172.20.26.87,49152,65535,fd00:1122:3344:108::1,6033221,184993467858944,356864,356873,2026-07-21 09:12:51.920038+00,2026-07-21 09:13:48.937282+00
d56a7101-3d1f-4e54-89c4-575bfda0348a,172.20.26.168,16384,32767,fd00:1122:3344:106::1,7547150,184993467858947,397024,397028,2026-07-28 22:34:58.710543+00,2026-07-28 22:35:59.404299+00
ba41c5cd-4d1f-4663-ac6f-aab16422daa3,172.20.52.132,0,65535,fd00:1122:3344:105::1,7547150,184993467858944,380655,380660,2026-07-24 22:31:07.557145+00,2026-07-24 22:31:38.516082+00
3ef07ec6-6882-41be-8683-5a9202b67fba,172.20.26.155,0,16383,fd00:1122:3344:105::1,6033221,184993467858951,382658,382660,2026-07-25 01:38:40.621018+00,2026-07-25 01:39:12.475842+00
7eafc4f3-0e13-450d-8a3f-1de0173a5019,172.20.26.155,16384,32767,fd00:1122:3344:104::1,6033221,184993467858946,384111,384120,2026-07-25 04:26:18.371376+00,2026-07-25 04:27:16.766128+00
91403e83-7291-4e57-b759-2807d3299e46,172.20.26.155,16384,32767,fd00:1122:3344:108::1,6033221,184993467858951,380973,380987,2026-07-24 22:59:39.989415+00,2026-07-24 23:00:47.58597+00
beb0bbec-83bc-4c3a-880d-814fbfe855f3,172.20.52.149,0,65535,fd00:1122:3344:102::1,15947946,184993467858944,397388,397454,2026-07-29 07:17:58.808884+00,2026-07-29 07:26:45.55816+00
c64acbaf-65b2-434c-a864-5fcd7b68c84f,172.20.26.85,0,16383,fd00:1122:3344:102::1,14363509,184993467878546,397176,NULL,2026-07-28 23:52:27.934612+00,NULL
1b1a3b33-3640-48dd-9064-d2299bf4d304,172.20.26.45,0,65535,fd00:1122:3344:10b::1,14363509,184993467878518,397473,NULL,2026-07-29 07:28:05.909338+00,NULL
59fd3ab1-a27f-4e41-8d56-27a261144bd4,172.20.52.95,0,65535,fd00:1122:3344:109::1,11580246,184993467858944,397337,NULL,2026-07-29 01:05:57.695747+00,NULL
64d1a3e8-95f7-4cbe-9588-ad8c8ef97c44,172.20.26.34,49152,65535,fd00:1122:3344:102::1,14363509,184993467878517,397234,NULL,2026-07-29 00:07:56.576463+00,NULL
1274bfd6-5376-4abb-859d-8b7dcffd8601,172.20.52.68,0,65535,fd00:1122:3344:10a::1,14363509,184993467878507,397200,NULL,2026-07-28 23:57:41.424732+00,NULL
8bd85157-3d73-41e0-9ac7-caad84fdd91d,10.10.32.5,0,65535,fd00:1122:3344:105::1,13834134,184993467858944,120013,NULL,2025-07-02 23:44:58.80565+00,NULL
b3a9597a-cb4a-47d0-b7e7-6fc5acdd0221,172.20.52.137,0,65535,fd00:1122:3344:108::1,14363509,184993467878500,397305,NULL,2026-07-29 00:51:47.828038+00,NULL
ce0a11c0-58ac-4c1d-b21c-ffee79a19729,172.20.26.58,0,65535,fd00:1122:3344:108::1,10877668,184993467858947,397144,NULL,2026-07-28 23:29:43.908239+00,NULL
d70c1b71-0c28-4afc-a7f7-b07c28fb775c,172.20.26.241,0,16383,fd00:1122:3344:10b::1,14363509,184993467878537,397313,NULL,2026-07-29 00:51:48.23887+00,NULL
19d2a46d-2803-46d5-afcc-bdbefaa8cd68,172.20.26.91,0,65535,fd00:1122:3344:127::1,1508093,184993468715800,397240,NULL,2026-07-29 00:07:56.74639+00,NULL
"#;

#[derive(Debug, PartialEq, Eq)]
struct NatEntry {
    id: Uuid,
    address: IpAddr,
    first_port: i32,
    last_port: i32,
    sled_address: IpAddr,
    vni: i32,
    mac: i64,
    version_added: i64,
    version_removed: Option<i64>,
    time_created: DateTime<Utc>,
    time_deleted: Option<DateTime<Utc>>,
}

impl NatEntry {
    fn parse_dogfood_test_data() -> Vec<Self> {
        fn parse_timestamp(s: &str) -> DateTime<Utc> {
            DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
                .unwrap()
                .with_timezone(&Utc)
        }

        let mut entries = Vec::new();
        for line in DOGFOOD_NAT_ENTRIES.split('\n') {
            if line.is_empty() {
                continue;
            }
            let cols = line.split(',').collect::<Vec<_>>();
            assert_eq!(cols.len(), 11);
            entries.push(Self {
                id: cols[0].parse().unwrap(),
                address: cols[1].parse().unwrap(),
                first_port: cols[2].parse().unwrap(),
                last_port: cols[3].parse().unwrap(),
                sled_address: cols[4].parse().unwrap(),
                vni: cols[5].parse().unwrap(),
                mac: cols[6].parse().unwrap(),
                version_added: cols[7].parse().unwrap(),
                version_removed: if cols[8] == "NULL" {
                    None
                } else {
                    Some(cols[8].parse().unwrap())
                },
                time_created: parse_timestamp(&cols[9]),
                time_deleted: if cols[10] == "NULL" {
                    None
                } else {
                    Some(parse_timestamp(cols[10]))
                },
            });
        }
        entries.sort_unstable_by_key(|e| e.id);
        assert_eq!(entries.len(), 37);
        entries
    }
}

pub(super) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    before_impl(ctx).boxed()
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    // clean up from any previous test
    ctx.client
        .batch_execute("DELETE FROM omicron.public.nat_entry WHERE 1=1;")
        .await
        .expect("deleted old entries");

    let stmt = ctx
        .client
        .prepare(
            "INSERT INTO omicron.public.nat_entry (
                id,external_address,first_port,last_port,sled_address,vni,mac,
                version_added,version_removed,time_created,time_deleted
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ",
        )
        .await
        .expect("prepared insert query");

    let entries = NatEntry::parse_dogfood_test_data();
    for entry in entries {
        ctx.client
            .execute(
                &stmt,
                &[
                    &entry.id,
                    &entry.address,
                    &entry.first_port,
                    &entry.last_port,
                    &entry.sled_address,
                    &entry.vni,
                    &entry.mac,
                    &entry.version_added,
                    &entry.version_removed,
                    &entry.time_created,
                    &entry.time_deleted,
                ],
            )
            .await
            .expect("inserted entry");
    }
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    after_impl(ctx).boxed()
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let rows = ctx
        .client
        .query(
            "SELECT
                id,external_address,first_port,last_port,sled_address,vni,mac,
                version_added,version_removed,time_created,time_deleted
             FROM omicron.public.nat_entry
             ORDER BY id",
            &[],
        )
        .await
        .expect("queried NAT entries");

    let after_entries = rows
        .iter()
        .map(|row| NatEntry {
            id: row.get("id"),
            address: row.get("external_address"),
            first_port: row.get("first_port"),
            last_port: row.get("last_port"),
            sled_address: row.get("sled_address"),
            vni: row.get("vni"),
            mac: row.get("mac"),
            version_added: row.get("version_added"),
            version_removed: row.get("version_removed"),
            time_created: row.get("time_created"),
            time_deleted: row.get("time_deleted"),
        })
        .collect::<Vec<_>>();

    // We should have deleted the 17 NAT entries with the service zone VNI,
    // leaving us with the 20 instance entries.
    assert_eq!(after_entries.len(), 20);

    let expected_entries = NatEntry::parse_dogfood_test_data()
        .into_iter()
        .filter(|e| e.vni as u32 != Vni::SERVICES_VNI.as_u32())
        .collect::<Vec<_>>();
    assert_eq!(after_entries, expected_entries);
}
