// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

const PORT_SETTINGS_ID_165_0: &str = "1e700b64-79e0-4515-9771-bcc2391b6d4d";
const PORT_SETTINGS_ID_165_1: &str = "c6b015ff-1c98-474f-b9e9-dfc30546094f";
const PORT_SETTINGS_ID_165_2: &str = "8b777d9b-62a3-4c4d-b0b7-314315c2a7fc";
const PORT_SETTINGS_ID_165_3: &str = "7c675e89-74b1-45da-9577-cf75f028107a";
const PORT_SETTINGS_ID_165_4: &str = "e2413d63-9307-4918-b9c4-bce959c63042";
const PORT_SETTINGS_ID_165_5: &str = "05df929f-1596-42f4-b78f-aebb5d7028c4";

// Insert records using the `local_pref` column before it's renamed and its
// database type is changed from INT8 to INT2. The receiving Rust type is u8
// so 2 records are outside the u8 range, 2 records are at the edge of the u8
// range, and 1 record is within the u8 range.
pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(&format!("
                INSERT INTO omicron.public.switch_port_settings_route_config
                  (port_settings_id, interface_name, dst, gw, vid, local_pref)
                VALUES
                  (
                    '{PORT_SETTINGS_ID_165_0}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, -1
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_1}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, 0
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_2}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, 128
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_3}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, 255
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_4}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, 256
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_5}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, NULL
                  );
              "),
            )
            .await
            .expect("failed to insert pre-migration rows for 165");
    })
}

// Query the records using the new `rib_priority` column and assert that the
// values were correctly clamped within the u8 range.
fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT * FROM omicron.public.switch_port_settings_route_config;",
                &[],
            )
            .await
            .expect("failed to query post-migration switch_port_settings_route_config table");
        assert_eq!(rows.len(), 6);

        let records: HashMap<Uuid, Option<i16>> = HashMap::from([
            (Uuid::from_str(PORT_SETTINGS_ID_165_0).unwrap(), Some(0)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_1).unwrap(), Some(0)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_2).unwrap(), Some(128)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_3).unwrap(), Some(255)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_4).unwrap(), Some(255)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_5).unwrap(), None),
        ]);

        for row in rows {
            let port_settings_id = row.get::<&str, Uuid>("port_settings_id");
            let rib_priority_got = row.get::<&str, Option<i16>>("rib_priority");

            let rib_priority_want = records
                .get(&port_settings_id)
                .expect("unexpected port_settings_id value when querying switch_port_settings_route_config");
            assert_eq!(rib_priority_got, *rib_priority_want);
        }
    })
}
