// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use uuid::Uuid;

const BP_OXIMETER_READ_POLICY_ID_0: &str =
    "5cb42909-d94a-4903-be72-330eea0325d9";
const BP_OXIMETER_READ_POLICY_ID_1: &str =
    "142b62c2-9348-4530-9eed-7077351fb94b";
const BP_OXIMETER_READ_POLICY_ID_2: &str =
    "3b5b7861-03aa-420a-a057-0a14347dc4c0";
const BP_OXIMETER_READ_POLICY_ID_3: &str =
    "de7ab4c0-30d4-4e9e-b620-3a959a9d59dd";

// Insert two blueprints and 4 oximeter read policies, two of which do not have
// a corresponding blueprint
pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(
                &format!("
        INSERT INTO omicron.public.blueprint
          (id, parent_blueprint_id, time_created, creator, comment, internal_dns_version, external_dns_version, cockroachdb_fingerprint, cockroachdb_setting_preserve_downgrade, target_release_minimum_generation)
        VALUES
          (
            '{BP_OXIMETER_READ_POLICY_ID_0}', NULL, now(), 'bob', 'hi', 1, 1, 'fingerprint', NULL, 1
          ),
          (
            '{BP_OXIMETER_READ_POLICY_ID_1}', NULL, now(), 'bab', 'hi', 1, 1, 'fingerprint', NULL, 1
          );

        INSERT INTO omicron.public.bp_oximeter_read_policy
          (blueprint_id, version, oximeter_read_mode)
        VALUES
          ('{BP_OXIMETER_READ_POLICY_ID_0}', 1, 'cluster'),
          ('{BP_OXIMETER_READ_POLICY_ID_1}', 2, 'cluster'),
          ('{BP_OXIMETER_READ_POLICY_ID_2}', 3, 'cluster'),
          ('{BP_OXIMETER_READ_POLICY_ID_3}', 4, 'cluster')
        "),
            )
            .await
            .expect("failed to insert pre-migration rows for 163");
    })
}

// Validate that rows that do not have a corresponding blueprint are gone
fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT blueprint_id FROM omicron.public.bp_oximeter_read_policy ORDER BY blueprint_id;",
                &[],
            )
            .await
            .expect("failed to query post-migration bp_oximeter_read_policy table");
        assert_eq!(rows.len(), 2);

        let id_1: Uuid = (&rows[0]).get::<&str, Uuid>("blueprint_id");
        assert_eq!(id_1.to_string(), BP_OXIMETER_READ_POLICY_ID_1);

        let id_2: Uuid = (&rows[1]).get::<&str, Uuid>("blueprint_id");
        assert_eq!(id_2.to_string(), BP_OXIMETER_READ_POLICY_ID_0);
    })
}
