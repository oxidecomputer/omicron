// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use nexus_test_utils::sql::AnySqlType;

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Create test data in inv_sled_config_reconciler table before the new columns are added.
        ctx.client
            .execute(
                "INSERT INTO omicron.public.inv_sled_config_reconciler
                 (inv_collection_id, sled_id, last_reconciled_config, boot_disk_slot)
                 VALUES
                 ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222',
                  '33333333-3333-3333-3333-333333333333', 0);",
                &[],
            )
            .await
            .expect("inserted pre-migration rows for 171");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // After the migration, the new columns should exist and be NULL for existing rows.
        let rows = ctx
            .client
            .query(
                "SELECT
                   clear_mupdate_override_boot_success,
                   clear_mupdate_override_boot_error,
                   clear_mupdate_override_non_boot_message
                 FROM omicron.public.inv_sled_config_reconciler
                 WHERE sled_id = '22222222-2222-2222-2222-222222222222';",
                &[],
            )
            .await
            .expect("queried post-migration inv_sled_config_reconciler");
        assert_eq!(rows.len(), 1);

        // All new columns should be NULL for existing rows.
        let boot_success: Option<AnySqlType> =
            (&rows[0]).get("clear_mupdate_override_boot_success");
        assert!(boot_success.is_none());

        let boot_error: Option<String> =
            (&rows[0]).get("clear_mupdate_override_boot_error");
        assert!(boot_error.is_none());

        let non_boot_message: Option<String> =
            (&rows[0]).get("clear_mupdate_override_non_boot_message");
        assert!(non_boot_message.is_none());

        // Test that the constraint allows valid combinations.
        // Case 1: All NULL (should work).
        ctx.client
            .execute(
                "INSERT INTO omicron.public.inv_sled_config_reconciler
                 (inv_collection_id, sled_id, last_reconciled_config, boot_disk_slot,
                  clear_mupdate_override_boot_success, clear_mupdate_override_boot_error,
                  clear_mupdate_override_non_boot_message)
                 VALUES
                 ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '33333333-3333-3333-3333-333333333333',
                  'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 1, NULL, NULL, NULL);",
                &[],
            )
            .await
            .expect("inserted row with all NULL clear_mupdate_override columns");

        // Case 2: Success case (boot_success NOT NULL, boot_error NULL, non_boot_message NOT NULL).
        ctx.client
            .execute(
                "INSERT INTO omicron.public.inv_sled_config_reconciler
                 (inv_collection_id, sled_id, last_reconciled_config, boot_disk_slot,
                  clear_mupdate_override_boot_success, clear_mupdate_override_boot_error,
                  clear_mupdate_override_non_boot_message)
                 VALUES
                 ('cccccccc-cccc-cccc-cccc-cccccccccccc', '44444444-4444-4444-4444-444444444444',
                  'dddddddd-dddd-dddd-dddd-dddddddddddd', 0,
                  'cleared', NULL, 'Non-boot disk cleared successfully');",
                &[],
            )
            .await
            .expect("inserted row with success case clear_mupdate_override columns");

        // Case 3: Error case (boot_success NULL, boot_error NOT NULL, non_boot_message NOT NULL).
        ctx.client
            .execute(
                "INSERT INTO omicron.public.inv_sled_config_reconciler
                 (inv_collection_id, sled_id, last_reconciled_config, boot_disk_slot,
                  clear_mupdate_override_boot_success, clear_mupdate_override_boot_error,
                  clear_mupdate_override_non_boot_message)
                 VALUES
                 ('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', '55555555-5555-5555-5555-555555555555',
                  'ffffffff-ffff-ffff-ffff-ffffffffffff', 1,
                  NULL, 'Failed to clear mupdate override', 'Non-boot disk operation failed');",
                &[],
            )
            .await
            .expect("inserted row with error case clear_mupdate_override columns");
    })
}
