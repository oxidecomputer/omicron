// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `support-bundle-sp-components` migration, which backfills
//! the new `include_sp_components` flag as `false` for existing data
//! selections.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

const BUNDLE_ID: Uuid = Uuid::from_u128(0x30900001_0000_0000_0000_000000000001);

const SITREP_ID: Uuid = Uuid::from_u128(0x30900002_0000_0000_0000_000000000001);
const REQUEST_ID: Uuid =
    Uuid::from_u128(0x30900003_0000_0000_0000_000000000001);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

async fn before_impl(ctx: &MigrationContext<'_>) {
    ctx.client
        .batch_execute(&format!(
            "
                INSERT INTO omicron.public.support_bundle_data_selection_flags (
                    bundle_id, include_reconfigurator,
                    include_sled_cubby_info, include_sp_dumps
                ) VALUES
                    ('{BUNDLE_ID}', true, true, true);

                INSERT INTO
                    omicron.public.fm_support_bundle_request_data_selection_flags (
                    sitrep_id, request_id, include_reconfigurator,
                    include_sled_cubby_info, include_sp_dumps
                ) VALUES
                    ('{SITREP_ID}', '{REQUEST_ID}', true, true, true);
            "
        ))
        .await
        .expect("migration 309 test data insertion should succeed");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let bundle_flag: bool = ctx
        .client
        .query_one(
            &format!(
                "
                SELECT include_sp_components
                FROM omicron.public.support_bundle_data_selection_flags
                WHERE bundle_id = '{BUNDLE_ID}'
                "
            ),
            &[],
        )
        .await
        .expect("migration 309 bundle flags query should succeed")
        .get("include_sp_components");
    assert_eq!(bundle_flag, false);

    let request_flag: bool = ctx
        .client
        .query_one(
            &format!(
                "
                SELECT include_sp_components
                FROM
                    omicron.public.fm_support_bundle_request_data_selection_flags
                WHERE sitrep_id = '{SITREP_ID}' AND request_id = '{REQUEST_ID}'
                "
            ),
            &[],
        )
        .await
        .expect("migration 309 FM request flags query should succeed")
        .get("include_sp_components");
    assert_eq!(request_flag, false);

    // Now that the test has passed, clean up after ourselves:
    ctx.client
        .batch_execute(&format!(
            "
                DELETE FROM omicron.public.support_bundle_data_selection_flags
                WHERE bundle_id = '{BUNDLE_ID}';

                DELETE FROM
                    omicron.public.fm_support_bundle_request_data_selection_flags
                WHERE sitrep_id = '{SITREP_ID}' AND request_id = '{REQUEST_ID}';
            "
        ))
        .await
        .expect("migration 309 test data cleanup should succeed");
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(before_impl(ctx))
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(after_impl(ctx))
}
