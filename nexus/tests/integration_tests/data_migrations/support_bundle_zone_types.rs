// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates the `support-bundle-zone-types` migration.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use uuid::Uuid;

// randomly-generated IDs
const BUNDLE_ID: &str = "708032ae-e64c-4051-a0bb-8f2f8e3d3d59";
const FM_SITREP_ID: &str = "ea51763c-2a2e-45c2-9d68-b4e196a10645";
const FM_REQUEST_ID: &str = "0e1de985-8b6d-4bfb-8ba4-9db02c2a2f2f";
const SLED_ID: &str = "9d219d1c-3a3f-4cdf-b1ea-a0cc86c637a4";

async fn before_impl(ctx: &MigrationContext<'_>) {
    // Insert pre-migration host-info selection rows, for a bundle and for an
    // FM support bundle request. The zone-type columns don't exist yet, so
    // only the sled-selection columns are populated.
    ctx.client
        .batch_execute(&format!(
            "
    -- Remove detritus from earlier migrations.
    DELETE FROM omicron.public.support_bundle_data_selection_host_info
        WHERE 1=1;
    DELETE FROM omicron.public.fm_support_bundle_request_data_selection_host_info
        WHERE 1=1;

    INSERT INTO omicron.public.support_bundle_data_selection_host_info (
        bundle_id, all_sleds, sled_ids
    )
    VALUES ('{BUNDLE_ID}', false, ARRAY['{SLED_ID}']::UUID[]);

    INSERT INTO omicron.public.fm_support_bundle_request_data_selection_host_info (
        sitrep_id, request_id, all_sleds, sled_ids
    )
    VALUES ('{FM_SITREP_ID}', '{FM_REQUEST_ID}', true, ARRAY[]::UUID[]);
            "
        ))
        .await
        .expect("inserted pre-migration data");
}

async fn after_impl(ctx: &MigrationContext<'_>) {
    let bundle_id: Uuid = BUNDLE_ID.parse().unwrap();
    let request_id: Uuid = FM_REQUEST_ID.parse().unwrap();

    // Both pre-existing rows must be backfilled to "all zone types": the
    // selection every bundle collected before zone-type filtering existed.
    let row = ctx
        .client
        .query_one(
            "
            SELECT all_zone_types, zone_types
            FROM omicron.public.support_bundle_data_selection_host_info
            WHERE bundle_id = $1
            ",
            &[&bundle_id],
        )
        .await
        .expect("queried post-migration bundle row");
    assert_eq!(row.get::<_, bool>("all_zone_types"), true);
    assert_eq!(row.get::<_, Vec<String>>("zone_types"), Vec::<String>::new());

    let row = ctx
        .client
        .query_one(
            "
            SELECT all_zone_types, zone_types
            FROM omicron.public.fm_support_bundle_request_data_selection_host_info
            WHERE request_id = $1
            ",
            &[&request_id],
        )
        .await
        .expect("queried post-migration FM request row");
    assert_eq!(row.get::<_, bool>("all_zone_types"), true);
    assert_eq!(row.get::<_, Vec<String>>("zone_types"), Vec::<String>::new());

    // The CHECK constraint must reject "all zone types" alongside a
    // specific list.
    let err = ctx
        .client
        .batch_execute(
            "
            INSERT INTO omicron.public.support_bundle_data_selection_host_info (
                bundle_id, all_sleds, sled_ids, all_zone_types, zone_types
            )
            VALUES (gen_random_uuid(), true, ARRAY[]::UUID[],
                    true, ARRAY['nexus']);
            ",
        )
        .await
        .expect_err("all_zone_types + a zone list violates the constraint");
    let db_err = err.as_db_error().expect("error came from the database");
    assert_eq!(
        db_err.constraint(),
        Some("all_zone_types_and_specific_zone_types_are_mutually_exclusive"),
        "got: {}",
        db_err.message(),
    );
}

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(before_impl(ctx))
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(after_impl(ctx))
}
