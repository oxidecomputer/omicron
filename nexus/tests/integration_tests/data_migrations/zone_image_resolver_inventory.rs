// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use nexus_test_utils::sql::AnySqlType;
use uuid::Uuid;

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Create some fake inventory data to test the zone image resolver migration.
        // Insert a sled agent record without the new zone image resolver columns.
        let sled_id = Uuid::new_v4();
        let inv_collection_id = Uuid::new_v4();

        ctx.client
            .batch_execute(&format!(
                "
        INSERT INTO omicron.public.inv_sled_agent
          (inv_collection_id, time_collected, source, sled_id, sled_agent_ip,
           sled_agent_port, sled_role, usable_hardware_threads, usable_physical_ram,
           reservoir_size, reconciler_status_kind)
        VALUES
          ('{inv_collection_id}', now(), 'test-source', '{sled_id}', '192.168.1.1',
           8080, 'gimlet', 32, 68719476736, 1073741824, 'not-yet-run');
        ",
                inv_collection_id = inv_collection_id,
                sled_id = sled_id
            ))
            .await
            .expect("inserted pre-migration inv_sled_agent data");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Verify that the zone image resolver columns have been added with
        // correct defaults.
        let rows = ctx
            .client
            .query(
                "SELECT zone_manifest_boot_disk_path, zone_manifest_source,
                        zone_manifest_mupdate_id, zone_manifest_boot_disk_error,
                        mupdate_override_boot_disk_path,
                        mupdate_override_id, mupdate_override_boot_disk_error
                 FROM omicron.public.inv_sled_agent
                 ORDER BY time_collected",
                &[],
            )
            .await
            .expect("queried post-migration inv_sled_agent data");

        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        // Check that path fields have the expected default message.
        let zone_manifest_path: String =
            row.get("zone_manifest_boot_disk_path");
        let mupdate_override_path: String =
            row.get("mupdate_override_boot_disk_path");
        assert_eq!(zone_manifest_path, "old-collection-data-missing");
        assert_eq!(mupdate_override_path, "old-collection-data-missing");

        // Check that the zone manifest and mupdate override source fields are
        // NULL.
        let zone_manifest_source: Option<AnySqlType> =
            row.get("zone_manifest_source");
        assert_eq!(zone_manifest_source, None);
        let zone_manifest_id: Option<Uuid> =
            row.get("zone_manifest_mupdate_id");
        let mupdate_override_id: Option<Uuid> = row.get("mupdate_override_id");
        assert_eq!(zone_manifest_id, None);
        assert_eq!(mupdate_override_id, None);

        // Check that error fields have the expected default message.
        let zone_manifest_error: String =
            row.get("zone_manifest_boot_disk_error");
        let mupdate_override_error: String =
            row.get("mupdate_override_boot_disk_error");
        assert_eq!(zone_manifest_error, "old collection, data missing");
        assert_eq!(mupdate_override_error, "old collection, data missing");
    })
}
