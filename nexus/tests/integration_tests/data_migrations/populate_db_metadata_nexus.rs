// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use nexus_test_utils::sql::AnySqlType;
use nexus_test_utils::sql::SqlEnum;
use nexus_test_utils::sql::process_rows;

const NEXUS_ID_185_0: &str = "387433f9-1473-4ca2-b156-9670452985e0";
const EXPUNGED_NEXUS_ID_185_0: &str = "287433f9-1473-4ca2-b156-9670452985e0";
const OLD_NEXUS_ID_185_0: &str = "187433f9-1473-4ca2-b156-9670452985e0";

const BP_ID_185_0: &str = "5a5ff941-3b5a-403b-9fda-db2049f4c736";
const OLD_BP_ID_185_0: &str = "4a5ff941-3b5a-403b-9fda-db2049f4c736";

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Create a blueprint which contains a Nexus - we'll use this for the migration.
        //
        // It also contains an exupnged Nexus, which should be ignored.
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.bp_target
                     (version, blueprint_id, enabled, time_made_target)
                     VALUES
                     (1, '{BP_ID_185_0}', true, now());",
                ),
                &[],
            )
            .await
            .expect("inserted bp_target rows for 182");
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.bp_omicron_zone (
                         blueprint_id, sled_id, id, zone_type,
                         primary_service_ip, primary_service_port,
                         second_service_ip, second_service_port,
                         dataset_zpool_name, bp_nic_id,
                         dns_gz_address, dns_gz_address_index,
                         ntp_ntp_servers, ntp_dns_servers, ntp_domain,
                         nexus_external_tls, nexus_external_dns_servers,
                         snat_ip, snat_first_port, snat_last_port,
                         external_ip_id, filesystem_pool, disposition,
                         disposition_expunged_as_of_generation,
                         disposition_expunged_ready_for_cleanup,
                         image_source, image_artifact_sha256
                     )
                     VALUES (
                         '{BP_ID_185_0}', gen_random_uuid(), '{NEXUS_ID_185_0}',
                         'nexus', '192.168.1.10', 8080, NULL, NULL, NULL, NULL,
                         NULL, NULL, NULL, NULL, NULL, false, ARRAY[]::INET[],
                         NULL, NULL, NULL, NULL, gen_random_uuid(),
                         'in_service', NULL, false, 'install_dataset', NULL
                     ),
                     (
                         '{BP_ID_185_0}', gen_random_uuid(),
                         '{EXPUNGED_NEXUS_ID_185_0}', 'nexus', '192.168.1.11',
                         8080, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                         NULL, false, ARRAY[]::INET[], NULL, NULL, NULL, NULL,
                         gen_random_uuid(), 'expunged', 1, false,
                         'install_dataset', NULL
                     );"
                ),
                &[],
            )
            .await
            .expect("inserted bp_omicron_zone rows for 182");

        // ALSO create an old blueprint, which isn't the latest target.
        //
        // We should ignore this one! No rows should be inserted for old data.
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.bp_target
                     (version, blueprint_id, enabled, time_made_target)
                     VALUES
                     (0, '{OLD_BP_ID_185_0}', true, now());",
                ),
                &[],
            )
            .await
            .expect("inserted bp_target rows for 182");
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.bp_omicron_zone (
                         blueprint_id, sled_id, id, zone_type,
                         primary_service_ip, primary_service_port,
                         second_service_ip, second_service_port,
                         dataset_zpool_name, bp_nic_id,
                         dns_gz_address, dns_gz_address_index,
                         ntp_ntp_servers, ntp_dns_servers, ntp_domain,
                         nexus_external_tls, nexus_external_dns_servers,
                         snat_ip, snat_first_port, snat_last_port,
                         external_ip_id, filesystem_pool, disposition,
                         disposition_expunged_as_of_generation,
                         disposition_expunged_ready_for_cleanup,
                         image_source, image_artifact_sha256
                     )
                     VALUES (
                         '{OLD_BP_ID_185_0}', gen_random_uuid(),
                         '{OLD_NEXUS_ID_185_0}', 'nexus', '192.168.1.10', 8080,
                         NULL, NULL, NULL, NULL,
                         NULL, NULL, NULL, NULL, NULL,
                         false, ARRAY[]::INET[], NULL, NULL, NULL,
                         NULL, gen_random_uuid(), 'in_service',
                         NULL, false, 'install_dataset', NULL
                     );"
                ),
                &[],
            )
            .await
            .expect("inserted bp_omicron_zone rows for 182");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // After the migration, the new row should be created - only for Nexuses
        // in the latest blueprint.
        //
        // Note that "OLD_NEXUS_ID_185_0" doesn't get a row - it's in an old
        // blueprint.
        let rows = ctx
            .client
            .query(
                "SELECT
                   nexus_id,
                   last_drained_blueprint_id,
                   state
                 FROM omicron.public.db_metadata_nexus;",
                &[],
            )
            .await
            .expect("queried post-migration inv_sled_config_reconciler");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        // Create a new row for the Nexuses in the target blueprint
        assert_eq!(
            row.values[0].expect("nexus_id").unwrap(),
            &AnySqlType::Uuid(NEXUS_ID_185_0.parse().unwrap())
        );
        assert_eq!(row.values[1].expect("last_drained_blueprint_id"), None);
        assert_eq!(
            row.values[2].expect("state").unwrap(),
            &AnySqlType::Enum(SqlEnum::from((
                "db_metadata_nexus_state",
                "active"
            )))
        );
    })
}
