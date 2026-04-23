// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::integration_tests::schema::DataMigrationFns;
use crate::integration_tests::schema::MigrationContext;
use futures::future::BoxFuture;
use nexus_networking::NEXUS_VPC_FW_RULE_NAME;
use nexus_test_utils::sql::process_rows;
use uuid::Uuid;

// A fake customer VPC ID, distinct from the built-in services VPC.
const CUSTOMER_VPC: Uuid =
    Uuid::from_u128(0xaaaaeeee_aaaa_bbbb_cccc_ddddddddddd1);
// IDs for the two nexus-inbound rules we'll insert.
const SVC_RULE: Uuid = Uuid::from_u128(0xaaaaeeee_aaaa_bbbb_cccc_ddddddddddd2);
const CUSTOMER_RULE: Uuid =
    Uuid::from_u128(0xaaaaeeee_aaaa_bbbb_cccc_ddddddddddd3);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        use nexus_db_queries::db::fixed_data::vpc::SERVICES_VPC_ID;

        let svc_vpc = *SERVICES_VPC_ID;

        // Insert a "nexus-inbound" rule in the built-in services VPC,
        // simulating what Nexus seeds on first boot. This is the row the
        // migration should delete.
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.vpc_firewall_rule (
                        id, name, description,
                        time_created, time_modified,
                        vpc_id, status, direction,
                        targets, action, priority
                    ) VALUES (
                        '{SVC_RULE}', '{NEXUS_VPC_FW_RULE_NAME}',
                        'default allow for nexus', now(), now(),
                        '{svc_vpc}', 'enabled', 'inbound',
                        ARRAY['subnet:nexus'], 'allow', 65534
                    );"
                ),
                &[],
            )
            .await
            .expect("inserted nexus-inbound rule in services VPC");

        // Also insert a "nexus-inbound" rule in a separate customer VPC.
        // The migration must NOT delete this row.
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.vpc_firewall_rule (
                        id, name, description,
                        time_created, time_modified,
                        vpc_id, status, direction,
                        targets, action, priority
                    ) VALUES (
                        '{CUSTOMER_RULE}', '{NEXUS_VPC_FW_RULE_NAME}',
                        'customer rule', now(), now(),
                        '{CUSTOMER_VPC}', 'enabled', 'inbound',
                        ARRAY['subnet:web'], 'allow', 100
                    );"
                ),
                &[],
            )
            .await
            .expect("inserted nexus-inbound rule in customer VPC");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        use nexus_db_queries::db::fixed_data::vpc::SERVICES_VPC_ID;

        let svc_vpc = *SERVICES_VPC_ID;

        // The services VPC "nexus-inbound" rule should have been soft-deleted:
        // no active (time_deleted IS NULL) rows remain.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id FROM omicron.public.vpc_firewall_rule
                     WHERE name = '{NEXUS_VPC_FW_RULE_NAME}'
                     AND vpc_id = '{svc_vpc}'
                     AND time_deleted IS NULL;"
                ),
                &[],
            )
            .await
            .expect("queried active nexus-inbound rules in services VPC");
        let records = process_rows(&rows);
        assert_eq!(
            records.len(),
            0,
            "expected nexus-inbound rule in services VPC to be soft-deleted"
        );

        // And there should be exactly one soft-deleted row.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id FROM omicron.public.vpc_firewall_rule
                     WHERE name = '{NEXUS_VPC_FW_RULE_NAME}'
                     AND vpc_id = '{svc_vpc}'
                     AND time_deleted IS NOT NULL;"
                ),
                &[],
            )
            .await
            .expect("queried soft-deleted nexus-inbound rules in services VPC");
        let records = process_rows(&rows);
        assert_eq!(
            records.len(),
            1,
            "expected exactly one soft-deleted nexus-inbound rule in services VPC"
        );

        // The customer VPC "nexus-inbound" rule must remain active and untouched.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id FROM omicron.public.vpc_firewall_rule
                     WHERE name = '{NEXUS_VPC_FW_RULE_NAME}'
                     AND vpc_id = '{CUSTOMER_VPC}'
                     AND time_deleted IS NULL;"
                ),
                &[],
            )
            .await
            .expect("queried nexus-inbound rules in customer VPC");
        let records = process_rows(&rows);
        assert_eq!(
            records.len(),
            1,
            "expected nexus-inbound rule in customer VPC to be preserved"
        );

        // Clean up: delete the customer rule so subsequent migration checks
        // aren't affected.
        ctx.client
            .execute(
                &format!(
                    "DELETE FROM omicron.public.vpc_firewall_rule
                     WHERE id = '{CUSTOMER_RULE}';"
                ),
                &[],
            )
            .await
            .expect("cleaned up customer nexus-inbound rule");
    })
}
