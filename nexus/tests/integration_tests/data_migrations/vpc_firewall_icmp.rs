// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use nexus_test_utils::sql::AnySqlType;
use nexus_test_utils::sql::ColumnValue;
use nexus_test_utils::sql::process_rows;
use uuid::Uuid;

const VPC: Uuid = Uuid::from_u128(0x1111566c_5c3d_4647_83b0_8f3515da7be1);
const FW_RULE: Uuid = Uuid::from_u128(0x11117213_5c3d_4647_83b0_8f3515da7be1);

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        use nexus_db_queries::db::fixed_data::project::*;
        use nexus_db_queries::db::fixed_data::vpc::*;
        use omicron_common::address::SERVICE_VPC_IPV6_PREFIX;

        // First, create the oxide-services vpc. The migration will only add
        // a new rule if it detects the presence of *this* VPC entry (i.e.,
        // RSS has run before).
        let svc_vpc = *SERVICES_VPC_ID;
        let svc_proj = *SERVICES_PROJECT_ID;
        let svc_router = *SERVICES_VPC_ROUTER_ID;
        let svc_vpc_name = &SERVICES_DB_NAME;
        ctx.client
            .batch_execute(&format!(
                "INSERT INTO vpc (
                    id, name, description, time_created, time_modified,
                    project_id, system_router_id, dns_name,
                    vni, ipv6_prefix, firewall_gen, subnet_gen
                )
                VALUES (
                    '{svc_vpc}', '{svc_vpc_name}', '', now(), now(),
                    '{svc_proj}', '{svc_router}', '{svc_vpc_name}',
                    100, '{}', 0, 0
                );",
                *SERVICE_VPC_IPV6_PREFIX
            ))
            .await
            .expect("failed to create services vpc record");

        // Second, create a firewall rule in a dummied-out VPC to validate
        // that we correctly convert from ENUM[] to STRING[].
        ctx.client
            .batch_execute(&format!(
                "INSERT INTO vpc_firewall_rule (
                    id,
                    name, description,
                    time_created, time_modified, vpc_id, status, direction,
                    targets, filter_protocols,
                    action, priority
                )
                VALUES (
                    '{FW_RULE}',
                    'test-fw', '',
                    now(), now(), '{VPC}', 'enabled', 'outbound',
                    ARRAY['vpc:fiction'], ARRAY['ICMP', 'TCP', 'UDP'],
                    'allow', 1234
                );"
            ))
            .await
            .expect("failed to create firewall rule");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        use nexus_db_queries::db::fixed_data::vpc::*;
        // Firstly -- has the new firewall rule been added?
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, description
                    FROM vpc_firewall_rule
                    WHERE name = 'nexus-icmp' and vpc_id = '{}'",
                    *SERVICES_VPC_ID
                ),
                &[],
            )
            .await
            .expect("failed to load instance auto-restart policies");
        let records = process_rows(&rows);

        assert_eq!(records.len(), 1);

        // Secondly, have FW_RULE's filter_protocols been correctly stringified?
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT filter_protocols
                    FROM vpc_firewall_rule
                    WHERE id = '{FW_RULE}'"
                ),
                &[],
            )
            .await
            .expect("failed to load instance auto-restart policies");
        let records = process_rows(&rows);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].values,
            vec![ColumnValue::new(
                "filter_protocols",
                AnySqlType::TextArray(vec![
                    "icmp".into(),
                    "tcp".into(),
                    "udp".into(),
                ])
            )],
        );
    })
}
