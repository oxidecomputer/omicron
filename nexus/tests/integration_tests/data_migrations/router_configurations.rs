// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Migration 299 (`router-configurations`) seeds every pre-existing,
//! live, non-internal silo with the built-in `default-switch0` router
//! configuration at priority 1000 (RFD 662 upgrade policy). Fresh silos
//! created afterwards get nothing; the seed is idempotent.

use super::super::schema::{DataMigrationFns, MigrationContext};
use futures::future::BoxFuture;
use uuid::Uuid;

// Silos present before the migration: two ordinary live silos, one deleted
// silo, and the internal silo (which never receives an assignment).
const LIVE_A: Uuid = Uuid::from_u128(0x29900001_0000_0000_0000_000000000001);
const LIVE_B: Uuid = Uuid::from_u128(0x29900001_0000_0000_0000_000000000002);
const DELETED: Uuid = Uuid::from_u128(0x29900001_0000_0000_0000_000000000003);
const INTERNAL_SILO: &str = "001de000-5110-4000-8000-000000000001";
const DEFAULT_SWITCH0: &str = "001de000-defa-4000-8000-000000000000";

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.silo
                    (id, name, description, time_created, time_modified,
                     time_deleted, discoverable, authentication_mode,
                     user_provision_type, mapped_fleet_roles, rcgen)
                VALUES
                    ('{LIVE_A}', 'rc-mig-a', 'live silo a', now(), now(),
                     NULL, true, 'local', 'api_only', '{{}}', 0),
                    ('{LIVE_B}', 'rc-mig-b', 'live silo b', now(), now(),
                     NULL, true, 'local', 'api_only', '{{}}', 0),
                    ('{DELETED}', 'rc-mig-deleted', 'deleted silo', now(),
                     now(), now(), true, 'local', 'api_only', '{{}}', 0),
                    ('{INTERNAL_SILO}', 'oxide-internal', 'internal silo',
                     now(), now(), NULL, false, 'local', 'api_only', '{{}}', 0)
                ON CONFLICT (id) DO NOTHING;
                "
            ))
            .await
            .expect("failed to insert silos before migration 299");
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT silo_id, router_configuration_id, priority
                 FROM omicron.public.silo_router_configuration
                 ORDER BY silo_id",
                &[],
            )
            .await
            .expect("failed to read silo router configurations");
        let seeded: Vec<(Uuid, Uuid, i32)> = rows
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect();
        let default_switch0: Uuid = DEFAULT_SWITCH0.parse().unwrap();
        // Exactly one seed per live ordinary silo (the harness applies each
        // migration step twice, so this also proves idempotence); nothing
        // for the deleted silo or the internal silo.
        assert_eq!(
            seeded,
            vec![
                (LIVE_A, default_switch0, 1000i32),
                (LIVE_B, default_switch0, 1000i32),
            ]
        );

        // Clean up so later checks see the same table as before.
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.silo_router_configuration
                    WHERE silo_id IN ('{LIVE_A}', '{LIVE_B}');
                DELETE FROM omicron.public.silo
                    WHERE id IN ('{LIVE_A}', '{LIVE_B}', '{DELETED}');
                "
            ))
            .await
            .expect("failed to clean up migration 299 fixtures");
    })
}
