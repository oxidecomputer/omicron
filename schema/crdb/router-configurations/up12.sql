/*
 * Seed every pre-existing silo (except the internal silo) with the built-in
 * "default" router configuration at priority 1000, preserving the
 * pre-modular-routing behavior (all guest traffic egresses via the default
 * router). Silos created after this migration are born with an empty list.
 *
 * The router_configuration row itself (id 001de000-defa-...) is created by
 * the Nexus populate step at startup; silo_router_configuration has no FK,
 * so inserting the assignments first is safe.
 */
SET LOCAL disallow_full_table_scans = 'off';
INSERT INTO omicron.public.silo_router_configuration
    (silo_id, router_configuration_id, priority)
SELECT
    id,
    '001de000-defa-4000-8000-000000000000',
    1000
FROM omicron.public.silo
WHERE
    time_deleted IS NULL AND
    id != '001de000-5110-4000-8000-000000000001'
ON CONFLICT DO NOTHING;
