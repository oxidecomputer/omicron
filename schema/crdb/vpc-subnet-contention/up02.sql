set local disallow_full_table_scans = off;

-- Remove all existing VPC Subnet routes.
-- These are fully managed by nexus, so no user state is at
-- risk of being lost.
DELETE FROM omicron.public.router_route
WHERE router_route.kind = 'vpc_subnet';

-- Insert subnet routes for every defined VPC subnet.
-- Do this first for the majority of routes...
INSERT INTO omicron.public.router_route
    (
        id, name,
        description,
        time_created, time_modified,
        vpc_router_id, kind,
        target, destination,
        vpc_subnet_id
    )
SELECT
    gen_random_uuid(), vpc_subnet.name,
    'System-managed VPC Subnet route.',
    now(), now(),
    omicron.public.vpc_router.id, 'vpc_subnet',
    'subnet:' || vpc_subnet.name, 'subnet:' || vpc_subnet.name,
    vpc_subnet.id
FROM
    (omicron.public.vpc_subnet JOIN omicron.public.vpc
        ON vpc_subnet.vpc_id = vpc.id) JOIN omicron.public.vpc_router
            ON (vpc_router.vpc_id = vpc.id AND vpc_router.id = vpc.system_router_id)
WHERE
    vpc_router.kind = 'system' AND vpc_subnet.time_deleted IS NULL AND
    vpc_subnet.name != 'default-v4' AND vpc_subnet.name != 'default-v6'
ON CONFLICT DO NOTHING;

-- ...and then revisit any data which has unfortunately
-- titled their subnets 'default-v4' or 'default-v6'.
INSERT INTO omicron.public.router_route
    (
        id,
        name,
        description,
        time_created, time_modified,
        vpc_router_id, kind,
        target, destination,
        vpc_subnet_id
    )
SELECT
    gen_random_uuid(),
    'conflict-' || vpc_subnet.name || '-' || gen_random_uuid(),
    'System-managed VPC Subnet route.',
    now(), now(),
    omicron.public.vpc_router.id, 'vpc_subnet',
    'subnet:' || vpc_subnet.name, 'subnet:' || vpc_subnet.name,
    vpc_subnet.id
FROM
    (omicron.public.vpc_subnet JOIN omicron.public.vpc
        ON vpc_subnet.vpc_id = vpc.id) JOIN omicron.public.vpc_router
            ON (vpc_router.vpc_id = vpc.id AND vpc_router.id = vpc.system_router_id)
WHERE
    vpc_router.kind = 'system' AND vpc_subnet.time_deleted IS NULL AND
    (vpc_subnet.name = 'default-v4' OR vpc_subnet.name = 'default-v6')
ON CONFLICT DO NOTHING;

-- Replace IDs of fixed_data VPC Subnet routes for the services VPC.
WITH known_ids (new_id, new_name) AS (
    VALUES
        (
            '001de000-c470-4000-8000-000000000004', 'external-dns'
        ),
        (
            '001de000-c470-4000-8000-000000000005', 'nexus'
        ),
        (
            '001de000-c470-4000-8000-000000000006', 'boundary-ntp'
        )
)
UPDATE omicron.public.router_route
SET
    id = CAST(new_id AS UUID)
FROM known_ids
WHERE vpc_router_id = '001de000-074c-4000-8000-000000000001' AND new_name = router_route.name;
