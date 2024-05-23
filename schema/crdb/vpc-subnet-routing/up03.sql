set local disallow_full_table_scans = off;

-- We need to manually rebuild a compliant set of routes.
-- Remove everything that exists today.
DELETE FROM omicron.public.router_route WHERE 1=1;

-- Insert gateway routes for all VPCs.
INSERT INTO omicron.public.router_route
    (
        id, name,
        description,
        time_created, time_modified,
        vpc_router_id, kind,
        target, destination
    )
SELECT
    gen_random_uuid(), 'default-v4',
    'The default route of a vpc',
    now(), now(),
    omicron.public.vpc_router.id, 'default',
    'inetgw:outbound', 'ipnet:0.0.0.0/0'
FROM
    omicron.public.vpc_router
ON CONFLICT DO NOTHING;

INSERT INTO omicron.public.router_route
    (
        id, name,
        description,
        time_created, time_modified,
        vpc_router_id, kind,
        target, destination
    )
SELECT
    gen_random_uuid(), 'default-v6',
    'The default route of a vpc',
    now(), now(),
    omicron.public.vpc_router.id, 'default',
    'inetgw:outbound', 'ipnet:::/0'
FROM
    omicron.public.vpc_router
ON CONFLICT DO NOTHING;

-- Insert subnet routes for every defined VPC subnet.
INSERT INTO omicron.public.router_route
    (
        id, name,
        description,
        time_created, time_modified,
        vpc_router_id, kind,
        target, destination
    )
SELECT
    gen_random_uuid(), 'sn-' || vpc_subnet.name,
    'VPC Subnet route for ''' || vpc_subnet.name || '''',
    now(), now(),
    omicron.public.vpc_router.id, 'default',
    'subnet:' || vpc_subnet.name, 'subnet:' || vpc_subnet.name
FROM
    (omicron.public.vpc_subnet JOIN omicron.public.vpc
        ON vpc_subnet.vpc_id = vpc.id) JOIN omicron.public.vpc_router
            ON vpc_router.vpc_id = vpc.id
ON CONFLICT DO NOTHING;

-- Replace IDs of fixed_data routes for the services VPC.
-- This is done instead of an insert to match the initial
-- empty state of dbinit.sql.
WITH known_ids (new_id, new_name, new_description) AS (
    VALUES
        (
            '001de000-074c-4000-8000-000000000002', 'default-v4',
            'Default internet gateway route for Oxide Services'
        ),
        (
            '001de000-074c-4000-8000-000000000003', 'default-v6',
            'Default internet gateway route for Oxide Services'
        ),
        (
            '001de000-c470-4000-8000-000000000004', 'sn-external-dns',
            'Built-in VPC Subnet for Oxide service (external-dns)'
        ),
        (
            '001de000-c470-4000-8000-000000000005', 'sn-nexus',
            'Built-in VPC Subnet for Oxide service (nexus)'
        ),
        (
            '001de000-c470-4000-8000-000000000006', 'sn-boundary-ntp',
            'Built-in VPC Subnet for Oxide service (boundary-ntp)'
        )
)
UPDATE omicron.public.router_route
SET
    id = CAST(new_id AS UUID),
    description = new_description
FROM known_ids
WHERE vpc_router_id = '001de000-074c-4000-8000-000000000001' AND new_name = router_route.name;
