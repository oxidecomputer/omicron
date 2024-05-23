-- We need to manually rebuild a compliant set of routes.
-- Remove everything that exists today.
DELETE FROM omicron.public.router_route WHERE 1=1;

-- Insert fixed_data routes for the services VPC.
INSERT INTO omicron.public.router_route
    (
        id, name,
        description,
        time_created, time_modified,
        vpc_router_id, kind,
        target, destination
    )
VALUES
    (
        '001de000-074c-4000-8000-000000000002', 'default-v4',
        'Default internet gateway route for Oxide Services',
        now(), now(),
        '001de000-074c-4000-8000-000000000001', 'default',
        'inetgw:outbound', 'ipnet:0.0.0.0/0'
    ),
    (
        '001de000-074c-4000-8000-000000000003', 'default-v6',
        'Default internet gateway route for Oxide Services',
        now(), now(),
        '001de000-074c-4000-8000-000000000001', 'default',
        'inetgw:outbound', 'ipnet:::/0'
    ),
    (
        '001de000-c470-4000-8000-000000000004', 'sn-external-dns',
        'Built-in VPC Subnet for Oxide service (external-dns)',
        now(), now(),
        '001de000-074c-4000-8000-000000000001', 'vpc_subnet',
        'subnet:external-dns', 'subnet:external-dns'
    ),
    (
        '001de000-c470-4000-8000-000000000005', 'sn-nexus',
        'Built-in VPC Subnet for Oxide service (nexus)',
        now(), now(),
        '001de000-074c-4000-8000-000000000001', 'vpc_subnet',
        'subnet:nexus', 'subnet:nexus'
    ),
    (
        '001de000-c470-4000-8000-000000000006', 'sn-boundary-ntp',
        'Built-in VPC Subnet for Oxide service (nexus)',
        now(), now(),
        '001de000-074c-4000-8000-000000000001', 'vpc_subnet',
        'subnet:boundary-ntp', 'subnet:boundary-ntp'
    )
ON CONFLICT DO NOTHING;

-- Insert gateway routes for user VPCs.
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
