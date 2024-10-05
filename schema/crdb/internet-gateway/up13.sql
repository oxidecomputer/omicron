-- Add default gateway for services

set local disallow_full_table_scans = off;

-- delete old style default routes
DELETE FROM omicron.public.router_route
       WHERE target = 'inetgw:outbound';

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
    'inetgw:default', 'ipnet:0.0.0.0/0'
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
    'inetgw:default', 'ipnet:::/0'
FROM
    omicron.public.vpc_router
ON CONFLICT DO NOTHING;

-- insert default internet gateways for all VPCs

INSERT INTO omicron.public.internet_gateway
    (
        id, name,
        description,
        time_created, time_modified, time_deleted,
        vpc_id,
        rcgen,
        resolved_version
    )
SELECT
    gen_random_uuid(), 'default',
    'Default VPC gateway',
    now(), now(), NULL,
    omicron.public.vpc_router.id,
    0,
    0
FROM
    omicron.public.vpc_router
ON CONFLICT DO NOTHING;

-- link default gateways to default ip pools

INSERT INTO omicron.public.internet_gateway_ip_pool
    (
        id, name,
        description,
        time_created, time_modified, time_deleted,
        internet_gateway_id,
        ip_pool_id
    )
SELECT
    gen_random_uuid(), 'default',
    'Default internet gateway IP pool',
    now(), now(), NULL,
    igw.id,
    ipp.ip_pool_id
FROM
    omicron.public.vpc AS vpc
JOIN
    omicron.public.internet_gateway as igw
    ON igw.vpc_id = vpc.id
JOIN
    omicron.public.project as project
    ON vpc.project_id = project.id
JOIN
    omicron.public.ip_pool_resource as ipp
    ON project.silo_id = ipp.resource_id
WHERE
    ipp.is_default = true;


