CREATE TABLE IF NOT EXISTS omicron.public.ip_pool_resource (
    ip_pool_id UUID NOT NULL,
    resource_type omicron.public.ip_pool_resource_type NOT NULL,
    resource_id UUID NOT NULL,
    is_default BOOL NOT NULL,

    PRIMARY KEY (ip_pool_id, resource_type, resource_id)
);
