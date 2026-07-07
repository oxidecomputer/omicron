CREATE TABLE IF NOT EXISTS omicron.public.subnet_pool_silo_link (
    subnet_pool_id UUID NOT NULL,
    silo_id UUID NOT NULL,
    ip_version omicron.public.ip_version NOT NULL,
    is_default BOOL NOT NULL,
    PRIMARY KEY (subnet_pool_id, silo_id)
);
