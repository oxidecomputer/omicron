CREATE TABLE IF NOT EXISTS omicron.public.external_service_ip_pool (
    service omicron.public.external_service_kind NOT NULL,
    ip_pool_id UUID NOT NULL,

    PRIMARY KEY (service, ip_pool_id)
);
