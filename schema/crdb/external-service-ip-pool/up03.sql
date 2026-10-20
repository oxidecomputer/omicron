CREATE INDEX IF NOT EXISTS external_service_ip_pool_by_ip_pool_id ON omicron.public.external_service_ip_pool (
    ip_pool_id
);
