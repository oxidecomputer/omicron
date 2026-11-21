ALTER TYPE omicron.public.fm_fact_physical_disk_kind
ADD VALUE IF NOT EXISTS
    'nvme_health_warning'
AFTER
    'zpool_unhealthy';
