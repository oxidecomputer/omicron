CREATE UNIQUE INDEX IF NOT EXISTS lookup_vmm_by_zpool ON omicron.public.vmm (
    zpool_id,
    id
) WHERE time_deleted IS NULL;
