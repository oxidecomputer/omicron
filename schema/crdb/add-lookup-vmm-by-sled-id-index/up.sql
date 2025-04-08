CREATE INDEX IF NOT EXISTS lookup_vmms_by_sled_id ON omicron.public.vmm (
    sled_id
) WHERE time_deleted IS NULL;
