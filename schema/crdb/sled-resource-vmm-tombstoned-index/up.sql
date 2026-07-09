CREATE INDEX IF NOT EXISTS
    lookup_tombstoned_vmm_resources
ON omicron.public.sled_resource_vmm (
    state
) WHERE state = 'tombstoned';
