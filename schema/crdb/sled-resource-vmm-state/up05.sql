CREATE UNIQUE INDEX IF NOT EXISTS single_vmm_reservation_per_state ON omicron.public.sled_resource_vmm (
    instance_id,
    state
) WHERE state != 'tombstoned';
