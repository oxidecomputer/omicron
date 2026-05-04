CREATE UNIQUE INDEX IF NOT EXISTS single_vmm_reservation_per_generation ON omicron.public.sled_resource_vmm (
    instance_id,
    instance_state_generation
);
