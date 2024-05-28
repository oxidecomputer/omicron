CREATE TABLE IF NOT EXISTS omicron.public.region_replacement_step (
    replacement_id UUID NOT NULL,

    step_time TIMESTAMPTZ NOT NULL,

    step_type omicron.public.region_replacement_step_type NOT NULL,

    step_associated_instance_id UUID,
    step_associated_vmm_id UUID,

    step_associated_pantry_ip INET,
    step_associated_pantry_port INT4 CHECK (step_associated_pantry_port BETWEEN 0 AND 65535),
    step_associated_pantry_job_id UUID,

    PRIMARY KEY (replacement_id, step_time, step_type)
);
