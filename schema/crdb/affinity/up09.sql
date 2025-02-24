CREATE TABLE IF NOT EXISTS omicron.public.anti_affinity_group_instance_membership (
    group_id UUID NOT NULL,
    instance_id UUID NOT NULL,

    PRIMARY KEY (group_id, instance_id)
);

