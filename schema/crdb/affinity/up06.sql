CREATE TABLE IF NOT EXISTS omicron.public.anti_affinity_group_instance_membership (
    group_id UUID,
    instance_id UUID,

    PRIMARY KEY (group_id, instance_id);
);

