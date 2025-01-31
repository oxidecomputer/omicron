CREATE INDEX IF NOT EXISTS lookup_anti_affinity_group_instance_membership_by_instance ON omicron.public.anti_affinity_group_instance_membership (
    instance_id
);

