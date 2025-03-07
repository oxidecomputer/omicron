CREATE TABLE IF NOT EXISTS omicron.public.anti_affinity_group_affinity_membership (
    anti_affinity_group_id UUID NOT NULL,
    affinity_group_id UUID NOT NULL,

    PRIMARY KEY (anti_affinity_group_id, affinity_group_id)
);
