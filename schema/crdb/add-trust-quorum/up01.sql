-- An LRTQ configuration explicitly placed in the database via a DB migration
-- 
-- LRTQ configurations are always epoch 1, and any subsequent trust quorum
-- configuration must have epoch > 1.
CREATE TABLE IF NOT EXISTS omicron.public.lrtq_member (
    -- Foreign key into the rack table
    rack_id UUID NOT NULL,

    -- Foreign key into the `hw_baseboard_id` table
    -- A sled can only be in one rack, hence the UNIQUE constraint.
    hw_baseboard_id UUID NOT NULL UNIQUE,

    PRIMARY KEY (rack_id, hw_baseboard_id)
);
