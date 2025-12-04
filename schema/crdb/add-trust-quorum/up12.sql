CREATE TABLE IF NOT EXISTS omicron.public.trust_quorum_acked_commit (
    -- Foreign key into the rack table
    -- Foreign key into the `trust_quorum_configuration` table along with `epoch`
    rack_id UUID NOT NULL,

    -- Foreign key into the `trust_quorum_configuration` table along with `rack_id`
    epoch INT8 PRIMARY KEY,

    -- Foreign key into the `hw_baseboard_id` table
    hw_baseboard_id UUID NOT NULL
);
