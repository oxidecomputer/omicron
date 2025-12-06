-- Total group membership in trust quorum for a given epoch
CREATE TABLE IF NOT EXISTS omicron.public.trust_quorum_member (
    -- Foreign key into the rack table
    -- Foreign key into the `trust_quorum_configuration` table along with `epoch`
    rack_id UUID NOT NULL,

    -- Foreign key into the `trust_quorum_configuration` table along with `rack_id`
    epoch INT8 NOT NULL,

    -- Foreign key into the `hw_baseboard_id` table
    hw_baseboard_id UUID NOT NULL,

    -- The sha3-256 hash of the key share for this node. This is only filled in
    -- after Nexus has retrieved the configuration from the coordinator during
    -- the prepare phase of the protocol.
    share_digest STRING(32)
);

