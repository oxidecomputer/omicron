-- Total group membership in trust quorum for a given epoch
CREATE TABLE IF NOT EXISTS omicron.public.trust_quorum_member (
    -- Foreign key into the rack table
    -- Foreign key into the `trust_quorum_configuration` table along with `epoch`
    rack_id UUID NOT NULL,

    -- Foreign key into the `trust_quorum_configuration` table along with `rack_id`
    epoch INT8 NOT NULL,

    -- Foreign key into the `hw_baseboard_id` table
    hw_baseboard_id UUID NOT NULL,

    -- Whether a node has acknowledged a prepare or commit yet
    state omicron.public.trust_quorum_member_state NOT NULL,

    -- The sha3-256 hash of the key share for this node. This is only filled in
    -- after Nexus has retrieved the configuration from the coordinator during
    -- the prepare phase of the protocol.
    --
    -- Hex formatted string
    share_digest STRING(64),

    -- For debugging only
    time_prepared TIMESTAMPTZ,
    time_committed TIMESTAMPTZ,

    CONSTRAINT time_committed_and_committed CHECK (
        (time_committed IS NULL AND state != 'committed')
        OR
        (time_committed IS NOT NULL AND state = 'committed')
    ),

    PRIMARY KEY (rack_id, epoch DESC, hw_baseboard_id)
);
