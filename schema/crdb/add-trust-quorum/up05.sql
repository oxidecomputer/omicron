-- Information for tracking trust quorum memberships over time
CREATE TABLE IF NOT EXISTS omicron.public.trust_quorum_configuration (
    -- Foreign key into the rack table
    rack_id UUID NOT NULL,

    -- Monotonically increasing version per rack_id
    epoch INT8 NOT NULL,

    -- The number of shares needed to compute the rack secret
    --
    -- In some documentation we call this the `K` parameter.
    threshold INT2 NOT NULL,

    -- The number of additional nodes beyond threshold to commit 
    --
    -- This represents the number of prepared nodes that can be offline after
    -- a commit at Nexus and still allow the secret to be reconstructed during
    -- rack unlock. If this number is equivalent to the total membership (`N`)
    -- minus `threshold` nodes, then all nodes in the membership set for this
    -- epoch must ack a prepare for a commit to occur. By varying this value we
    -- allow commit to occur even if some nodes haven't prepared, thus providing
    -- fault tolerance during the prepare phase and also during unlock.
    --
    -- In some documentation we call this the `Z` parameter.
    commit_crash_tolerance INT2 NOT NULL,

    -- Which member is coordinating the prepare phase of the protocol this epoch
    -- Foreign key into the `hw_baseboard_id` table
    coordinator UUID NOT NULL,

    -- Encrypted rack secrets for prior committed epochs
    --
    -- These are only filled in during a reconfiguration and retrieved
    -- during the prepare phase of the protocol by Nexus from the coordinator.
    encrypted_rack_secrets TEXT,

    -- Each rack has its own trust quorum
    PRIMARY KEY (rack_id, epoch)
);
