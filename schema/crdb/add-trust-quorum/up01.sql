-- The state of a given trust quorum configuration
CREATE TYPE IF NOT EXISTS omicron.public.trust_quorum_configuration_state AS ENUM (
    -- Nexus is waiting for prepare acknowledgments by polling the coordinator
    -- In this case, a normal trust quorum reconfiguration is being prepared
    'preparing',
    -- Nexus is waiting for prepare acknowledgments by polling the coordinator
    -- In this case, an LRTQ upgrade is being prepared.
    'preparing-lrtq-upgrade',
    -- The configuration has committed to the dataabase, and nexus may still be
    -- trying to inform nodes about the commit.
    'committing',
    -- All nodes in the trust quorum have committed the configuration and nexus
    -- has no more work to do.
    'committed',
    -- Only some nodes have acknowledged commitment, but a new configuration
    -- was inserted.
    --
    -- We set this value so that we can tell that nexus is done trying to commit
    -- that old configuration.
    'committed-partially',
    -- The configuration has aborted and will not commit. The epoch can be
    -- skipped.
    'aborted'
);
