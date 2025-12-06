-- The state of a given trust quorum configuration
CREATE TYPE IF NOT EXISTS omicron.public.trust_quorum_configuration_state AS ENUM (
    -- Nexus is waiting for prepare acknowledgments by polling the coordinator
    -- These may come as part of a reconfiguration or LRTQ upgrade
    'preparing',
    -- The configuration has committed to the dataabase, and nexus may still be
    -- trying to inform nodes about the commit.
    'committed',
    -- The configuration has aborted and will not commit. The epoch can be
    -- skipped.
    'aborted'
);

