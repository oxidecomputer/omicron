-- Whether a node has prepared or committed yet
CREATE TYPE IF NOT EXISTS omicron.public.trust_quorum_member_state AS ENUM (
    -- The node has not acknowledged either a `Prepare` or `Commit` message
    'unacked',
    -- The node has acknoweledged a `Prepare` message
    'prepared',
    -- The node has acknowledged a `Commit` or `PrepareAndCommit` message
    -- `committed` implies `prepared`
    'committed'
);
