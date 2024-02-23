-- The actual state of the sled. This is updated exclusively by Nexus.
--
-- Nexus's goal is to match the sled's state with the operator-indicated
-- policy. For example, if the sled_policy is "expunged" and the sled_state is
-- "active", Nexus will start removing zones from the sled, reallocating them
-- elsewhere, etc. Once that is done, Nexus will mark it as decommissioned.
CREATE TYPE IF NOT EXISTS omicron.public.sled_state AS ENUM (
    -- The sled has resources of any kind allocated on it, or, is available for
    -- new resources.
    --
    -- The sled can be in this state and have a different sled policy, e.g.
    -- "expunged".
    'active',

    -- The sled no longer has resources allocated on it, now or in the future.
    --
    -- This is a terminal state. This state is only valid if the sled policy is
    -- 'expunged'.
    'decommissioned'
);
