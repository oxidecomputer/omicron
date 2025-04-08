-- The actual state of the sled. This is updated exclusively by Nexus.
--
-- Nexus's goal is to match the sled's state with the operator-indicated
-- policy. For example, if the sled_policy is "expunged" and the sled_state is
-- "active", Nexus will assume that the sled is gone. Based on that, Nexus will
-- reallocate resources currently on the expunged sled to other sleds, etc.
-- Once the expunged sled no longer has any resources attached to it, Nexus
-- will mark it as decommissioned.
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
