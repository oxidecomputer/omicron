-- The disposition for a particular sled. This is updated solely by the
-- operator, and not by Nexus.
CREATE TYPE IF NOT EXISTS omicron.public.sled_policy AS ENUM (
    -- The sled is in service, and new resources can be provisioned onto it.
    'in_service',
    -- The sled is in service, but the operator has indicated that new
    -- resources should not be provisioned onto it.
    'no_provision',
    -- The operator has marked that the sled has, or will be, removed from the
    -- rack, and it should be assumed that any resources currently on it are
    -- now permanently missing.
    'expunged'
);
