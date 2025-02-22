CREATE TYPE IF NOT EXISTS omicron.public.affinity_policy AS ENUM (
    -- If the affinity request cannot be satisfied, fail.
    'fail',

    -- If the affinity request cannot be satisfied, allow it anyway.
    'allow'
);

