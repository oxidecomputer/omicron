CREATE TYPE IF NOT EXISTS omicron.public.failure_domain AS ENUM (
    -- Instances are co-located if they are on the same sled.
    'sled'
);

