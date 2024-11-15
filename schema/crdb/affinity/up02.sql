CREATE TYPE IF NOT EXISTS omicron.public.affinity_distance AS ENUM (
    -- Instances are co-located if they are on the same sled.
    'sled'
);

