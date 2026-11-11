CREATE TYPE IF NOT EXISTS omicron.public.inv_zpool_health AS ENUM (
    -- The device is online and functioning.
    'online',
    -- One or more components are degraded or faulted, but sufficient replicas
    -- exist to continue functioning.
    'degraded',
    -- One or more components are degraded or faulted, and insufficient replicas
    -- exist to continue functioning.
    'faulted',
    -- The device was explicitly taken offline by "zpool offline".
    'offline',
    -- The device was physically removed.
    'removed',
    -- The device could not be opened.
    'unavailable'
);
