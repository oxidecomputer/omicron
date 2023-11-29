/*
 * The kind of metric producer each record corresponds to.
 */
CREATE TYPE IF NOT EXISTS omicron.public.producer_kind AS ENUM (
    -- A sled agent for an entry in the sled table.
    'sled_agent',
    -- A service in the omicron.public.service table
    'service',
    -- A Propolis VMM for an instance in the omicron.public.instance table
    'instance'
);
