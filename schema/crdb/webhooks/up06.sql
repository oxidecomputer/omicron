CREATE TYPE IF NOT EXISTS omicron.public.webhook_event_class AS ENUM (
    -- Liveness probes, which are technically not real events, but, you know...
    'probe',
    -- Test classes used to test globbing.
    --
    -- These are not publicly exposed.
    'test.foo',
    'test.foo.bar',
    'test.foo.baz',
    'test.quux.bar',
    'test.quux.bar.baz'
    -- Add new event classes here!
);
