-- 12400 was the hardcoded PROPOLIS_PORT prior to this addition; default to
-- that for all existing instances when creating the column.
-- in production, this value will always end up still being 12400.
-- however, nexus' testability in scenarios where each "propolis-server" API
-- isn't necessarily served from an illumos zone with its own IP address, but
-- rather in a sled-agent-sim thread during a cargo nextest run? useful to
-- allow some dynamic range there to avoid flakes caused by port collisions.
ALTER TABLE omicron.public.vmm
ADD COLUMN IF NOT EXISTS propolis_port INT4 NOT NULL
    CHECK (propolis_port BETWEEN 0 AND 65535)
    DEFAULT 12400;
