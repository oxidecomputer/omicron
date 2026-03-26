ALTER TABLE omicron.public.blueprint
    ADD COLUMN IF NOT EXISTS source omicron.public.bp_source NOT NULL
    -- This will be wrong for any deployed systems that still have a blueprint
    -- produced by RSS (unlikely, given we archive old blueprints during
    -- updates, but possible) or that have blueprints produced from
    -- reconfigurator-cli edits (unlikely for customer systems, but maybe we've
    -- done that on dogfood or colo?). However: this value is only for
    -- debugging, and almost all blueprints on all systems will have been
    -- produced by the planner. It seems more prudent to be slightly wrong on
    -- some unlikely cases than to do a bunch of work to be more precise.
    DEFAULT 'planner';
