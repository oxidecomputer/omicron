CREATE TABLE IF NOT EXISTS omicron.public.system_networking_settings (
    -- Singleton row for fleet-wide networking settings. See `db_metadata`
    -- for an explanation of the singleton pattern.
    singleton BOOL NOT NULL PRIMARY KEY,

    -- When true, end users may opt in to jumbo frames (8500 byte MTU) on
    -- the primary interface of an instance. When false, the per-instance
    -- bit is ignored and ports are created with the default MTU.
    external_jumbo_frames_opt_in_enabled BOOL NOT NULL,

    CHECK (singleton = true)
);
