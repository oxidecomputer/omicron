CREATE TABLE IF NOT EXISTS background_task_toggles (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    name STRING NOT NULL,
    enabled BOOLEAN NOT NULL,
    time_created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    time_modified TIMESTAMPTZ ON UPDATE NOW() NOT NULL DEFAULT NOW(),
    time_deleted TIMESTAMPTZ
)
