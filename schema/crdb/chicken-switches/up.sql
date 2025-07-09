CREATE TABLE IF NOT EXISTS omicron.public.reconfigurator_chicken_switches (
    version INT8 PRIMARY KEY,
    planner_enabled BOOL NOT NULL DEFAULT FALSE,
    time_modified TIMESTAMPTZ NOT NULL
);
