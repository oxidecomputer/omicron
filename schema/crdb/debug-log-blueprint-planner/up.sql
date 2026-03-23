CREATE TABLE IF NOT EXISTS omicron.public.debug_log_blueprint_planning (
    blueprint_id UUID NOT NULL PRIMARY KEY,
    debug_blob JSONB NOT NULL
);
