CREATE INDEX IF NOT EXISTS lookup_blueprint_by_creation ON omicron.public.blueprint (
    time_created
);