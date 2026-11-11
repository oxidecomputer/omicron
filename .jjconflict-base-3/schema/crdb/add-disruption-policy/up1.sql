CREATE TYPE IF NOT EXISTS
omicron.public.reconfigurator_disruption_policy AS ENUM (
    'terminate',
    'migrate_or_terminate',
    'migrate_only'
);
