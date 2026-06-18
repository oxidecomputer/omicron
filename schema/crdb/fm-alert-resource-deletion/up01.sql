CREATE TABLE IF NOT EXISTS omicron.public.rendezvous_alert_created (
    alert_id UUID PRIMARY KEY,
    created_at_generation INT8 NOT NULL
);
