ALTER TABLE omicron.public.fm_alert_request
    ADD COLUMN IF NOT EXISTS comment TEXT NOT NULL DEFAULT '';
