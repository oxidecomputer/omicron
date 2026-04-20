ALTER TABLE omicron.public.fm_support_bundle_request
    ADD COLUMN IF NOT EXISTS comment TEXT NOT NULL DEFAULT '';
