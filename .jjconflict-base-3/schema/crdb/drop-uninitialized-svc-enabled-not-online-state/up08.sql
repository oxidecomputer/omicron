ALTER TABLE omicron.public.inv_svc_enabled_not_online_service
    ADD COLUMN IF NOT EXISTS state omicron.public.inv_svc_enabled_not_online_state;
