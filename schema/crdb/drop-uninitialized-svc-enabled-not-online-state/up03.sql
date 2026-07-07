ALTER TABLE omicron.public.inv_svc_enabled_not_online_service
    ADD COLUMN IF NOT EXISTS state_temp omicron.public.inv_svc_enabled_not_online_state_temp;
