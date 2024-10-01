ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS auto_restart_policy_temp omicron.public.instance_auto_restart_v2_temp;
