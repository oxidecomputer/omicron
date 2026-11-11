ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS instance_manager_update_disposition omicron.public.inv_sled_update_disposition DEFAULT 'available';
