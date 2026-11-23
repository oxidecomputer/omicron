ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS instance_manager_num_registered_vmms INT8 NOT NULL DEFAULT 0 CHECK (instance_manager_num_registered_vmms >= 0);
