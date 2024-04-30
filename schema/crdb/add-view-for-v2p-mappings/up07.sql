CREATE INDEX IF NOT EXISTS vmm_by_instance_id
ON omicron.public.vmm (instance_id) STORING (sled_id);
