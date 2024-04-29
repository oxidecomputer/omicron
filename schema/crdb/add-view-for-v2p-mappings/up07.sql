CREATE INDEX IF NOT EXISTS vmm_by_instance_id
ON vmm (instance_id) STORING (sled_id);
