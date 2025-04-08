CREATE INDEX IF NOT EXISTS active_vmm
on omicron.public.vmm (time_deleted, sled_id, instance_id);
