ALTER TABLE
 omicron.public.disk_type_local_storage
ADD COLUMN IF NOT EXISTS
 local_storage_unencrypted_dataset_allocation_id UUID DEFAULT NULL;
