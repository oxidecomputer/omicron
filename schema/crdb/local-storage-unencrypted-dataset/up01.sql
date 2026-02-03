ALTER TYPE
 omicron.public.dataset_kind
ADD VALUE IF NOT EXISTS
 'local_storage_unencrypted'
AFTER
 'local_storage';
