ALTER TYPE
 omicron.public.dataset_kind
ADD VALUE IF NOT EXISTS
 'local_storage'
AFTER
 'update';
