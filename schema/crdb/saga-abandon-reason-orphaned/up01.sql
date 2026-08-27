ALTER TYPE
 omicron.public.saga_abandon_reason
ADD VALUE IF NOT EXISTS
 'orphaned'
AFTER
 'unrecoverable';
