ALTER TABLE omicron.public.ip_pool
ADD COLUMN is_delegated BOOL NOT NULL
DEFAULT FALSE;
