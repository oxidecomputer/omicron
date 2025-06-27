ALTER TABLE omicron.public.device_access_token
  ADD COLUMN IF NOT EXISTS id UUID NOT NULL DEFAULT gen_random_uuid();
