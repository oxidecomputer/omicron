-- Rename device_access_token_new to device_access_token
ALTER TABLE IF EXISTS omicron.public.device_access_token_new RENAME TO device_access_token;
