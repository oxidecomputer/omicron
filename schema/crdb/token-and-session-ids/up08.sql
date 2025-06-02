-- the docs say to do both of these in the same transaction
-- https://www.cockroachlabs.com/docs/v22.1/add-constraint#drop-and-add-a-primary-key-constraint

-- docs use the constraint name "primary" in the example, but that doesn't work
-- because it's actually called device_access_token_pkey, which I figured out with
-- 
--   show create table omicron.public.device_access_token

ALTER TABLE omicron.public.device_access_token
  DROP CONSTRAINT IF EXISTS "device_access_token_pkey";

ALTER TABLE omicron.public.device_access_token
  ADD CONSTRAINT "device_access_token_pkey" PRIMARY KEY (id);

