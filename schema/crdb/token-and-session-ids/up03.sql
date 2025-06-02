-- the docs say to do both of these in the same transaction
-- https://www.cockroachlabs.com/docs/v22.1/add-constraint#drop-and-add-a-primary-key-constraint

-- docs use the constraint name "primary" in the example, but that doesn't work
-- because it's actually called console_session_pkey, which I figured out with
-- 
--   show create table omicron.public.console_session

ALTER TABLE omicron.public.console_session
  DROP CONSTRAINT IF EXISTS "console_session_pkey";

ALTER TABLE omicron.public.console_session
  ADD CONSTRAINT "console_session_pkey" PRIMARY KEY (id);

