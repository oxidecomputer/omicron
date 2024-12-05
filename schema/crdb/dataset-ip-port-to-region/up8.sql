ALTER TABLE omicron.public.region ADD CONSTRAINT check_port CHECK (port BETWEEN 0 and 65535);
