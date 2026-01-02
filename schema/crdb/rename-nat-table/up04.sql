ALTER TABLE IF EXISTS
omicron.public.nat_entry
ALTER COLUMN version_added
SET DEFAULT nextval('omicron.public.nat_version');
