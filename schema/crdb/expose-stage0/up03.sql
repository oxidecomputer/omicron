-- add stage0/stage0next to the caboose targets
ALTER TYPE omicron.public.caboose_which ADD VALUE IF NOT EXISTS 'stage0';
