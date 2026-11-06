ALTER TABLE omicron.public.fm_sitrep
    ADD COLUMN IF NOT EXISTS problem_generation INT8 NOT NULL DEFAULT 1;
