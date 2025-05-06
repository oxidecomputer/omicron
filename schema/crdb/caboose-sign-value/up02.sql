ALTER TABLE omicron.public.sw_caboose
    ADD COLUMN IF NOT EXISTS sign_idx TEXT NOT NULL AS (IFNULL(sign, 'n/a')) VIRTUAL;
