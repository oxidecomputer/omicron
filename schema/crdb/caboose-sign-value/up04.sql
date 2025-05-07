CREATE UNIQUE INDEX IF NOT EXISTS caboose_properties_no_sign
    on omicron.public.sw_caboose (board, git_commit, name, version)
    WHERE sign IS NULL;