CREATE UNIQUE INDEX IF NOT EXISTS caboose_properties
    on omicron.public.sw_caboose (board, git_commit, name, version, sign);