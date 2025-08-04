CREATE TABLE IF NOT EXISTS omicron.public.tuf_repo_rot_by_sign (
    tuf_repo_id UUID NOT NULL,
    tuf_rot_by_sign_id UUID NOT NULL,

    PRIMARY KEY (tuf_repo_id, tuf_rot_by_sign_id)
);