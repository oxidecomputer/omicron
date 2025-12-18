CREATE TABLE IF NOT EXISTS omicron.public.bp_single_measurements (
    blueprint_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    id UUID NOT NULL,

    image_artifact_sha256 STRING(64),
    prune BOOLEAN,
    PRIMARY KEY (blueprint_id, id)
);

