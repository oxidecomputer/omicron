CREATE TABLE IF NOT EXISTS omicron.public.tuf_generation (
    singleton BOOL NOT NULL PRIMARY KEY,
    generation INT8 NOT NULL,
    CHECK (singleton = true)
);
INSERT INTO omicron.public.tuf_generation (
    singleton,
    generation
) VALUES
    (TRUE, 1)
ON CONFLICT DO NOTHING;
