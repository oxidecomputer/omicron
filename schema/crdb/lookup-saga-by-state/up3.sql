CREATE TABLE IF NOT EXISTS omicron.public.inv_stale_saga (
    inv_collection_id UUID NOT NULL,
    saga_id UUID NOT NULL,
    creator UUID NOT NULL,
    current_sec UUID,
    name TEXT NOT NULL,
    state omicron.public.stale_saga_state NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_collected TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (inv_collection_id, saga_id)
);
