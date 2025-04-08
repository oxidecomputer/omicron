CREATE TABLE IF NOT EXISTS omicron.public.upstairs_repair_progress (
    repair_id UUID NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    current_item INT8 NOT NULL,
    total_items INT8 NOT NULL,

    PRIMARY KEY (repair_id, time, current_item, total_items)
);
