CREATE INDEX IF NOT EXISTS lookup_producer_by_time_modified ON omicron.public.metric_producer (
    time_modified
);
