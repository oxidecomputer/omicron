-- start_time is created nullable here even though the final schema requires
-- it: a later step in this migration (up3.sql) inserts rows promoted from
-- end-only ereport filters, whose starts are backfilled by up8.sql before
-- up9.sql makes the column NOT NULL.
--
-- The CHECK accepts a NULL start during that window: NULL <= end_time
-- evaluates to NULL, which satisfies a CHECK constraint.
CREATE TABLE IF NOT EXISTS omicron.public.support_bundle_data_selection_time_range (
    bundle_id UUID NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,

    PRIMARY KEY (bundle_id),
    CONSTRAINT start_before_end CHECK (
        end_time IS NULL OR start_time <= end_time
    )
);
