CREATE INDEX IF NOT EXISTS lookup_alerts_by_time_dispatched
ON omicron.public.alert (
    time_dispatched
)
-- Since this is the query that's actually used operationally (by
-- `alert_select_next_for_dispatch`), make it a covering index to optimize the
-- background task's performance. This will also make *some* of thte
-- `alert_fetch_matching` queries that filter by time dispatched a bit nicer by
-- eliminating some of the index joins.
STORING (
    time_created,
    time_modified,
    alert_class,
    payload,
    num_dispatched,
    case_id,
    alert_version
);
