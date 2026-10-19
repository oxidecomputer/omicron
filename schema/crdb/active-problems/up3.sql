CREATE INDEX IF NOT EXISTS
    lookup_active_problem_by_time_created
ON
    omicron.public.active_problem (time_created)
WHERE
    time_deleted IS NULL;
