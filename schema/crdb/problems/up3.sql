CREATE INDEX IF NOT EXISTS
    lookup_problem_by_time_created
ON
    omicron.public.problem (time_created)
WHERE
    time_deleted IS NULL;
