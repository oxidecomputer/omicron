-- Many control plane operations wish to select all the instances in particular
-- states.
CREATE INDEX IF NOT EXISTS lookup_instance_by_state
ON
    omicron.public.instance (state)
WHERE
    time_deleted IS NULL;
