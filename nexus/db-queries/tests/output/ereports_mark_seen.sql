UPDATE
  omicron.public.ereport
SET
  marked_seen_in = $1
WHERE
  (restart_id, ena) IN (SELECT unnest($2), unnest($3)) AND marked_seen_in IS NULL
