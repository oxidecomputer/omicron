SET LOCAL disallow_full_table_scans = off;

-- Use a placeholder value to backfill any existing `failed_unreachable records
-- which predate our tracking of error messages. We cannot determine the actual
-- reason the request could not be delivered at this point, as the error details
-- were never recorded. Oh well.
UPDATE
    omicron.public.webhook_delivery_attempt
SET
    unreachable_reason = '<before recorded history>'
WHERE
    result = 'failed_unreachable'
AND
    unreachable_reason IS NULL;
