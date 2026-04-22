-- Backfill the next_inv_min_time_started column for any existing sitreps. If the
-- inventory ID referenced by the sitrep still exists in the database, use the
-- corresponding `inv_collection` record's `time_done`. If that collection no
-- longer exists, (as inventory collections are pruned on a separate schedule
-- from sitreps), use `NOW()` as a fallback --- this is fairly conservative, but
-- will always be correct, since the collection must have finished to have been
-- used, and `NOW()` ought to be later than when it started....
SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.fm_sitrep
    SET next_inv_min_time_started = COALESCE(
        (
            SELECT inv_collection.time_done
            FROM omicron.public.inv_collection
            WHERE inv_collection.id = fm_sitrep.inv_collection_id
        ),
        NOW()
    )
    WHERE fm_sitrep.next_inv_min_time_started IS NULL;
