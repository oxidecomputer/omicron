/*
 * Previous commits added the optional kind of a producer. In this version,
 * we're making the value required and not nullable. We'll first delete all
 * records with a NULL kind -- there should not be any, since all producers both
 * in an out of tree have been updated. Nonetheless, this is safe because
 * currently we're updating offline, and all producers should re-register when
 * they are restarted.
 *
 * NOTE: Full table scans are disallowed, however we don't have an index on
 * producer kind (and don't currently need one). Allow full table scans for the
 * context of this one statement.
 */
SET LOCAL disallow_full_table_scans = off;
DELETE FROM omicron.public.metric_producer WHERE kind IS NULL;
