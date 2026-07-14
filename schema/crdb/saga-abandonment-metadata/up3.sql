/*
 * Backfill abandonment metadata for any sagas that were abandoned before these
 * columns existed.  The only way to abandon a saga prior to this migration was
 * via omdb, so the reason is 'omdb'.  The true time of abandonment was never
 * recorded, so we use `adopt_time` (the time the saga was last adopted, which
 * is the most recent timestamp we have and therefore the tightest lower bound
 * on when it could have been abandoned) as a deterministic placeholder, and
 * note this in `abandon_comment`.
 */
UPDATE omicron.public.saga
SET
    abandon_reason = 'omdb',
    abandon_time = adopt_time,
    abandon_comment = 'Backfilled by schema migration: abandoned via omdb before abandonment metadata was recorded. The true time of abandonment is unknown; abandon_time is set to adopt_time as a placeholder.'
WHERE
    saga_state = 'abandoned'
    AND (
        abandon_reason IS NULL
        OR abandon_time IS NULL
        OR abandon_comment IS NULL
    );
