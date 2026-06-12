/*
 * Backfill abandonment metadata for any sagas that were abandoned before these
 * columns existed.  The only way to abandon a saga prior to this migration was
 * via omdb, so the reason is 'omdb'.  The true time of abandonment was never
 * recorded, so we use `adopt_time` (the time the saga was last adopted, which
 * is the most recent timestamp we have and therefore the tightest lower bound
 * on when it could have been abandoned) as a deterministic placeholder, and
 * note this in `abandon_information`.
 */
UPDATE omicron.public.saga
SET
    reason_abandoned = 'omdb',
    time_abandoned = adopt_time,
    abandon_information = 'Backfilled by schema migration: abandoned via omdb before abandonment metadata was recorded. The true time of abandonment is unknown; time_abandoned is set to adopt_time as a placeholder.'
WHERE
    saga_state = 'abandoned'
    AND (
        reason_abandoned IS NULL
        OR time_abandoned IS NULL
        OR abandon_information IS NULL
    );
