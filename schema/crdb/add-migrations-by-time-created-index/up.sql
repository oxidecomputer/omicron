
/* Migrations by time created.
 *
 * Currently, this is only used by OMDB for ordering the `omdb migration list`
 * output, but it may be used by other UIs in the future...
*/
CREATE INDEX IF NOT EXISTS migrations_by_time_created ON omicron.public.migration (
    time_created
);
