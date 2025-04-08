/*
 * Next, we make the field itself required in the database.
 */
ALTER TABLE IF EXISTS omicron.public.metric_producer ALTER COLUMN kind SET NOT NULL;
