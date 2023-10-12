/*
 * The sled_instance view cannot be modified in place because it depends on the
 * VMM table. It would be nice to define the VMM table and then alter the
 * sled_instance table, but there's no way to express this correctly in the
 * clean-slate DB initialization SQL (dbinit.sql) because it requires inserting
 * a table into the middle of an existing sequence of table definitions. (See
 * the README for more on why this causes problems.) Instead, delete the
 * `sled_instance` view, then add the VMM table, then add the view back and
 * leave it to `dbinit.sql` to re-create the resulting object ordering when
 * creating a database from a clean slate.
 */

DROP VIEW IF EXISTS omicron.public.sled_instance;
