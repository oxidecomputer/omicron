/*
 * dbwipe.sql: idempotently wipe everything created by "dbinit.sql"
 *
 * Obviously, this script is dangerous!  Use carefully.
 */

DROP DATABASE IF EXISTS oxcp;
DROP USER IF EXISTS oxcp;
