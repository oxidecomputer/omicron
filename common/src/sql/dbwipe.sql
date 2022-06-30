/*
 * dbwipe.sql: idempotently wipe everything created by "dbinit.sql"
 *
 * Obviously, this script is dangerous!  Use carefully.
 */

/*
 * NOTE: the database and user names MUST be kept in sync with the
 * initialization code and dbwipe.sql.
 */
DROP DATABASE IF EXISTS omicron;
ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM omicron;
DROP USER IF EXISTS omicron;
