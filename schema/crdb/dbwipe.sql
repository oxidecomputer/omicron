/*
 * dbwipe.sql: idempotently wipe everything created by "dbinit.sql"
 *
 * Obviously, this script is dangerous!  Use carefully.
 */

/*
 * NOTE: the database and user names MUST be kept in sync with the
 * initialization code and dbwipe.sql.
 */

/*
 * This is silly, but we throw an error if the user was already deleted.
 * Create the user so we can always delete it.
 */

BEGIN;

CREATE DATABASE IF NOT EXISTS omicron;
CREATE USER IF NOT EXISTS omicron;

ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM omicron;

DROP DATABASE IF EXISTS omicron;
DROP USER IF EXISTS omicron;

COMMIT;
