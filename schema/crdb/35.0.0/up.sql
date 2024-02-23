-- This isn't realy a schema migration, but is instead a one-off fix for
-- incorrect data (https://github.com/oxidecomputer/omicron/issues/5056) after
-- the root cause for the incorrect data has been addressed.
UPDATE omicron.public.network_interface
    SET slot = 0
    WHERE kind = 'service' AND time_deleted IS NULL;
