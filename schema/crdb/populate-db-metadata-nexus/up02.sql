-- Nexuses which may be attempting to access the database, and a state
-- which identifies if they should be allowed to do so.
--
-- This table is used during upgrade implement handoff between old and new
-- Nexus zones.
CREATE TABLE IF NOT EXISTS omicron.public.db_metadata_nexus (
  nexus_id UUID NOT NULL PRIMARY KEY,
  last_drained_blueprint_id UUID,
  state omicron.public.db_metadata_nexus_state NOT NULL
);

