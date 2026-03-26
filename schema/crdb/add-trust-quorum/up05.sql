-- A partial index to retrieve all "active" trust quorum configurations.
--
-- These are configurations that are either still preparing or committing and
-- therefore require work from Nexus.
CREATE UNIQUE INDEX IF NOT EXISTS trust_quorum_active_configurations
    on omicron.public.trust_quorum_configuration(rack_id, epoch DESC)
    WHERE state = 'preparing'
        OR state = 'preparing-lrtq-upgrade'
        OR state = 'committing';
