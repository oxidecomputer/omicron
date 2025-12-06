CREATE UNIQUE INDEX IF NOT EXISTS lookup_trust_quorum_members_unique
ON omicron.public.trust_quorum_member (rack_id, epoch, hw_baseboard_id);
