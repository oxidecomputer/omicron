CREATE UNIQUE INDEX IF NOT EXISTS lookup_trust_quorum_acked_prepares_unique
ON omicron.public.trust_quorum_acked_prepare (rack_id, epoch, hw_baseboard_id);

