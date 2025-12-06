CREATE UNIQUE INDEX IF NOT EXISTS lookup_trust_quorum_acked_commits_unique
ON omicron.public.trust_quorum_acked_commit (rack_id, epoch, hw_baseboard_id);

