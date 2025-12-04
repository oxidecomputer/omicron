CREATE INDEX IF NOT EXISTS lookup_trust_quroum_acked_commits_by_rack_id_and_epoch
ON omicron.public.trust_quorum_acked_commit (rack_id, epoch);
