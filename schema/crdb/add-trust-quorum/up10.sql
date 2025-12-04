CREATE INDEX IF NOT EXISTS lookup_trust_quroum_acked_prepares_by_rack_id_and_epoch
ON omicron.public.trust_quorum_acked_prepare (rack_id, epoch);
