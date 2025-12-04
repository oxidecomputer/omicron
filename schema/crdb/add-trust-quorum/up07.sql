CREATE INDEX IF NOT EXISTS lookup_trust_quroum_members_by_rack_id_and_epoch
ON omicron.public.trust_quorum_member (rack_id, epoch);
