SET LOCAL disallow_full_table_scans = off;

INSERT INTO omicron.public.support_bundle_data_selection_flags (bundle_id, include_reconfigurator, include_sled_cubby_info, include_sp_dumps)
SELECT id, true, true, true FROM omicron.public.support_bundle
ON CONFLICT DO NOTHING;

INSERT INTO omicron.public.support_bundle_data_selection_host_info (bundle_id, all_sleds, sled_ids)
SELECT id, true, ARRAY[]::UUID[]
FROM omicron.public.support_bundle
ON CONFLICT DO NOTHING;

INSERT INTO omicron.public.support_bundle_data_selection_ereports (bundle_id, start_time, end_time, only_serials, only_classes)
SELECT id, NULL, NULL, ARRAY[]::TEXT[], ARRAY[]::TEXT[]
FROM omicron.public.support_bundle
ON CONFLICT DO NOTHING;
