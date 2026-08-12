/*
 * Backfill the names / descriptions for SNAT IPs, which as of this migration,
 * are only used for boundary NTP zones.
 *
 * The name is the `ZoneType::name_prefix()` plus the UUID, separated by a dash.
 * For NTP zones (boundary or internal), this is 'ntp`.
 *
 * The description is `ZoneKind::report_str()`. For Boundary NTP zones, this is
 * 'boundary_ntp'.
 */
SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.external_ip SET
name = 'ntp-' || CAST(parent_id AS TEXT),
description = 'boundary_ntp'
WHERE kind = 'snat' AND is_service = TRUE;
