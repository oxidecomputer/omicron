SET LOCAL disallow_full_table_scans = off;
-- As of this schema version, we no longer consider `uninitialized` services to
-- be broken and the schema no longer supports expressing that a broken service
-- can have state `uninitialized`. It's safe to just delete any such services..
DELETE FROM omicron.public.inv_svc_enabled_not_online_service
    WHERE state::text = 'uninitialized';
