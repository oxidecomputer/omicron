SET LOCAL disallow_full_table_scans = 'off';

-- 'kind' tag, used by all artifacts
INSERT INTO omicron.public.tuf_artifact_tag (tuf_artifact_id, key, value)
    SELECT id, 'kind', (CASE kind
        WHEN 'installinator_document' THEN 'installinator_document'
        WHEN 'measurement_corpus' THEN 'measurement_corpus'
        WHEN 'cosmo_host_phase_1' THEN 'os_phase1'
        WHEN 'cosmo_trampoline_phase_1' THEN 'os_phase1'
        WHEN 'gimlet_host_phase_1' THEN 'os_phase1'
        WHEN 'gimlet_trampoline_phase_1' THEN 'os_phase1'
        WHEN 'host_phase_2' THEN 'os_phase2'
        WHEN 'trampoline_phase_2' THEN 'os_phase2'
        WHEN 'gimlet_rot_image_a' THEN 'rot'
        WHEN 'gimlet_rot_image_b' THEN 'rot'
        WHEN 'psc_rot_image_a' THEN 'rot'
        WHEN 'psc_rot_image_b' THEN 'rot'
        WHEN 'switch_rot_image_a' THEN 'rot'
        WHEN 'switch_rot_image_b' THEN 'rot'
        WHEN 'gimlet_rot_bootloader' THEN 'rot_bootloader'
        WHEN 'psc_rot_bootloader' THEN 'rot_bootloader'
        WHEN 'switch_rot_bootloader' THEN 'rot_bootloader'
        WHEN 'gimlet_sp' THEN 'sp'
        WHEN 'psc_sp' THEN 'sp'
        WHEN 'switch_sp' THEN 'sp'
        WHEN 'zone' THEN 'zone'
        ELSE NULL
    END)
        FROM omicron.public.tuf_artifact
        WHERE kind IN (
            'installinator_document',
            'measurement_corpus',
            'cosmo_host_phase_1',
            'cosmo_trampoline_phase_1',
            'gimlet_host_phase_1',
            'gimlet_trampoline_phase_1',
            'host_phase_2',
            'trampoline_phase_2',
            'gimlet_rot_image_a',
            'gimlet_rot_image_b',
            'psc_rot_image_a',
            'psc_rot_image_b',
            'switch_rot_image_a',
            'switch_rot_image_b',
            'gimlet_rot_bootloader',
            'psc_rot_bootloader',
            'switch_rot_bootloader',
            'gimlet_sp',
            'psc_sp',
            'switch_sp',
            'zone'
        )
ON CONFLICT DO NOTHING;

-- 'os_board' tag, used by os_phase1 artifacts
INSERT INTO omicron.public.tuf_artifact_tag (tuf_artifact_id, key, value)
    SELECT id, 'os_board', (CASE kind
        WHEN 'cosmo_host_phase_1' THEN 'cosmo'
        WHEN 'cosmo_trampoline_phase_1' THEN 'cosmo'
        WHEN 'gimlet_host_phase_1' THEN 'gimlet'
        WHEN 'gimlet_trampoline_phase_1' THEN 'gimlet'
    END)
        FROM omicron.public.tuf_artifact
        WHERE kind IN (
            'cosmo_host_phase_1',
            'cosmo_trampoline_phase_1',
            'gimlet_host_phase_1',
            'gimlet_trampoline_phase_1'
        )
ON CONFLICT DO NOTHING;

-- 'os_variant' tag, used by os_phase1 and os_phase2 artifacts
INSERT INTO omicron.public.tuf_artifact_tag (tuf_artifact_id, key, value)
    SELECT id, 'os_variant', (CASE kind
        WHEN 'cosmo_host_phase_1' THEN 'host'
        WHEN 'cosmo_trampoline_phase_1' THEN 'recovery'
        WHEN 'gimlet_host_phase_1' THEN 'host'
        WHEN 'gimlet_trampoline_phase_1' THEN 'recovery'
        WHEN 'host_phase_2' THEN 'host'
        WHEN 'trampoline_phase_2' THEN 'recovery'
    END)
        FROM omicron.public.tuf_artifact
        WHERE kind IN (
            'cosmo_host_phase_1',
            'cosmo_trampoline_phase_1',
            'gimlet_host_phase_1',
            'gimlet_trampoline_phase_1',
            'host_phase_2',
            'trampoline_phase_2'
        )
ON CONFLICT DO NOTHING;

-- 'rot_board' tag, used by rot and rot_bootloader artifacts
INSERT INTO omicron.public.tuf_artifact_tag (tuf_artifact_id, key, value)
    SELECT id, 'rot_board', board
        FROM omicron.public.tuf_artifact
        WHERE board IS NOT NULL AND kind IN (
            'gimlet_rot_image_a',
            'gimlet_rot_image_b',
            'psc_rot_image_a',
            'psc_rot_image_b',
            'switch_rot_image_a',
            'switch_rot_image_b',
            'gimlet_rot_bootloader',
            'psc_rot_bootloader',
            'switch_rot_bootloader'
        )
ON CONFLICT DO NOTHING;

-- 'rot_sign' tag, used by rot and rot_bootloader artifacts
INSERT INTO omicron.public.tuf_artifact_tag (tuf_artifact_id, key, value)
    SELECT id, 'rot_sign', sign
        FROM omicron.public.tuf_artifact
        WHERE sign IS NOT NULL AND kind IN (
            'gimlet_rot_image_a',
            'gimlet_rot_image_b',
            'psc_rot_image_a',
            'psc_rot_image_b',
            'switch_rot_image_a',
            'switch_rot_image_b',
            'gimlet_rot_bootloader',
            'psc_rot_bootloader',
            'switch_rot_bootloader'
        )
ON CONFLICT DO NOTHING;

-- 'rot_slot' tag, used by rot artifacts
INSERT INTO omicron.public.tuf_artifact_tag (tuf_artifact_id, key, value)
    SELECT id, 'rot_slot', (CASE kind
        WHEN 'gimlet_rot_image_a' THEN 'a'
        WHEN 'gimlet_rot_image_b' THEN 'b'
        WHEN 'psc_rot_image_a' THEN 'a'
        WHEN 'psc_rot_image_b' THEN 'b'
        WHEN 'switch_rot_image_a' THEN 'a'
        WHEN 'switch_rot_image_b' THEN 'b'
    END)
        FROM omicron.public.tuf_artifact
        WHERE kind IN (
            'gimlet_rot_image_a',
            'gimlet_rot_image_b',
            'psc_rot_image_a',
            'psc_rot_image_b',
            'switch_rot_image_a',
            'switch_rot_image_b'
        )
ON CONFLICT DO NOTHING;

-- 'sp_board' tag, used by sp artifacts
INSERT INTO omicron.public.tuf_artifact_tag (tuf_artifact_id, key, value)
    SELECT id, 'sp_board', board
        FROM omicron.public.tuf_artifact
        WHERE board IS NOT NULL AND kind IN (
            'gimlet_sp',
            'psc_sp',
            'switch_sp'
        )
ON CONFLICT DO NOTHING;

-- 'zone_name' tag, used by zone artifacts
INSERT INTO omicron.public.tuf_artifact_tag (tuf_artifact_id, key, value)
    SELECT id, 'zone_name', name
        FROM omicron.public.tuf_artifact
        WHERE kind = 'zone'
ON CONFLICT DO NOTHING;
