SET LOCAL disallow_full_table_scans = 'off';
INSERT INTO omicron.public.tuf_artifact_file
    SELECT DISTINCT ON (sha256) sha256, version, artifact_size
        FROM omicron.public.tuf_artifact
        ORDER BY sha256 ASC, version ASC, artifact_size ASC
ON CONFLICT DO NOTHING;
