SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.silo_quotas
SET
    cpus = GREATEST(cpus, 0),
    memory_bytes = GREATEST(memory_bytes, 0),
    storage_bytes = GREATEST(storage_bytes, 0)
WHERE cpus < 0 OR memory_bytes < 0 OR storage_bytes < 0;
