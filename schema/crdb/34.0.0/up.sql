CREATE UNIQUE INDEX IF NOT EXISTS network_interface_parent_id_slot_key ON omicron.public.network_interface (
    parent_id,
    slot
)
WHERE
    time_deleted IS NULL;
