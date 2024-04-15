CREATE UNIQUE INDEX IF NOT EXISTS vendor_serial_model_unique on omicron.public.physical_disk (
  vendor, serial, model
) WHERE time_deleted IS NULL AND disk_state != 'decommissioned';
