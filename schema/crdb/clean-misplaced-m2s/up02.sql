-- Remove m2s from the physical disk table
DELETE FROM omicron.public.physical_disk WHERE variant = 'm2';
