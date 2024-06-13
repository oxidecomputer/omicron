-- Add new fields for stage0. These can all correctly be NULL
ALTER TABLE omicron.inv_root_of_trust
	ADD COLUMN IF NOT EXISTS stage0_fwid TEXT,
	ADD COLUMN IF NOT EXISTS stage0next_fwid TEXT,
	ADD COLUMN IF NOT EXISTS slot_a_error omicron.public.rot_image_error,
	ADD COLUMN IF NOT EXISTS slot_b_error omicron.public.rot_image_error,
	ADD COLUMN IF NOT EXISTS stage0_error omicron.public.rot_image_error,
	ADD COLUMN IF NOT EXISTS stage0next_error omicron.public.rot_image_error;
