CREATE TYPE IF NOT EXISTS omicron.public.sled_expunge_power_off_outcome AS ENUM (
    'powered_off',
    'already_off',
    'other_board_present',
    'not_present',
    'absent_at_creation'
);
