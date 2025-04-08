-- Create the `rot_image_error` type
CREATE TYPE IF NOT EXISTS omicron.public.rot_image_error AS ENUM (
        'unchecked',
        'first_page_erased',
        'partially_programmed',
        'invalid_length',
        'header_not_programmed',
        'bootloader_too_small',
        'bad_magic',
        'header_image_size',
        'unaligned_length',
        'unsupported_type',
        'not_thumb2',
        'reset_vector',
        'signature'
);

