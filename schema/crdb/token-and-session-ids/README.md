# Add `id` to token and session tables

This migration adds UUID primary keys to the `console_session` and `device_access_token` tables, replacing the previous token-based primary keys.

## `console_session`

1. Add non-nullable `id` column (UUID) with default
2. Drop default from `id` column
3. Change primary key from `token` to `id`
4. Make `token` column non-nullable
5. Add unique index on `token`

## `device_access_token`

6. Add non-nullable `id` column (UUID) with default
7. Drop default from `id` column
8. Change primary key from `token` to `id`
9. Make `token` column non-nullable
10. Add unique index on `token`

