# Add `id` to token and session tables

This migration adds UUID primary keys to the `console_session` and `device_access_token` tables, replacing the previous token-based primary keys.

## `console_session`

1. Add nullable `id` column (UUID)
2. Populate existing rows with generated UUIDs
3. Make `id` column non-nullable
4. Change primary key from `token` to `id`
5. Make `token` column non-nullable
6. Add unique index on `token`

## `device_access_token`

7. Add nullable `id` column (UUID)
8. Populate existing rows with generated UUIDs
9. Make `id` column non-nullable
10. Change primary key from `token` to `id`
11. Make `token` column non-nullable
12. Add unique index on `token`

