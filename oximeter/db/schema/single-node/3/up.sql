/* This adds missing field types to the timeseries schema table field.type
 * column, by augmentin the enum to capture new values. Note that the existing
 * variants can't be moved or changed, so the new ones are added at the end. The
 * client never sees this discriminant, only the string, so it should not
 * matter.
 */
ALTER TABLE oximeter.timeseries_schema
    MODIFY COLUMN IF EXISTS fields.type
    Array(Enum(
        'Bool' = 1,
        'I64' = 2,
        'IpAddr' = 3,
        'String' = 4,
        'Uuid' = 6,
        'I8' = 7,
        'U8' = 8,
        'I16' = 9,
        'U16' = 10,
        'I32' = 11,
        'U32' = 12,
        'U64' = 13
    ));
