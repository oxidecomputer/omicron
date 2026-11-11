/*
 * To support missing measurements, we are making all scalar datum columns
 * Nullable, so that a NULL value (None in Rust) represents a missing datum at
 * the provided timestamp.
 *
 * Note that arrays cannot be made Nullable, so we need to use an empty array as
 * the sentinel value implying a missing measurement.
 */
ALTER TABLE oximeter.measurements_bool MODIFY COLUMN datum Nullable(UInt8)
