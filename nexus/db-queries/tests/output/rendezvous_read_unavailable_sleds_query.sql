SELECT
  rendezvous_sled_bp_availability.sled_id
FROM
  rendezvous_sled_bp_availability
WHERE
  rendezvous_sled_bp_availability.bp_availability = $1
