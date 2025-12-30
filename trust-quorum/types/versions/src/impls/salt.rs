use rand::{TryRngCore as _, rngs::OsRng};

use crate::v1::crypto::Salt;

impl Salt {
    /// Generate a new random salt.
    ///
    /// This is a free function because `Salt` is defined in `trust-quorum-types` (for API versioning
    /// akin to RFD 619).
    pub fn new() -> Salt {
        let mut rng = OsRng;
        let mut salt = [0u8; 32];
        rng.try_fill_bytes(&mut salt).expect("fetched random bytes from OsRng");
        Salt(salt)
    }
}
