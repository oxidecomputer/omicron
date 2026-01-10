use rand::{TryRngCore as _, rngs::OsRng};

use crate::v1::crypto::Salt;

impl Salt {
    /// Generate a new random salt.
    pub fn new() -> Salt {
        let mut rng = OsRng;
        let mut salt = [0u8; 32];
        rng.try_fill_bytes(&mut salt).expect("fetched random bytes from OsRng");
        Salt(salt)
    }
}
