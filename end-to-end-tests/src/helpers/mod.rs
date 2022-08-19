pub mod ctx;

use anyhow::Result;
use oxide_client::types::Name;
use rand::{thread_rng, Rng};

pub fn generate_name(prefix: &str) -> Result<Name> {
    format!("{}-{:x}", prefix, thread_rng().gen_range(0..0xfff_ffff_ffffu64))
        .try_into()
        .map_err(anyhow::Error::msg)
}
