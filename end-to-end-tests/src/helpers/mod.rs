pub mod ctx;

use anyhow::{Context, Result};
use oxide_client::types::Name;
use rand::{thread_rng, Rng};
use std::future::Future;
use std::time::{Duration, Instant};

pub fn generate_name(prefix: &str) -> Result<Name> {
    format!("{}-{:x}", prefix, thread_rng().gen_range(0..0xfff_ffff_ffffu64))
        .try_into()
        .map_err(anyhow::Error::msg)
}

pub async fn try_loop<F, Fut, T, E>(mut f: F, timeout: Duration) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    Result<T, E>: Context<T, E>,
{
    let start = Instant::now();
    loop {
        match f().await {
            Ok(t) => return Ok(t),
            Err(err) => {
                if Instant::now() - start > timeout {
                    return Err(err).with_context(|| {
                        format!(
                            "try_loop timed out after {} seconds",
                            timeout.as_secs_f64()
                        )
                    });
                }
            }
        }
    }
}
