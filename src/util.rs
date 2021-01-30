/*!
 * Shared utility code for the OXCP prototype.
 */

use rand::{thread_rng, Rng};
use std::time::Duration;

/**
 * Implements a simple exponential backoff strategy, with a maximum delay
 * and number of retries. Each retry interval is slightly randomized, to
 * avoid multiple clients attempting simultaneous retries.
 *
 * This type is an iterator, so the easiest way to use it is call `.next()`
 * to retrieve the next retry delay. When the maximum number of retries
 * has been reached, the iterator is exhausted and returns `None`.
 */
#[derive(Debug, Copy, Clone)]
pub(crate) struct RetryBackoff {
    current_delay: Duration,
    backoff_factor: f64,
    randomization: f64,
    max_delay: Duration,
    current_retry: usize,
    n_max_retries: usize,
}

impl RetryBackoff {
    /**
     * Construct a new backoff strategy.
     */
    pub fn new(
        initial_delay: Duration,
        max_delay: Duration,
        n_max_retries: usize,
        backoff_factor: f64,
        randomization: f64,
    ) -> Result<RetryBackoff, String> {
        if backoff_factor <= 1.0 {
            return Err("backoff factor must be > 1.0".to_string());
        }
        if randomization <= 0.0 || randomization > 1.0 {
            return Err("randomization must be in (0, 1.0]".to_string());
        }
        if max_delay < initial_delay {
            return Err("maximum delay must be >= initial delay".to_string());
        }
        Ok(RetryBackoff {
            current_delay: initial_delay,
            backoff_factor,
            randomization,
            max_delay,
            current_retry: 0,
            n_max_retries,
        })
    }
}

impl Iterator for RetryBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if self.current_retry > self.n_max_retries {
            None
        } else {
            let current = self.current_delay;
            self.current_retry += 1;
            let range = 1.0..=(1.0 + self.randomization);
            let factor = self.backoff_factor * thread_rng().gen_range(range);
            self.current_delay = self.current_delay.mul_f64(factor);
            if self.current_delay > self.max_delay {
                self.current_delay = self.max_delay;
            }
            Some(current)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, RetryBackoff};

    #[test]
    fn test_retry_backoff() {
        let initial_delay = Duration::from_millis(100);
        let backoff_factor = 2.0;
        let randomization = 0.1;
        let max_delay = Duration::from_secs(100);
        let n_max_retries = 10;

        /*
         * Argument verification tests.
         */
        assert!(RetryBackoff::new(
            initial_delay,
            Duration::from_secs(0),
            n_max_retries,
            backoff_factor,
            randomization,
        )
        .is_err());
        assert!(RetryBackoff::new(
            initial_delay,
            max_delay,
            n_max_retries,
            0.5,
            randomization,
        )
        .is_err());
        assert!(RetryBackoff::new(
            initial_delay,
            max_delay,
            n_max_retries,
            backoff_factor,
            1.1,
        )
        .is_err());

        /*
         * Exercise actual backoff implementation.
         */
        let mut backoff = RetryBackoff::new(
            initial_delay,
            max_delay,
            n_max_retries,
            backoff_factor,
            randomization,
        )
        .expect("Failed to build RetryBackoff");

        let mut n_retries = 0;
        let delay = backoff.next().unwrap();
        assert_eq!(delay, initial_delay);

        assert!(
            (backoff.next().unwrap().as_secs_f64()
                - (backoff_factor * initial_delay.as_secs_f64()))
                <= randomization
        );
        n_retries += 1;

        assert!(
            (backoff.next().unwrap().as_secs_f64()
                - (backoff_factor * initial_delay.as_secs_f64()))
                > randomization
        );
        n_retries += 1;

        while let Some(_) = backoff.next() {
            n_retries += 1;
        }
        assert_eq!(n_retries, n_max_retries);
    }
}
