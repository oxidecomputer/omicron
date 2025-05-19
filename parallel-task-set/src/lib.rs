// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

/// The default number of parallel tasks used by [ParallelTaskSet].
pub const DEFAULT_MAX_PARALLELISM: usize = 16;

/// A collection of tokio tasks which execute in parallel on distinct tokio
/// tasks, up to a user-specified maximum amount of parallelism.
///
/// This parallelism is achieved by spawning tasks on a [JoinSet],
/// and may be further limited by the underlying machine's ability
/// to execute many tokio tasks.
///
/// # Why not just use FuturesUnordered?
///
/// FuturesUnordered can execute any number of futures concurrently,
/// but makes no attempt to execute them in parallel (assuming the underlying
/// futures are not themselves spawning additional tasks).
///
/// # Why not just use a JoinSet?
///
/// The tokio [JoinSet] has not limit on the "maximum number of tasks".
/// Given a bursty workload, it's possible to spawn an enormous number
/// of tasks, which may not be desirable.
///
/// Although [ParallelTaskSet] uses a [JoinSet] internally, it also
/// respects a parallism capacity.
pub struct ParallelTaskSet<T> {
    semaphore: Arc<Semaphore>,
    set: JoinSet<T>,
}

/// A reserved permit to spawn one additional task on a [`ParallelTaskSet`].
#[must_use = "a TaskSetPermit does nothing if not used"]
pub struct TaskSetPermit(OwnedSemaphorePermit);

impl<T: 'static + Send> Default for ParallelTaskSet<T> {
    fn default() -> Self {
        ParallelTaskSet::new()
    }
}

impl<T: 'static + Send> ParallelTaskSet<T> {
    /// Creates a new [ParallelTaskSet], with [DEFAULT_MAX_PARALLELISM] as the
    /// maximum number of tasks to run in parallel.
    ///
    /// If a different amount of parallism is desired, refer to:
    /// [Self::new_with_parallelism].
    pub fn new() -> ParallelTaskSet<T> {
        Self::new_with_parallelism(DEFAULT_MAX_PARALLELISM)
    }

    /// Creates a new [ParallelTaskSet], with `max_parallism` as the
    /// maximum number of tasks to run in parallel.
    pub fn new_with_parallelism(max_parallism: usize) -> ParallelTaskSet<T> {
        let semaphore = Arc::new(Semaphore::new(max_parallism));
        let set = JoinSet::new();

        Self { semaphore, set }
    }

    /// Spawn a task immediately, but only allow it to execute if the task
    /// set is within the maximum parallelism constraint.
    pub fn spawn<F>(&mut self, command: F)
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        let semaphore = Arc::clone(&self.semaphore);
        let _abort_handle = self.set.spawn(async move {
            // Hold onto the permit until the command finishes executing
            let _permit =
                semaphore.acquire_owned().await.expect("semaphore acquire");
            command.await
        });
    }

    pub fn spawn_with_permit(
        &mut self,
        TaskSetPermit(permit): TaskSetPermit,
        future: impl std::future::Future<Output = T> + Send + 'static,
    ) {
        let _abort_handle = self.set.spawn(async move {
            let result = future.await;
            // Hold onto the permit until the command finishes executing
            drop(permit);
            result
        });
    }

    pub async fn ready_to_spawn(&mut self) -> TaskSetPermit {
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        TaskSetPermit(permit)
    }

    /// Waits for the next task to complete and return its output.
    ///
    /// # Panics
    ///
    /// This method panics if the task returns a JoinError
    pub async fn join_next(&mut self) -> Option<T> {
        self.set.join_next().await.map(|r| r.expect("Failed to join task"))
    }

    /// Wait for all commands to execute and return their output.
    ///
    /// # Panics
    ///
    /// This method panics any of the tasks return a JoinError
    pub async fn join_all(self) -> Vec<T> {
        self.set.join_all().await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn test_spawn_many() {
        let count = Arc::new(AtomicUsize::new(0));

        let task_limit = 16;
        let mut set = ParallelTaskSet::new_with_parallelism(task_limit);

        for _ in 0..task_limit * 10 {
            set.spawn({
                let count = count.clone();
                async move {
                    // How many tasks - including our own - are running right
                    // now?
                    let watermark = count.fetch_add(1, Ordering::SeqCst) + 1;

                    // The tasks should all execute for a short but variable
                    // amount of time.
                    let duration_ms = rand::thread_rng().gen_range(0..10);
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        duration_ms,
                    ))
                    .await;

                    count.fetch_sub(1, Ordering::SeqCst);

                    watermark
                }
            });
        }

        let watermarks = set.join_all().await;

        for (i, watermark) in watermarks.into_iter().enumerate() {
            println!("task {i} saw {watermark} concurrent tasks");

            assert!(
                watermark <= task_limit,
                "Observed simultaneous task execution of {watermark} tasks on the {i}-th worker"
            );
        }
    }
}
