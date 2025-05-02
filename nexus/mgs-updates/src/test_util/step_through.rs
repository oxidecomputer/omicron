// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use futures::future::BoxFuture;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use tokio::sync::mpsc;

/// Steps through *wakeups* of a `Future`
pub struct StepThrough<'a, T> {
    fut: BoxFuture<'a, T>,
}

impl<'a, T> StepThrough<'a, T> {
    pub fn new(fut: BoxFuture<'a, T>) -> StepThrough<'a, T> {
        StepThrough { fut }
    }

    /// Run the future to the next await point where it blocks, then also wait
    /// for it to be ready to run again.
    ///
    /// This is not quite the same as running to an await point, since it's
    /// possible that the future doesn't block at the await point.  In that
    /// case, it will keep running and this function will not notice until the
    /// next time the function does block.
    pub async fn step(mut self) -> StepResult<'a, T> {
        let (tx, mut rx) = mpsc::channel(16);
        let my_waker = StepThroughWaker(tx);
        let waker = Waker::from(Arc::new(my_waker));
        let mut context = Context::from_waker(&waker);

        // This is an oversimplification, but you can think of this like:
        //
        // - On entry to this function, the future is generally ready to run (to
        //   its next await point).
        // - This function polls the future, causing it to run until it either
        //   completes or reaches an await point.
        // - If the future reaches an await point, this function waits for the
        //   future to be woken up again.
        // - Then this function returns, preserving the invariant that on entry,
        //   it's ready to run again.
        match self.fut.as_mut().poll(&mut context) {
            Poll::Pending => {
                // The future has reached an await point.  We want to wake up
                // when it gets woken up.  We gave its `poll()` a waker that
                // will send a message on this channel when that happens.
                //
                // unwrap(): this only fails if the tx side has been dropped,
                // which means the waker was dropped.  It's not clear how this
                // could happen unless the future was being dropped, in which
                // case this object must also be dropped.
                rx.recv().await.unwrap();
                StepResult::ReadyAgain(StepThrough { fut: self.fut })
            }
            Poll::Ready(v) => StepResult::Done(v),
        }
    }

    /// Run the future to completion.
    pub async fn finish(self) -> T {
        self.fut.await
    }
}

pub enum StepResult<'a, T> {
    ReadyAgain(StepThrough<'a, T>),
    Done(T),
}

impl<T> StepResult<'_, T> {
    pub async fn step(self) -> Self {
        match self {
            StepResult::ReadyAgain(stepper) => stepper.step().await,
            StepResult::Done(_) => self,
        }
    }
}

struct StepThroughWaker(mpsc::Sender<()>);
impl Wake for StepThroughWaker {
    fn wake(self: Arc<Self>) {
        // `send()` will only fail if the other side of the channel has been
        // dropped.  There's nothing for us to do in that case.
        let _ = self.0.try_send(());
    }
}

#[cfg(test)]
mod test {
    use super::StepResult;
    use super::StepThrough;
    use futures::FutureExt;

    /// helper future for testing
    async fn the_test_future() -> usize {
        for i in 0..10 {
            eprintln!("the_test_future: waiting ({i})");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        50
    }

    #[tokio::test]
    async fn test_basic() {
        // It's tempting to try to use channels to carefully orchestrate exactly
        // when our test future blocks.  But we need to make sure it actually
        // does block.   If a message is ready on the channel at the point when
        // it goes read it, it will never block and we'll never step through it.
        // So we'd need at least two tasks: one that's somehow blocking on the
        // test future to block and one that's waiting for step() to complete.
        // It's far simpler to just have the test future sleep a bit.
        let fut = the_test_future().boxed();

        let mut step_through = StepThrough::new(fut);
        let mut nsteps = 0;
        loop {
            eprintln!("test: stepping");
            match step_through.step().await {
                StepResult::ReadyAgain(s) => {
                    nsteps += 1;
                    eprintln!("test: stepped ({nsteps} steps, not done)");
                    step_through = s;
                }
                StepResult::Done(v) => {
                    nsteps += 1;
                    eprintln!(
                        "test: stepped (done after {nsteps} steps: value = {v})"
                    );
                    assert_eq!(v, 50);
                    break;
                }
            }

            if nsteps > 20 {
                panic!("got into infinite loop");
            }
        }

        // The future should have blocked 10 times.  That's 11 steps: one for
        // each block plus one to get to the end.
        assert_eq!(11, nsteps);
    }
}
