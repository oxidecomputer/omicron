Nexus API handlers (and other sagas) that start sagas have the ability
to wait for the completion of the saga by `await`ing the `RunningSaga`
future returned by `RunnableSaga::start`. Background tasks, on the other
hand, cannot currently do this, as their only interface to the saga
executor is an `Arc<dyn StartSaga>`, which provides only the
[`saga_start` method][1]. This method throws away the `RunningSaga`
returned by `RunnableSaga::start`, so the completion of the saga cannot
be awaited. In some cases, it's desirable for background tasks to be
able to await a saga running to completion. I described some motivations
for this in #6569.

This commit adds a new `StartSaga::saga_run` method to the `StartSaga`
trait, which starts a saga and returns a second future that can be
awaited to wait for the saga to finish. Since many tests use a
`NoopStartSaga` type which doesn't actually start sagas, this interface
still throws away most of the saga *outputs* provided by `StoppedSaga`,
to make it easier for the noop test implementation to implement this
method. If that's an issue in the future, we can revisit the interface,
and maybe make `NoopStartSaga` return fake saga results or something.

I've updated the `instance_watcher` and `instance_updater` background
tasks to use the `saga_run` method, because they were written with the
intent to spawn tasks that run sagas to completion --- this is necessary
for how the number of concurrent update sagas is *supposed* to be
limited by `instance_watcher`. I left the region-replacement tasks using
`StartSaga::saga_start`, because --- per @jmpesp --- the "fire and forget"
behavior is desirable there.

Closes #6569

[1]: https://github.com/oxidecomputer/omicron/blob/8be99b0c0dd18495d4a98187145961eafdb17d8f/nexus/src/app/saga.rs#L96-L108
