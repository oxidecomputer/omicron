-[ ] Fix tests
-[x] Add a storage monitor to report to nexus and handle dump setup
-[ ] Move sprockets server into LongRunningTasks
-[ ] Add a bunch of log messages
-[ ] Rename sled-agent/src/bootstrap/bootstore.rs to bootstore_setup.rs
-[ ] See if we can make `ServiceManager` part of long-running tasks (will this
require calling all the spawn methods directly in pre-server?)
-[ ] Change the name of `long_running_task_handles` to something else ?
-[ ] Fix installinator to use new code, and any other packages that need it.
