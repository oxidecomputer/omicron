# Falcon VM based testing

It is now possible to run omicron tests in a VM. Any test can be run in a VM,
but many of them will not work, because their dependencies are not installed.
This is possible, and can be done in a number of ways. We just need to think
about how we want to do it.

Running an individual test in a VM currently works by:

 1. Building the test binary
 2. Copying it into a temporary directory
 3. Mounting that temporary directory inside the VM
 4. Logging into and running the binary with the given test name over the
    serial port.

## Running an Omicron test

An individual test can be run in the following manner:

```shell
cd omicron
CARGO_TARGET_X86_64_UNKNOWN_ILLUMOS_RUNNER=./test-utils/src/bin/falcon_runner.sh cargo nextest run -p <package> <test name> --nocapture
```

A concrete example, which can be used for sanity checking that the test actually runs
in a VM, is provided in the `omicron-test-utils` package, and named `launch`.

```shell
CARGO_TARGET_X86_64_UNKNOWN_ILLUMOS_RUNNER=./test-utils/src/bin/falcon_runner.sh cargo nextest run -p omicron-test-utils launch --nocapture
```

## Logging into a test VM

On failure, the test VM remains running for debugging purposes. All VMs
currently share the name `launchpad_mcduck_test_vm`, although the falcon
deployment metadata resides in a unique temp directory. At the start of each
test, and upon failure this directory is printed to the console.

```
Setting falcon directory to /tmp/.tmpXcLx5g
...
Test failed: VM remains running, with falcon dir: /tmp/.tmpXcLx5g
```

A user can login to that VM via the serial port with the following command:

```shell
cargo run -p omicron-test-utils --bin falcon_runner_cli serial launchpad_mcduck_test_vm -f /tmp/.tmpXcLx5g
```

Similarly, a user can execute a command on the VM via:

```shell
cargo run -p omicron-test-utils --bin falcon_runner_cli exec launchpad_mcduck_test_vm '<CMD>' -f /tmp/.tmpXcLx5g
```

This can be used to cat log files, etc..

Once a user is done debugging the VM, they should tear it and its associated
resources down with the following command. Note that here, you don't have to
name the VM, as the entire deployment is torn down. However, you do need to
use `pfexec`.

```shell
pfexec cargo run -p omicron-test-utils --bin falcon_runner_cli destroy -f /tmp/.tmpXcLx5g
```

## Troubleshooting

If for some reason you fail to tear down your VM and need to do it manually you
must remove the VM and its backing filesystems. Don't worry about the falcon
dirs, as they are just small directories in `/tmp`.

VM backing images are created in zfs under `rpool/falcon/topo`. These can be
destroyed via the following command. Note that this will destroy the backing
filesystems for **all** of your falcon VMs. If you want to be more precise, you
should `zfs list` and figure out what you want to kill.

```shell
pfexec zfs destroy -r rpool/falcon/topo
```

Similarly, all your bhyve backed VMs live in `/dev/vmm`. All VMs can be torn
down with the following command, although you should be careful when doing this
on shared infrastructure.

```shell
for i in `ls /dev/vmm`; do pfexec bhyvectl --vm=$i --destroy; done
```

For more troubleshooting info, see the [falcon wiki](https://github.com/oxidecomputer/falcon/wiki/Reference)

## Current Limitations

* Tests must be self-contained and not rely on dependencies. E2E tests will
not  currently work. We can easily install pre-reqs via our scripts, but this
will delay each test run. We may instead want to build falcon base images with
the pre-reqs included. There's a tradeoff here of test run time, vs getting the
latest bits.
* The names of the test runner, and VM are currently hardcoded, and start with
`launchpad_mcduck`. Therefore, tests can only be run serially for now. This is
an easy limitation to lift, we just need to figure out a naming convention and
do it.
* The commands to run tests are quite verbose. We should really have a wrapper
script.





