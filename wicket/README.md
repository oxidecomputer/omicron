# Overview

Wicket is a TUI built for operator usage at the technician port. It is intended
to support a limited set of responsibilities including:

- Rack Initialization
- Boundary service setup
- Disaster Recovery
- Minimal rack update / emergency update

Wicket is built on top of [crossterm](https://github.com/crossterm-rs/
crossterm) and [tui-rs](https://github.com/fdehau/tui-rs).

# Navigating

- `banners` - Files containing "banner-like" output using `#` characters for
  glyph drawing
- `src/dispatch.rs` - Setup code for shell management, to allow uploading of
  TUF repos or running the TUI.
- `src/upload.rs` - Code to upload a TUF repo to wicketd via wicket
- `src/wicketd.rs` - Code for interacting with wicketd
- `src/runner` - The main entrypoint to the TUI. Runs the main loop and spawns
  a tokio runtime to interact with wicketd.
- `src/ui` - All code for UI management. This contains the primary types of the
  UI: `Controls` and `Widgets` which will be discussed in more detail below.
- `src/state` - Global state managed by wicket. This state is mutated by the
  `Runner` mainloop, as well as by `Control::on` methods. It is used immutably to
  draw the UI.

# Design

When wicket starts as a TUI, a `Runner` is created, which is really a bucket of
state which can be utilized by the `main_loop`. The `Runner` is in charge of:

The main type of the wicket crate is the `Wizard`. The wizard is run by the `wicket` binary and is in charge of:

- Handling user input
- Sending requests to wicketd
- Handling events from downstream services
- Dispatching events to the UI `Screen`
- Triggering terminal rendering

There is a main thread that runs an infinite loop in the `Runner::main_loop`
method. The loop's job is to receive `Event`s from a single MPSC channel and
update internal state, either directly or by forwarding events to the `Screen`
by calling its `on` method. The `Screen`'s job is solely to dispatch events to
the splash screen at startup (to allow early cancellation of the animation),
and to the `MainScreen` after the splash screen has finished its animation.

The `MainScreen` is _stable_ across the TUI, with a sidebar widget that allows
selecting among a list of `Pane`s. `Pane`s get shown to the right of the
sidebar, and are available to render to the rectangle available to them in that
space. Each pane is responsible for rendering in its own space, and handling
events when it is `selected` or `active`. Once inside a `Pane`, we enter the
land of `Control`s. Each `Control` has 2 methods: `on` for handling events, and
`draw` for rendering to the screen. Controls can arbitrarily nest, and each
control can handle events and/or dispatch them down to controls. Currently,
there is no bubbling back up of events. This is unlike some UIs where events
first go to the deepest node in the tree and are passed upwards. Due to the
limited space of the terminal and the consistency needs of the somewhat non-
generic UI, we stick to the simpler model of top down event handling. However,
each `Control`, returns an `Option<Action>` after handling an event, and
so these actions bubble up to each parent `Control`, and eventually to the
`Runner` if not swallowed. Currently, actions are only handled by the runner
and never directly inspected by parent Controls, but its always possible this
will change. There are only two `Action`s at this point that are handled by
the `Runner`.

- `Action::Redraw` - Instructs the `Runner` to call `Screen::draw` and
  trigger a terminal render if necessary. This allows us to limit the relatively
  expensive operation to those times when it's strictly necessary.
- `Action::Update(ComponentId)` - Instructs the Runner to dispatch an update
  command for a given component to `wicketd`.

It's important to notice that the global `State` of the system is only updated
upon event receipt, and that a screen never processes an event that can mutate
the global state and render in the same method. However, due to the stateful
rendering model of `tui.rs`, we do allow `Controls` to mutate their internal
state when executing the `draw` method. This allows reuse of `tui.rs` stateful
widgets like `list`. `Controls` create `ratatui::widget::Widgets` on demand during
rendering, which themselves display to a given subset of the terminal, known
as a `ratatui::layout::Rect`. If necessary, custom `Widgets` can be created, as
we have done with the `Rack` and `BoxConnector` widgets. Custom widgets can be
found in `src/ui/widgets`.

Besides the main thread, which runs `main_loop`, there is a separate tokio
runtime which is used to drive communications with `wicketd`, and to manage
inputs and timers. Requests are driven by wicketd clients and all replies
are handled by these clients in the tokio runtime. Any important information
in these replies is forwarded as an `Event` over a channel to be received
in `main_loop`. All `Event`s, whether respones from downstream services, user
input, or timer ticks, are sent over the same channel in an `Event` enum. This
keeps the `main_loop` simple and provides a total ordering of all events, which
can allow for easier debugging.

As mentioned above, a timer tick is sent as an `Event::Tick` message over
a channel to the main_loop. Timers currently fire every 25ms, and help drive
any animations. We don't redraw on every timer tick, since it's relatively
expensive to calculate widget positions, and since the screens themselves
return actions when they need to be redrawn. However, the `Runner` also doesn't
know when a screen animation is ongoing, and so it forwards all ticks to the
`Screen` which returns an `Action::Redraw` if a redraw is necessary.

# Manually testing wicket

Use these to test out particular scenarios with wicket by hand. (Feel free to
add more as needed!)

## Running an end-to-end-ish test

Part of the edit/compile cycle for wicket mupdates is setting up something
similar to an end-to-end flow. As a reminder, the general way updates work is
that wicket communicates with wicketd, which instructs MGS to send commands to
the individual SPs.

Based on this, one way to have an end-to-end flow is with:

- real wicketd
- real MGS
- sp-sim, an in-memory service that simulates how the SP behaves

Making this simpler is tracked in
[omicron#5550](https://github.com/oxidecomputer/omicron/issues/5550).

### Running sp-sim and MGS

The easiest way to do this is to run:

```
cargo run -p omicron-dev mgs-run
```

This will print out a line similar to `omicron-dev: MGS API: http://[::1]:12225`. Note the address for use below.

Another option, which may lead to quicker iteration cycles if you're modifying
MGS or sp-sim, is to run the services by hand from the root of omicron:

```
cargo run --bin sp-sim -- sp-sim/examples/config.toml
cargo run --bin mgs run --id c19a698f-c6f9-4a17-ae30-20d711b8f7dc --address '[::1]:12225' gateway/examples/config.toml
```

The port number in `--address` is arbitrary.

**Note:** If you're adding new functionality to wicket, it is quite possible
that sp-sim is missing support for it! Generally, sp-sim has features added to
it on an as-needed basis.

### Using a real SP

TODO

### Running wicketd

Taking the port number mentioned above, run:

```
cargo run -p wicketd -- run wicketd/examples/config.toml --address '[::1]:12226' --artifact-address '[::]:12227' --nexus-proxy-address '[::1]:12228' --mgs-address '[::1]:12225'
```

In this case, the port number in `--address` provides the interface between
wicketd and wicket. The port number is _not_ arbitrary: wicket connects to port
12226 by default. There is currently no way to specify a different port (but
there probably should be!)

### Running wicket

After running the above commands, simply running `cargo run -p wicket` should
connect to the wicketd instance.

## Adding simulated failures to operations

Add a simulated failure while starting an update:

```
WICKET_TEST_START_UPDATE_ERROR=<value> cargo run --bin wicket
```

Add a simulated failure while clearing update state:

```
WICKET_TEST_CLEAR_UPDATE_STATE_ERROR=<value> cargo run --bin wicket
```

Here, `<value>` can be:

- `fail`: Simulate a failure for this operation.
- `timeout`: Simulate a timeout for this operation.
  - `timeout:<secs>`: Specify a custom number of seconds (15 seconds by
    default)
- (implement more options as needed)

## Adding a test update step

Add a step which just reports progress and otherwise does nothing else. To add
such a step, set the environment variable `WICKET_UPDATE_TEST_STEP_SECONDS` to
an appropriate value. For example:

```
WICKET_UPDATE_TEST_STEP_SECONDS=15 cargo run --bin wicket
```

## Adding simulated results to individual steps

Some individual steps support having simulated results via environment variables.

Environment variables supported are:

- `WICKET_UPDATE_TEST_SIMULATE_ROT_RESULT`: Simulates a result for the "Updating RoT" step.
- `WICKET_UPDATE_TEST_SIMULATE_SP_RESULT`: Simulates a result for the "Updating SP" step.

The environment variable can be set to:

- `success`: A success outcome.
- `warning`: Success with warning.
- `failure`: A failure.
- `skipped`: A skipped outcome.

### Example

If wicket is invoked as:

```
WICKET_UPDATE_TEST_SIMULATE_ROT_RESULT=skipped cargo run --bin wicket
```

Then, while performing an update, the "Updating RoT" step will be simulated as skipped.

![Screenshot showing that the "Updating RoT" step has a "skipped" status with a message saying "Simulated skipped result"](https://user-images.githubusercontent.com/180618/254689686-99259bc0-4e68-421d-98ca-362774eef155.png).

## Testing upload functionality

Test upload functionality without setting up wicket as an SSH captive shell (see below for instructions). (This is the most common use case.)

```
SSH_ORIGINAL_COMMAND=upload cargo run -p wicket < my-tuf-repo.zip
```

Test upload functionality if wicket is set up as an SSH captive shell:

```
ssh user@$IP_ADDRESS upload < my-tuf-repo.zip
```

## Testing wicket as an SSH captive shell

Wicket is meant to be used as a captive shell over ssh. If you're making changes to the SSH shell support, you'll likely want to test the captive shell support on a local Unix machine. Here's how to do so.

1. Make the `wicket` available globally. For the rest of this section we're going to use the path `/usr/local/bin/wicket`.
   - If your build directory is globally readable, create a symlink to `wicket` in a well-known location. From omicron's root, run: `sudo ln -s $(readlink -f target/debug/wicket) /usr/local/bin/wicket`
   - If it isn't globally accessible, run `sudo cp target/debug/wicket /usr/local/bin`. (You'll have to copy `wicket` each time you build it.)
2. Add a new user to test against, for example `wicket-test`:
   1. Add a group for the new user: `groupadd wicket-test`.
   2. Add the user: `sudo useradd -m -g wicket-test`
3. Set up SSH authentication for this user, using either passwords or public keys (`.ssh/authorized_keys`).
   - To configure SSH keys, you'll need to first log in as the `wicket-test` user. To do so, run `sudo -u wicket-test -i` (Linux) or `pfexec su - wicket-test` (illumos).
   - If using `.ssh/authorized_keys`, be sure to set up the correct permissions for `~/.ssh` and its contents. As the `wicket-test` user, run `chmod go-rwx -R ~/.ssh`.
4. Test that you can log in as the user: run `ssh wicket-test@localhost`. If it works, move on to step 5. If it doesn't work:
   - To debug issues related to logging in, for example `~/.ssh` permissions issues, check the sshd authentication log.
   - On Linux, the authentication log is typically at `/var/log/auth.log`.
   - On illumos, the authentication log is at `/var/log/authlog`. If it is empty, logging needs to be enabled. (If you're an Oxide employee, see [this issue](https://github.com/oxidecomputer/helios-engvm/issues/18) for how to enable logging.)
5. Add this to the end of `/etc/ssh/sshd_config`:
   ```
   Match User wicket-test
       ForceCommand /usr/local/bin/wicket
   ```
6. Restart sshd:
   - Linux using systemd: `sudo systemctl restart ssh`
   - illumos: `svcadm restart ssh`

From now on, if you run `ssh wicket-test@localhost`, you should get the wicket captive shell. Also, `ssh wicket-test@localhost upload` should let you upload a zip file as a TUF repository.
