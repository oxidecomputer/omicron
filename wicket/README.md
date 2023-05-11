# Overview

Wicket is a TUI built for operator usage at the technician port. It is intended
to support a limited set of responsibilities including:
 * Rack Initialization
 * Boundary service setup
 * Disaster Recovery
 * Minimal rack update / emergency update

Wicket is built on top of [crossterm](https://github.com/crossterm-rs/
crossterm)  and [tui-rs](https://github.com/fdehau/tui-rs).

# Navigating

* `banners` - Files containing "banner-like" output using `#` characters for
glyph drawing
* `src/dispatch.rs` - Setup code for shell management, to allow uploading of
TUF repos or running the TUI.
* `src/upload.rs` - Code to upload a TUF repo to wicketd via wicket
* `src/wicketd.rs` - Code for interacting with wicketd 
* `src/runner` - The main entrypoint to the TUI. Runs the main loop and spawns
a tokio runtime to interact with wicketd.
* `src/ui` - All code for UI management. This contains the primary types of the
UI: `Controls` and `Widgets` which will be discussed in more detail below.
* `src/state` - Global state managed by wicket. This state is mutated by the
`Runner` mainloop, as well as by `Control::on` methods. It is used immutably to
draw the UI.

# Design

When wicket starts as a TUI, a `Runner` is created, which is really a bucket of
state which can be utilized by the `main_loop`. The `Runner` is in charge of:

The main type of the wicket crate is the `Wizard`. The wizard is run by the `wicket` binary and is in charge of:
 * Handling user input
 * Sending requests to wicketd
 * Handling events from downstream services
 * Dispatching events to the UI `Screen`
 * Triggering terminal rendering

There is a main thread that runs an infinite loop in the `Runner::main_loop`
method. The loop's job is to receive `Event`s from a single MPSC channel and
update internal state, either directly or by forwarding events to the `Screen`
by calling its `on` method. The `Screen`'s job is solely to dispatch events to
the splash screen at startup (to allow early cancellation of the animation),
and to the `MainScreen`  after the splash screen has finished its  animation.

The `MainScreen` is *stable* across the TUI, with a sidebar widget that allows
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

 * `Action::Redraw` - Instructs the `Runner` to call `Screen::draw` and
trigger a terminal render if necessary. This allows us to limit the relatively
expensive operation to those times when it's strictly necessary.
 * `Action::Update(ComponentId)` - Instructs the Runner to dispatch an update
command for a given component to `wicketd`.

It's important to notice that the global `State` of the system is only updated
upon event receipt, and that a screen never processes an event that can mutate
the global state and render in the same method. However, due to the stateful
rendering model of `tui.rs`, we do allow `Controls` to mutate their internal
state when executing the `draw` method. This allows reuse of `tui.rs` stateful
widgets like `list`. `Controls` create `tui::widget::Widgets` on demand during
rendering, which themselves display to a given subset of the terminal, known
as a `tui::layout::Rect`. If necessary, custom `Widgets` can be created, as
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

# Testing wicket as a captive shell

Wicket is meant to be used as a captive shell over ssh. To test the captive shell support on a local Unix machine:

1. Make the `wicket` available globally. For the rest of this section we're going to use the path `/usr/local/bin/wicket`.
    * If your build directory is globally readable, create a symlink to `wicket` in a well-known location. From omicron's root, run: `sudo ln -s $(readlink -f target/debug/wicket) /usr/local/bin/wicket`
    * If it isn't globally accessible, run `sudo cp target/debug/wicket /usr/local/bin`. (You'll have to copy `wicket` each time you build it.)
2. Add a new user to test against, for example `wicket-test`:
    1. Add a group for the new user: `groupadd wicket-test`.
    2. Add the user: `sudo useradd -m -g wicket-test`
3. Set up SSH authentication for this user, using either passwords or public keys (`.ssh/authorized_keys`).
    * To configure SSH keys, you'll need to first log in as the `wicket-test` user. To do so, run `sudo -u wicket-test -i` (Linux) or `pfexec su - wicket-test` (illumos).
    * If using `.ssh/authorized_keys`, be sure to set up the correct permissions for `~/.ssh` and its contents. As the `wicket-test` user, run `chmod go-rwx -R ~/.ssh`.
4. Test that you can log in as the user: run `ssh wicket-test@localhost`. If it works, move on to step 5. If it doesn't work:
    * To debug issues related to logging in, for example `~/.ssh` permissions issues, check the sshd authentication log.
    * On Linux, the authentication log is typically at `/var/log/auth.log`.
    * On illumos, the authentication log is at `/var/log/authlog`. If it is empty, logging needs to be enabled. (If you're an Oxide employee, see [this issue](https://github.com/oxidecomputer/helios-engvm/issues/18) for how to enable logging.)
5. Add this to the end of `/etc/ssh/sshd_config`:
    ```
    Match User wicket-test
        ForceCommand /usr/local/bin/wicket
    ```
6. Restart sshd:
    * Linux using systemd: `sudo systemctl restart ssh`
    * illumos: `svcadm restart ssh`

From now on, if you run `ssh wicket-test@localhost`, you should get the wicket captive shell. Also, `ssh wicket-test@localhost upload` should let you upload a zip file as a TUF repository.

# Testing upload functionality without a captive shell

If you don't want to test wicket as a captive shell and simply want to try out the upload functionality, run:

```
SSH_ORIGINAL_COMMAND=upload cargo run -p wicket
```

# Adding a test update step

While developing wicket, it can be useful to add a step which just reports
progress and otherwise does nothing else. To add such a step, set the
environment variable `WICKET_UPDATE_TEST_STEP_SECONDS` to an appropriate value.
For example:

```
WICKET_UPDATE_TEST_STEP_SECONDS=15 cargo run --bin wicket
```
