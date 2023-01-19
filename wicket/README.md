# Overview

Wicket is a TUI built for operator usage at the technician port. It is intended to support a limited set of responsibilities including:
 * Rack Initialization
 * Boundary service setup
 * Disaster Recovery
 * Minimal rack update / emergency update

Wicket is built on top of [crossterm](https://github.com/crossterm-rs/crossterm) 
and [tui-rs](https://github.com/fdehau/tui-rs), although the objects
themselves, and the placement of objects on the screen is mostly custom.

# Navigating

* `banners` - Files containing "banner-like" output using `#` characters for
glyph drawing
* `src/lib.rs` - Contains the main `Wizard` type which manages the UI,
downstream services, incoming events, and rendering
* `src/mgs.rs` - Source code for interacting with the [Management Gatewway
Service (MGS)](https://github.com/oxidecomputer/management-gateway-service)
* `src/inventory.rs` - Contains rack related components to show in the `Component` `Screen`
* `src/screens` - Each file represents a full terminal size view in the UI.
`Screen`s manage specific events and controls, and render `Widget`s. 
* `src/widgets` - These are implementations of a [`tui::Widget`](https://github.com/fdehau/tui-rs/blob/master/src/widgets/mod.rs#L63-L68) 
specific to wicket. Widgets are created only to be rendered immediately into a
[`tui::Frame`](https://github.com/fdehau/tui-rs/blob/9806217a6a4c240462bba3b32cb1bc59524f1bc2/src/terminal.rs#L58-L70). 
Most widgets contain a reference to state which is mutated by input events, and
defined in the same file as the widget. The state often implements a `Control`
that allows it to be manipulated  by the rest of the system in a unified
manner.

# Design

The main type of the wicket crate is the `Wizard`. The wizard is run by the `wicket` binary and is in charge of:
 * Handling user input (mouse, keyboard, resize) events
 * Sending requests to downstream services (MGS + RSS)
 * Handling events from downstream services
 * Managing the active screen and triggering terminal rendering

There is a main thread that runs an infinite loop in the `Wizard::mainloop`
method. The loop's job is to receive `Event`s from a single MPSC channel
and update internal state, either directly or by forwarding events to the
currently active screen by calling its `on` method. The active screen
processes events, updates its internal state (possibly including global state
passed in via the `on` method), and returns a list of `Action`s that instructs
the wizard what to do next. Currently there are only two types of `Action`s:

 * `Action::Redraw` - which instructs the Wizard to call the active screen's `draw` method
 * `Action::SwitchScreen(ScreenId)` - which instructs the wizard to transition between screens

It's important to notice that the internal state of the system is only updated
upon event receipt, and that a screen never processes an event that can
mutate state and render in the same method. This makes it very easy to test
the internal state mutations and behavior of a screen. It also means that all
drawing code is effectively stateless and fully immediate. While rendering
`Widget`s relies on the current state of the system, the state of the  system
does not change at all during rendering, and so an immutable borrow can be
utilized for this state. This fits well with the `tui-rs` immediate drawing
paradigm where widgets are created right before rendering and passed by value
to the render function, which consumes them.

Besides the main thread, which runs `mainloop`, there is a separate tokio
runtime which is used to drive communications with MGS and RSS, and to manage
inputs and timers. Requests are driven by MGS and RSS clients and all replies
are handled by these clients in the tokio runtime. Any important information
in these replies is forwarded as an `Event` over a channel to be received
in `mainloop`. All `Event`s, whether respones from downstream services, user
input, or timer ticks, are sent over the same channel in an `Event` enum. This
keeps the `mainloop` simple and provides a total ordering of all events, which
can allow for easier debugging.

As mentioned above, a timer tick is sent as an `Event::Tick` message over
a channel to the mainloop. Timers currently fire every 25ms, and help drive
any animations. We don't redraw on every timer tick, since it's relatively
expensive to calculate widget positions, and since the screens themselves
return actions when they need to be redrawn. However, the wizard also doesn't
know when a screen animation is ongoing, and so it forwards all ticks to the
currently active screen which returns an `Action::Redraw` if the screen needs
to be redrawn.

# Screens, Widgets, and Controls

A `Screen` represents the current visual state of the `Wizard` to the user, and
what inputs are available to the user. Each `Screen` maintains its own internal
state which can be mutated in response to events delivered to it via its `on`
method. The `on` method also provides mutable access to a globl `State` which
is relevant across sceens. As mentioned above, a `Screen::draw` method is
called to render the current screen.

Screens abstract the terminal display, or tty, which itself can be modeled
as a buffer of characters or a rectangle with a width and height, and x
and y coordinates for the upper left hand corner. This rectangle can be
further divided into rectangles that can be independently styled and drawn.
These rectangles can be manipulated directly, but in the common case this
manipulation is abstracted into a drawable `Widget`. We have implemented
several of our own widgets includng the rack view. Each screen has manual
placement code for these widgets which allows full flexibility and responsive
design.

Widgets get consumed when drawn to the screen. And the placement code
determines where the widget rectangles are drawn. However, how do we change the
styling of the Widgets, such that we know when a mouse hover is occurring or a
button was clicked? For this purpose, we must track the minimal state required
to render the widgets and implement a `Control`. Controls provide two key
things: access to the rectangle, or `Rect`, that we need in order to draw the
widget on the next render, and a unique ID, that allows screens to keep track
of which control is currently `active` or being hovered over. For example, when
a mouse movement event comes in, we can use rectangle intersection to see if
the mouse is currently over a given Control, and mark it as `hovered'.


# What's left?

There are currently 3 screens implemented:
 * Splash screen
 * Rack view screen
 * Component (Sled, Switch, PSC) view

Navigation and UI for these screens works well, but there is no backend
functionality implemented. All the inventory and power data shown in the
`Component` screen is fake. We also aren't currently really talking to the MGS
and RSS. Lastly, we don't have a way to take rack updates and install them, or
initialize the rack (including trust quorum). This is a lot of functionality
that will be implemented incrementally.

# Testing wicket as a login shell

Wicket is meant to be used as a login shell. To test the login shell on a local Unix machine:

1. Make the `wicket` available globally, at e.g. `/usr/local/bin/wicket`:
    * If your build directory is globally readable, create a symlink to `wicket` in a well-known location. From omicron's root, run: `sudo ln -s $(readlink -f target/debug/wicket) /usr/local/bin/wicket`
    * If it isn't globally accessible, run `sudo cp target/debug/wicket /usr/local/bin`. (You'll have to copy `wicket` each time you build it.)
2. Add a new user to test against, for example `wicket-test`:
    1. Add a group for the new user: `groupadd wicket-test`.
    2. Add the user: `sudo useradd -m -g wicket-test -s /usr/local/bin/wicket wicket-test`

At this point, you can use `sudo -u wicket-test -i` (Linux) or `pfexec su - wicket-test` (illumos) to test wicket as a login shell.

* A plain `sudo -u wicket-test -i` will show the TUI.
* `sudo -u wicket-test -i upload ...` will let you upload an artifact over stdin.

If you'd like to test connections over ssh, add your ssh key to the new user's `.ssh/authorized_keys`, then run `ssh wicket-test@localhost [upload ...]`.
