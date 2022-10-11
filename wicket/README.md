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
