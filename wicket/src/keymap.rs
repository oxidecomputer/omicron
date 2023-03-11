// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mapping of keys to behaviors interpreted by the UI
//!
//! The purpose of the keymap is allow making the operation of wicket consistent
//! while decoupling keys from actions on [`crate::Control`]s

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// All commands handled by [`crate::Control::on`].
///
/// These are mostly user input commands from the keyboard,
/// but also include certain events like `Tick`.,
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
    /// Select the only available action for a current control
    /// This can trigger an operation, popup, etc...
    Enter,

    /// Exit the current context
    Exit,

    /// Expand the current tree context
    Expand,

    /// Collapse the current tree context
    Collapse,

    /// Raw mode directly passes key presses through to the underlying
    /// [`crate::Control`]s where the user needs to directly input text.
    ///
    /// When a user `Select`s  a user input an [`crate::Action`]  will be
    /// returned that establishes Raw mode. When the context is exited, `Raw`
    /// mode will be disabled.
    Raw(KeyEvent),

    /// Display details for the given selection
    /// This can be used to do things like open a scollable popup for a given
    /// `Control`.
    Details,

    /// Move up or scroll up
    Up,

    /// Move down or scroll down
    Down,

    /// Move right
    Right,

    /// Move left
    Left,

    /// Accept
    Yes,

    /// Decline
    No,

    /// Easter Egg in Rack View
    KnightRiderMode,

    /// Trigger any operation that must be executed periodically, like
    /// animations.
    Tick,
}

/// A Key Handler maintains any state that is needed across key presses,
/// such as whether the user is in `insert` mode, or a key sequence is
/// being processed.
///
/// Return the [`Cmd`] that gets interpreted or `None` if the key press is part
/// of a sequence or not a valid key press.
///
/// Note: We don't handle raw events or key sequences yet, although this is
/// possible.
#[derive(Debug, Default)]
pub struct KeyHandler {}

impl KeyHandler {
    pub fn on(&mut self, event: KeyEvent) -> Option<Cmd> {
        let cmd = match event.code {
            KeyCode::Enter => Cmd::Enter,
            KeyCode::Esc => Cmd::Exit,
            KeyCode::Char('e') => Cmd::Expand,
            KeyCode::Char('c') => Cmd::Collapse,
            KeyCode::Char('d') => Cmd::Details,
            KeyCode::Up => Cmd::Up,
            KeyCode::Down => Cmd::Down,
            KeyCode::Right => Cmd::Right,
            KeyCode::Left => Cmd::Left,
            KeyCode::Char('y') => Cmd::Yes,
            KeyCode::Char('n') => Cmd::No,
            KeyCode::Char('k') if event.modifiers == KeyModifiers::CONTROL => {
                Cmd::KnightRiderMode
            }
            _ => return None,
        };
        Some(cmd)
    }
}
