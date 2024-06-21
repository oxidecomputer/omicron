// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mapping of keys to behaviors interpreted by the UI
//!
//! The purpose of the keymap is allow making the operation of wicket consistent
//! while decoupling keys from actions on [`crate::Control`]s

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use serde::{Deserialize, Serialize};

use crate::state::ComponentId;

/// All commands handled by [`crate::Control::on`].
///
/// These are mostly user input commands from the keyboard,
/// but also include certain events like `Tick`.,
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cmd {
    /// Select the only available action for a current control
    /// This can trigger an operation, popup, etc...
    Enter,

    /// Exit the current context
    Exit,

    /// Toggle the currently-selected item (e.g., checkbox).
    Toggle,

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
    ///
    /// TODO: This is currently commented out because we need to translate key
    /// presses to something we can serialize/deserialize once we decide to use
    /// it.
    /// Raw(KeyEvent),

    /// Display details for the given selection
    /// This can be used to do things like open a scrollable popup for a given
    /// `Control`.
    Details,

    /// Display ignition control for the given selection
    Ignition,

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

    /// Begin an update.
    StartUpdate,

    /// Force cancel an update.
    AbortUpdate,

    /// Reset screen-specific state (e.g., clearing the state for a
    /// completed/failed update, or resetting the rack from the rack setup
    /// screen).
    ResetState,

    /// Begin rack setup.
    StartRackSetup,

    /// Page up.
    PageUp,

    /// Page down.
    PageDown,

    /// Goto top of list/screen/etc...
    GotoTop,

    /// Goto bottom of list/screen/etc...
    GotoBottom,

    /// Decline
    No,

    /// Easter Egg in Rack View
    KnightRiderMode,

    /// Trigger any operation that must be executed periodically, like
    /// animations.
    Tick,

    /// Display a popup.
    ///
    /// This isn't a key shortcut (it is typically driven by the system) but
    /// needs to be handled by screens the same way keys are.
    ShowPopup(ShowPopupCmd),

    /// Switch to the next pane.
    NextPane,

    /// Switch to the previous pane.
    PrevPane,

    /// Write the current snapshot to a file
    DumpSnapshot,
}

/// A command to display a popup.
///
/// Part of [`Cmd::ShowPopup`].
///
/// This isn't a key shortcut (it is typically driven by the system) but
/// needs to be handled by screens the same way keys are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShowPopupCmd {
    /// A response to a start-update request.
    StartUpdateResponse {
        component_id: ComponentId,
        response: Result<(), String>,
    },

    /// A response to a abort-update request.
    AbortUpdateResponse {
        component_id: ComponentId,
        response: Result<(), String>,
    },

    /// A response to a clear-update-state request.
    ClearUpdateStateResponse {
        component_id: ComponentId,
        response: Result<(), String>,
    },

    /// A response to a rack-setup request.
    StartRackSetupResponse(Result<(), String>),

    /// A response to a rack-reset request.
    StartRackResetResponse(Result<(), String>),
}

/// We allow certain multi-key sequences, and explicitly enumerate the starting
/// key(s) here.
#[derive(Debug, Clone, Copy)]
#[allow(non_camel_case_types)]
enum MultiKeySeqStart {
    g,
    CtrlR,
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
#[derive(Debug, Default, Clone, Copy)]
pub struct KeyHandler {
    seq: Option<MultiKeySeqStart>,
}

impl KeyHandler {
    pub fn on(&mut self, event: KeyEvent) -> Option<Cmd> {
        if let Some(seq) = self.seq {
            match seq {
                MultiKeySeqStart::g => match event.code {
                    KeyCode::Char('g') | KeyCode::Char('G') => {
                        self.seq = None;
                        return Some(Cmd::GotoTop);
                    }
                    KeyCode::Char('e') | KeyCode::Char('E') => {
                        self.seq = None;
                        return Some(Cmd::GotoBottom);
                    }
                    _ => (),
                },
                MultiKeySeqStart::CtrlR => match event.code {
                    KeyCode::Char('a') | KeyCode::Char('A')
                        if event.modifiers == KeyModifiers::CONTROL =>
                    {
                        self.seq = None;
                        return Some(Cmd::AbortUpdate);
                    }
                    KeyCode::Char('r') | KeyCode::Char('R')
                        if event.modifiers == KeyModifiers::CONTROL =>
                    {
                        self.seq = None;
                        return Some(Cmd::ResetState);
                    }
                    KeyCode::Char('s') | KeyCode::Char('S')
                        if event.modifiers == KeyModifiers::CONTROL =>
                    {
                        self.seq = None;
                        return Some(Cmd::DumpSnapshot);
                    }
                    KeyCode::Char('t') | KeyCode::Char('T')
                        if event.modifiers == KeyModifiers::CONTROL =>
                    {
                        self.seq = None;
                        return Some(Cmd::KnightRiderMode);
                    }
                    _ => (),
                },
            }
        }

        // We didn't match any multi-sequence starting characters. Treat this
        // key-press as a new key-press and reset the sequence.
        self.seq = None;

        let cmd = match event.code {
            KeyCode::Enter => Cmd::Enter,
            KeyCode::Esc => Cmd::Exit,
            KeyCode::Char(' ') => Cmd::Toggle,
            KeyCode::Char('e') | KeyCode::Char('E') => Cmd::Expand,
            KeyCode::Char('c') | KeyCode::Char('C') => Cmd::Collapse,
            KeyCode::Char('d') | KeyCode::Char('D') => Cmd::Details,
            KeyCode::Char('i') | KeyCode::Char('I') => Cmd::Ignition,
            KeyCode::Up => Cmd::Up,
            KeyCode::Down => Cmd::Down,
            KeyCode::Right => Cmd::Right,
            KeyCode::Left => Cmd::Left,
            KeyCode::PageUp => Cmd::PageUp,
            KeyCode::PageDown => Cmd::PageDown,
            KeyCode::Home => Cmd::GotoTop,
            KeyCode::End => Cmd::GotoBottom,
            KeyCode::Char('y') | KeyCode::Char('Y') => Cmd::Yes,
            KeyCode::Char('u') | KeyCode::Char('U')
                if event.modifiers == KeyModifiers::CONTROL =>
            {
                Cmd::StartUpdate
            }
            KeyCode::Char('r') | KeyCode::Char('R')
                if event.modifiers == KeyModifiers::CONTROL =>
            {
                self.seq = Some(MultiKeySeqStart::CtrlR);
                return None;
            }
            KeyCode::Char('k') | KeyCode::Char('K')
                if event.modifiers == KeyModifiers::CONTROL =>
            {
                Cmd::StartRackSetup
            }
            KeyCode::Char('n') | KeyCode::Char('N') => Cmd::No,
            KeyCode::Tab => Cmd::NextPane,
            KeyCode::BackTab => Cmd::PrevPane,

            // Vim navigation
            KeyCode::Char('k') => Cmd::Up,
            KeyCode::Char('j') => Cmd::Down,
            KeyCode::Char('h') => Cmd::Left,
            KeyCode::Char('l') => Cmd::Right,
            KeyCode::Char('g') => {
                self.seq = Some(MultiKeySeqStart::g);
                return None;
            }
            _ => return None,
        };
        Some(cmd)
    }
}
