// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The inverse of `clap` parsing: turning a parsed command back into the
//! arguments that parse to it.
//!
//! `clap` has no un-parse (clap-rs/clap#3131). This crate adds one as a
//! trait, [`Unparse`], whose implementation pushes a command's words into an
//! [`Argv`]. The words are what `clap` consumes, one argument each, so no
//! implementation has to think about quoting: [`Argv`]'s `Display` joins
//! them into a shell line, quoting whatever needs it, and
//! `shell_words::split` of that line gives the words back.
//!
//! `#[derive(Unparse)]` writes the implementation for a type that derives
//! clap's `Subcommand` or `Args`. It takes each variant's subcommand name
//! from the item, the way `clap`'s derive does, and everything else about
//! an argument (positional or `--long`, value delimiter, action, `--`
//! rules) from the `clap::Command` the type builds at run time, so it never
//! re-derives clap's `#[arg]` rules. Within one command the words come out
//! positionals first, then options, then any subcommand, whatever the
//! declaration order; when a positional needs `--` or takes everything
//! after it, the options come first instead. A value is spelled by clap's canonical
//! name if its type is a `ValueEnum`, and by its `Display` otherwise; a
//! type whose `Display` does not invert its parser needs a newtype whose
//! does, and a generic value type is not supported. The generated code
//! names this crate as `omicron_clap_unparse`, so it must be a direct,
//! unrenamed dependency.
//!
//! A value containing its argument's `value_delimiter` cannot round-trip;
//! that is clap's limit, not this crate's.
//!
//! Whether an implementation really is the inverse of the parser is the
//! caller's test to write: parse the unparsed words, and compare.

use std::fmt;

use clap::ValueEnum;

pub use omicron_clap_unparse_derive::Unparse;

/// The inverse of parsing: the words that parse back to `self`.
///
/// An implementation pushes its own name if it has one, then its arguments,
/// in an order the parser accepts. The result is normalized, not verbatim:
/// an alias comes out as the canonical name, and a value comes out however
/// its `Display` spells it.
pub trait Unparse {
    fn unparse(&self, argv: &mut Argv);

    /// The words of `self`, in a fresh [`Argv`].
    fn to_argv(&self) -> Argv {
        let mut argv = Argv::default();
        self.unparse(&mut argv);
        argv
    }
}

/// The words of a command, as [`Unparse`] builds them.
///
/// Each method pushes zero or more words and returns `self`, so a
/// hand-written implementation is one chain per variant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Argv(Vec<String>);

impl Argv {
    /// A verb or flag name.
    pub fn word(&mut self, word: &str) -> &mut Self {
        self.0.push(word.to_owned());
        self
    }

    /// A positional or flag value, via its `Display`.
    pub fn value(&mut self, value: impl fmt::Display) -> &mut Self {
        self.0.push(value.to_string());
        self
    }

    /// A [`ValueEnum`] value, by clap's canonical name for it.
    ///
    /// # Panics
    ///
    /// If `value` is a `#[value(skip)]` variant, which has no name.
    pub fn choice(&mut self, value: &impl ValueEnum) -> &mut Self {
        self.word(&choice_name(value))
    }

    /// `flag value`, or nothing if `value` is `None`.
    ///
    /// A value that starts with `-` is attached as `flag=value`, which
    /// clap accepts whether or not the argument allows hyphen values.
    pub fn option(
        &mut self,
        flag: &str,
        value: Option<impl fmt::Display>,
    ) -> &mut Self {
        if let Some(value) = value {
            let value = value.to_string();
            let attach = value.starts_with('-');
            self.flag_value(flag, value, attach);
        }
        self
    }

    /// `flag` if `on`, or nothing.
    pub fn flag(&mut self, flag: &str, on: bool) -> &mut Self {
        if on {
            self.word(flag);
        }
        self
    }

    /// `flag value`, or, if `attach`, `flag=value` for a long flag and
    /// `flagvalue` for a short one.
    fn flag_value(
        &mut self,
        flag: &str,
        value: String,
        attach: bool,
    ) -> &mut Self {
        if attach {
            if flag.starts_with("--") {
                self.word(&format!("{flag}={value}"))
            } else {
                self.word(&format!("{flag}{value}"))
            }
        } else {
            self.word(flag).value(value)
        }
    }

    /// The words so far.
    pub fn words(&self) -> &[String] {
        &self.0
    }

    /// The words, giving up the builder.
    pub fn into_words(self) -> Vec<String> {
        self.0
    }
}

impl fmt::Display for Argv {
    /// The words as one shell line, quoted so that `shell_words::split`
    /// gives back exactly [`Argv::words`].
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&shell_words::join(&self.0))
    }
}

/// Clap's canonical name for a [`ValueEnum`] value: the first of its
/// possible names, without aliases.
///
/// # Panics
///
/// If `value` is a `#[value(skip)]` variant, which has no name. Such a
/// variant can never have been parsed, so an unparse never meets one.
pub fn choice_name(value: &impl ValueEnum) -> String {
    value
        .to_possible_value()
        .expect("no value is skipped")
        .get_name()
        .to_owned()
}

/// What `#[derive(Unparse)]`'s generated code calls. Not a public API.
#[doc(hidden)]
pub mod __private {
    use std::fmt;
    use std::marker::PhantomData;

    use clap::builder::ArgAction;
    use clap::{Args, Command, Subcommand, ValueEnum};

    pub use clap;

    pub use super::{Argv, Unparse};

    /// The `clap::Command` for the subcommand named `name` of `S`.
    ///
    /// The command is built first, which is where clap runs its own debug
    /// assertions on the vocabulary.
    ///
    /// # Panics
    ///
    /// If `S` has no such subcommand, which means the macro's idea of a
    /// variant's name disagrees with clap's.
    pub fn subcommand<S: Subcommand>(name: &str) -> Command {
        let mut cmd = S::augment_subcommands(Command::new("unparse"));
        cmd.build();
        cmd.find_subcommand(name).cloned().unwrap_or_else(|| {
            panic!("clap built no subcommand named `{name}` for this variant")
        })
    }

    /// The `clap::Command` holding the arguments of `A`.
    pub fn args<A: Args>() -> Command {
        let mut cmd = A::augment_args(Command::new("unparse"));
        cmd.build();
        cmd
    }

    /// The arguments of one derived struct, emitted into the level of the
    /// command they belong to. Derived alongside [`Unparse`] for an `Args`
    /// struct, so that a `#[command(flatten)]` field can add its arguments
    /// to its parent's level.
    pub trait UnparseArgs {
        fn unparse_args(&self, cmd: &Command, level: &mut Level);
    }

    /// The words of one command level, ordered so that clap accepts them
    /// whatever the declaration order: positionals, then options and
    /// flags, then `--` and the `last` positionals if there are any, then
    /// a subcommand. When a positional value starts with `-` and clap was
    /// not told to allow that, or a positional takes everything after it
    /// (`trailing_var_arg`), the options come first instead, then `--` if
    /// needed, then the positionals.
    #[derive(Default)]
    pub struct Level {
        options: Argv,
        positionals: Argv,
        /// `#[arg(last = true)]` positionals, which clap takes only after
        /// `--`.
        last: Argv,
        /// Whether an ordinary positional needs `--` before it.
        hyphen: bool,
        /// Whether a positional takes everything after it.
        trailing: bool,
        subcommand: Option<Argv>,
    }

    impl Level {
        pub fn new() -> Self {
            Self::default()
        }

        /// The words of a `#[command(subcommand)]` field, which come after
        /// everything else at this level.
        ///
        /// # Panics
        ///
        /// If called twice: clap allows one subcommand per command.
        pub fn subcommand(&mut self, inner: &dyn Unparse) {
            assert!(self.subcommand.is_none(), "a command has one subcommand");
            self.subcommand = Some(inner.to_argv());
        }

        /// Appends this level's words to `argv`.
        ///
        /// # Panics
        ///
        /// If a positional needs `--` and there is also a subcommand: after
        /// `--` clap takes every word as a value, so no spelling reaches
        /// the subcommand.
        pub fn finish(self, argv: &mut Argv) {
            let separator = self.hyphen || !self.last.0.is_empty();
            if separator {
                assert!(
                    self.subcommand.is_none(),
                    "a positional value that needs `--` cannot precede a subcommand"
                );
            }
            if self.hyphen || self.trailing {
                argv.0.extend(self.options.0);
                if separator {
                    argv.word("--");
                }
                argv.0.extend(self.positionals.0);
            } else {
                argv.0.extend(self.positionals.0);
                argv.0.extend(self.options.0);
                if separator {
                    argv.word("--");
                }
            }
            argv.0.extend(self.last.0);
            if let Some(subcommand) = self.subcommand {
                argv.0.extend(subcommand.0);
            }
        }
    }

    /// How clap accepts one argument: what the derive needs to spell it.
    pub struct ArgMeta {
        positional: bool,
        /// `--long`, or `-s` if the argument has only a short.
        flag: String,
        delimiter: Option<char>,
        action: ArgAction,
        /// Values per occurrence, when more than one: `--pair 1 2`.
        values_per_occurrence: usize,
        require_equals: bool,
        allow_hyphen: bool,
        last: bool,
        trailing: bool,
    }

    /// The argument of `cmd` whose id is `id`, which for a derived type is
    /// the field name.
    ///
    /// # Panics
    ///
    /// If there is none, which means the field is not an argument clap
    /// knows about; `#[arg(skip)]` fields are skipped by the macro and
    /// never reach here.
    pub fn arg_meta(cmd: &Command, id: &str) -> ArgMeta {
        let arg = cmd
            .get_arguments()
            .find(|arg| arg.get_id().as_str() == id)
            .unwrap_or_else(|| {
                panic!("clap built no argument with id `{id}` for this field")
            });
        let flag = match (arg.get_long(), arg.get_short()) {
            (Some(long), _) => format!("--{long}"),
            (None, Some(short)) => format!("-{short}"),
            (None, None) => String::new(),
        };
        let values_per_occurrence =
            arg.get_num_args().map_or(1, |range| range.max_values().max(1));
        ArgMeta {
            positional: arg.is_positional(),
            flag,
            delimiter: arg.get_value_delimiter(),
            action: arg.get_action().clone(),
            values_per_occurrence,
            require_equals: arg.is_require_equals_set(),
            allow_hyphen: arg.is_allow_hyphen_values_set(),
            last: arg.is_last_set(),
            trailing: arg.is_trailing_var_arg_set(),
        }
    }

    impl ArgMeta {
        fn check_valued(&self) {
            match self.action {
                ArgAction::Set | ArgAction::Append => {}
                ref other => panic!(
                    "unsupported clap action {other:?} for a valued argument"
                ),
            }
        }

        fn positional(&self, level: &mut Level, value: String) {
            if self.last {
                level.last.value(value);
                return;
            }
            if value.starts_with('-') && !self.allow_hyphen {
                level.hyphen = true;
            }
            if self.trailing {
                level.trailing = true;
            }
            level.positionals.value(value);
        }

        /// One occurrence of an option with `values`.
        fn occurrence(&self, level: &mut Level, mut values: Vec<String>) {
            let first = values.remove(0);
            let attach = self.require_equals || first.starts_with('-');
            level.options.flag_value(&self.flag, first, attach);
            for value in values {
                level.options.value(value);
            }
        }
    }

    /// One value: a positional, or `flag value`.
    pub fn emit_one(level: &mut Level, meta: &ArgMeta, value: String) {
        if let ArgAction::Count = meta.action {
            let count: usize = value
                .parse()
                .unwrap_or_else(|_| panic!("a count renders as an integer"));
            for _ in 0..count {
                level.options.word(&meta.flag);
            }
            return;
        }
        meta.check_valued();
        if meta.positional {
            meta.positional(level, value);
        } else {
            meta.occurrence(level, vec![value]);
        }
    }

    /// One optional value, or nothing.
    pub fn emit_optional(
        level: &mut Level,
        meta: &ArgMeta,
        value: Option<String>,
    ) {
        if let Some(value) = value {
            emit_one(level, meta, value);
        }
    }

    /// Several values: joined by the argument's delimiter if it has one,
    /// grouped by occurrence if clap takes several per occurrence, else
    /// one occurrence each; nothing if there are none.
    pub fn emit_many(level: &mut Level, meta: &ArgMeta, values: Vec<String>) {
        if values.is_empty() {
            return;
        }
        meta.check_valued();
        if let Some(delimiter) = meta.delimiter {
            emit_one(level, meta, values.join(&delimiter.to_string()));
        } else if meta.positional {
            for value in values {
                meta.positional(level, value);
            }
        } else if meta.values_per_occurrence > 1 {
            for chunk in values.chunks(meta.values_per_occurrence) {
                meta.occurrence(level, chunk.to_vec());
            }
        } else {
            for value in values {
                meta.occurrence(level, vec![value]);
            }
        }
    }

    /// A flag: present when its action would set the field to the value
    /// it holds; or `flag true|false` if the flag takes its value.
    pub fn emit_bool(level: &mut Level, meta: &ArgMeta, on: bool) {
        match meta.action {
            ArgAction::SetTrue => level.options.flag(&meta.flag, on),
            ArgAction::SetFalse => level.options.flag(&meta.flag, !on),
            ArgAction::Set => level.options.flag_value(
                &meta.flag,
                on.to_string(),
                meta.require_equals,
            ),
            ref other => panic!("unsupported clap action {other:?} for a bool"),
        };
    }

    /// Chooses how a value is spelled by its type: clap's canonical name if
    /// it is a [`ValueEnum`], else its `Display`.
    ///
    /// The choice is made by method resolution. [`LeafViaValueEnum`] is
    /// implemented for `Leaf<T>` and [`LeafViaDisplay`] for `&Leaf<T>`, so
    /// `(&Leaf::<T>::new()).leaf(v)` finds the first when `T: ValueEnum`
    /// and only otherwise autorefs to the second.
    pub struct Leaf<T>(PhantomData<T>);

    impl<T> Leaf<T> {
        pub const fn new() -> Self {
            Self(PhantomData)
        }
    }

    impl<T> Default for Leaf<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    pub trait LeafViaValueEnum<T> {
        fn leaf(&self, value: &T) -> String;
    }

    impl<T: ValueEnum> LeafViaValueEnum<T> for Leaf<T> {
        fn leaf(&self, value: &T) -> String {
            super::choice_name(value)
        }
    }

    pub trait LeafViaDisplay<T> {
        fn leaf(&self, value: &T) -> String;
    }

    impl<T: fmt::Display> LeafViaDisplay<T> for &Leaf<T> {
        fn leaf(&self, value: &T) -> String {
            value.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, ValueEnum)]
    enum Color {
        Red,
        #[value(name = "grn", alias = "green")]
        Green,
    }

    #[test]
    fn words_and_line() {
        let mut argv = Argv::default();
        argv.word("paint")
            .value("fence one")
            .choice(&Color::Green)
            .option("--coats", Some(2))
            .option("--brush", None::<&str>)
            .option("--note", Some("-x"))
            .flag("--dry", true)
            .flag("--wet", false);
        assert_eq!(
            argv.words(),
            ["paint", "fence one", "grn", "--coats", "2", "--note=-x", "--dry"]
        );
        let line = argv.to_string();
        assert_eq!(line, "paint 'fence one' grn --coats 2 '--note=-x' --dry");
        assert_eq!(shell_words::split(&line).unwrap(), argv.words());
        assert_eq!(choice_name(&Color::Red), "red");
    }
}
