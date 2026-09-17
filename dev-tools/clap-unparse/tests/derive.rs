// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `#[derive(Unparse)]` against a vocabulary that uses each shape it
//! supports, checked by parsing what it unparses.

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use omicron_clap_unparse::Unparse;

#[derive(Debug, Parser)]
#[command(multicall = true)]
struct Line {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, PartialEq, Subcommand, Unparse)]
enum Command {
    /// A plain tuple variant: the name, then the `Args` struct.
    Paint(PaintArgs),
    /// A nested subcommand enum.
    #[command(subcommand)]
    Fence(Verb<FenceQuery, FenceMutate>),
    /// Unit and named-field variants, with a renamed subcommand.
    #[command(name = "go")]
    Walk,
    List {
        #[arg(long, short)]
        color: Option<Color>,
        #[arg(long = "tag", value_delimiter = ',')]
        tags: Vec<String>,
        #[arg(short)]
        n: Option<i32>,
        /// A field clap does not see.
        #[arg(skip)]
        cached: bool,
    },
    /// Legacy attribute spelling, and a variant-level `rename_all`.
    #[clap(rename_all = "snake_case")]
    OldStyle {
        #[clap(long)]
        r#type: Option<String>,
        #[clap(name = "level", long)]
        lvl: Option<u8>,
    },
    /// Fields whose names collide with the generated code's.
    Shadow { argv: u32, inner: u32 },
    /// A subcommand field declared before an option, and a `Vec`
    /// positional.
    Run {
        #[command(subcommand)]
        task: Option<Task>,
        #[arg(long)]
        quiet: bool,
    },
    Exec {
        words: Vec<String>,
        #[arg(long)]
        dry: bool,
    },
    /// Positionals that need `--`, or that swallow what follows.
    Last {
        first: String,
        #[arg(last = true)]
        rest: Vec<String>,
    },
    Trailing {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        rest: Vec<String>,
        #[arg(long)]
        flag: bool,
    },
    /// Option shapes: `require_equals`, several values per occurrence, a
    /// bool that takes its value, a counted flag.
    Opts {
        #[arg(long, require_equals = true)]
        eq: Option<String>,
        #[arg(long, num_args = 2)]
        pair: Vec<u32>,
        #[arg(long, action = ArgAction::Set)]
        enabled: bool,
        #[arg(short, action = ArgAction::Count)]
        verbose: u8,
    },
}

#[derive(Debug, PartialEq, Subcommand, Unparse)]
enum Task {
    Build,
    Test { name: String },
}

/// A generic glue enum with only flattened variants.
#[derive(Debug, PartialEq, Subcommand, Unparse)]
enum Verb<Q: Subcommand, M: Subcommand> {
    #[command(flatten)]
    Query(Q),
    #[command(flatten)]
    Mutate(M),
}

#[derive(Debug, PartialEq, Subcommand, Unparse)]
#[command(rename_all = "snake_case")]
enum FenceQuery {
    /// Renders as `show_all` under `rename_all`.
    ShowAll,
    Show {
        post: u32,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, PartialEq, Subcommand, Unparse)]
enum FenceMutate {
    Add {
        post: u32,
        color: Color,
    },
    Remove {
        post: u32,
        #[arg(long)]
        force: bool,
    },
}

#[derive(Debug, PartialEq, Args, Unparse)]
struct PaintArgs {
    /// A positional that may start with `-`, or be empty.
    what: String,
    #[arg(long)]
    coats: Option<u8>,
    #[arg(long, action = ArgAction::SetFalse)]
    primer: bool,
    #[command(flatten)]
    brush: BrushArgs,
}

#[derive(Debug, PartialEq, Args, Unparse)]
struct BrushArgs {
    #[arg(long)]
    width: Option<u32>,
    #[arg(long)]
    wet: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum Color {
    Red,
    #[value(name = "grn", alias = "green")]
    Green,
}

/// An empty enum derives too.
#[derive(Debug, PartialEq, Subcommand, Unparse)]
enum Never {}

fn parse(words: &[String]) -> Command {
    Line::try_parse_from(words)
        .unwrap_or_else(|e| panic!("{words:?} fails to parse: {e}"))
        .command
}

#[track_caller]
fn round_trip(command: Command, expected: &[&str]) {
    let argv = command.to_argv();
    assert_eq!(argv.words(), expected, "{command:?}");
    assert_eq!(parse(argv.words()), command);
    assert_eq!(shell_words::split(&argv.to_string()).unwrap(), argv.words());
}

fn list(color: Option<Color>, tags: &[&str], n: Option<i32>) -> Command {
    Command::List {
        color,
        tags: tags.iter().map(|s| s.to_string()).collect(),
        n,
        cached: false,
    }
}

#[test]
fn args_structs_and_flatten() {
    round_trip(
        Command::Paint(PaintArgs {
            what: "wall".into(),
            coats: Some(2),
            primer: true,
            brush: BrushArgs { width: None, wet: Some(false) },
        }),
        &["paint", "wall", "--coats", "2", "--wet", "false"],
    );
    round_trip(
        Command::Paint(PaintArgs {
            what: String::new(),
            coats: None,
            primer: false,
            brush: BrushArgs { width: Some(3), wet: None },
        }),
        &["paint", "", "--primer", "--width", "3"],
    );
}

#[test]
fn variants() {
    round_trip(Command::Walk, &["go"]);
    round_trip(
        list(Some(Color::Green), &["a", "b"], Some(4)),
        &["list", "--color", "grn", "--tag", "a,b", "-n", "4"],
    );
    round_trip(list(None, &[], None), &["list"]);
    round_trip(
        Command::OldStyle { r#type: Some("x".into()), lvl: Some(3) },
        &["old_style", "--type", "x", "--level", "3"],
    );
    round_trip(Command::Shadow { argv: 1, inner: 2 }, &["shadow", "1", "2"]);
}

#[test]
fn nested_subcommands() {
    round_trip(
        Command::Fence(Verb::Query(FenceQuery::ShowAll)),
        &["fence", "show_all"],
    );
    round_trip(
        Command::Fence(Verb::Query(FenceQuery::Show { post: 7, json: true })),
        &["fence", "show", "7", "--json"],
    );
    round_trip(
        Command::Fence(Verb::Mutate(FenceMutate::Add {
            post: 1,
            color: Color::Red,
        })),
        &["fence", "add", "1", "red"],
    );
    round_trip(
        Command::Fence(Verb::Mutate(FenceMutate::Remove {
            post: 1,
            force: false,
        })),
        &["fence", "remove", "1"],
    );
    // A subcommand field comes last whatever its declaration order.
    round_trip(
        Command::Run {
            task: Some(Task::Test { name: "t".into() }),
            quiet: true,
        },
        &["run", "--quiet", "test", "t"],
    );
    round_trip(Command::Run { task: None, quiet: false }, &["run"]);
}

#[test]
fn positionals() {
    round_trip(
        Command::Exec { words: vec!["a".into(), "b".into()], dry: true },
        &["exec", "a", "b", "--dry"],
    );
    // A value clap would take for a flag goes after `--`, and the options
    // move before it.
    round_trip(
        Command::Paint(PaintArgs {
            what: "-wall".into(),
            coats: Some(1),
            primer: true,
            brush: BrushArgs { width: None, wet: None },
        }),
        &["paint", "--coats", "1", "--", "-wall"],
    );
    round_trip(
        Command::Last {
            first: "f".into(),
            rest: vec!["a".into(), "-b".into()],
        },
        &["last", "f", "--", "a", "-b"],
    );
    round_trip(
        Command::Trailing { rest: vec!["-b".into(), "c".into()], flag: true },
        &["trailing", "--flag", "-b", "c"],
    );
}

#[test]
fn option_shapes() {
    round_trip(list(None, &["-x"], Some(-1)), &["list", "--tag=-x", "-n-1"]);
    round_trip(
        Command::Opts {
            eq: Some("v".into()),
            pair: vec![1, 2, 3, 4],
            enabled: false,
            verbose: 2,
        },
        &[
            "opts",
            "--eq=v",
            "--pair",
            "1",
            "2",
            "--pair",
            "3",
            "4",
            "--enabled",
            "false",
            "-v",
            "-v",
        ],
    );
    round_trip(
        Command::Opts { eq: None, pair: vec![], enabled: true, verbose: 0 },
        &["opts", "--enabled", "true"],
    );
}
