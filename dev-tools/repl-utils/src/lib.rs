// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! helpers for running a REPL with reedline

use anyhow::Context;
use camino::Utf8Path;
use clap::Parser;
use reedline::Prompt;
use reedline::Reedline;
use reedline::Signal;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::str::CharIndices;
use subprocess::Exec;

/// Adapts a callback that takes a parsed `C` into a line handler.
///
/// The returned handler splits the line with shell-style quoting, parses the
/// words as a `C` (a `clap::Parser`, used in multicall mode so the first word
/// is a subcommand rather than a program name), and calls the given `run_one`
/// callback with the result.
pub fn parsed<C: Parser>(
    mut run_one: impl FnMut(C) -> anyhow::Result<Option<String>>,
) -> impl FnMut(&str) -> anyhow::Result<String> {
    move |text: &str| {
        let words = shell_words::split(text)?;

        C::command()
            .multicall(true)
            .try_get_matches_from(words)
            .and_then(|matches| C::from_arg_matches(&matches))
            .map_or_else(
                // Print the usage message the way clap wants it printed (help
                // goes to stdout, errors to stderr). If even that fails,
                // report it like any other error on the line.
                |error| Ok(error.print().map(|()| String::new())?),
                |command| run_one(command).map(Option::unwrap_or_default),
            )
    }
}

/// Runs the same kind of REPL as `run_repl_on_stdin()`, but reads commands from
/// a file
///
/// Commands are printed to stdout before being executed.
///
/// This is useful for expectorate tests.  You can define a file of input
/// commands and check it against known output.  The output has the commands in
/// it so that it's easier to verify.
pub fn run_repl_from_file(
    input_file: &Utf8Path,
    mut run_line: impl FnMut(&str) -> anyhow::Result<String>,
) -> anyhow::Result<()> {
    let file = File::open(&input_file)
        .with_context(|| format!("open {:?}", input_file))?;
    let bufread = BufReader::new(file);
    let mut lines = bufread.lines().peekable();
    while let Some(line_res) = lines.next() {
        let line =
            line_res.with_context(|| format!("read {:?}", input_file))?;

        // Extract and handle multi-line comments.
        if line.starts_with('#') {
            println!("> {}", line);
            loop {
                let next = lines.next_if(|l| {
                    match l {
                        Ok(line) => line.starts_with('#'),
                        Err(_) => {
                            // If an error occurs, bail out immediately.
                            true
                        }
                    }
                });
                let Some(next_res) = next else {
                    // Next line is not part of a multi-line comment (or is an
                    // EOF), so exit the loop.
                    break;
                };
                let next_line = next_res
                    .with_context(|| format!("read {:?}", input_file))?;
                println!("> {}", next_line);
            }

            continue;
        }

        if line.is_empty() {
            // We print empty lines as-is, relying on the println! at the end of
            // the loop.
        } else {
            // Print the command with a prompt sign.
            println!("> {}", line);
        }

        process_entry(&line, &mut run_line);
        println!();
    }
    Ok(())
}

/// Runs a REPL using stdin/stdout
///
/// Behavior:
///
/// - Each input line is scanned for a comment and a pipe, following a few
///   rules:
///   - We use shell-style quoting (single quotes, double quotes, backslash
///     escapes), so a quoted `#` or `!` is an ordinary character.
///   - An unquoted `#` character marks the beginning of a comment, which is
///     ignored.
///   - An unquoted `!` character is a pipe, indicating that the output of the
///     command (described below) should be postprocessed by a given shell
///     command. With no command before it, the shell command runs with no
///     input.
///
/// - The remaining text is handed to `run_line`, which has been stripped of
///   surrounding whitespace and of any trailing comment or shell pipe. The
///   handler parses it itself. Use [`parsed`] to have it split into words and
///   parsed as a `clap::Parser` first.
///
/// - `run_line` is invoked for each non-empty line.
///   - On success, the output is printed, followed by a newline, unless it is
///     empty, in which case nothing is printed.
///   - On failure, the error and its cause chain are printed.
pub fn run_repl_on_stdin(
    run_line: impl FnMut(&str) -> anyhow::Result<String>,
) -> anyhow::Result<()> {
    let ed = Reedline::create();
    let prompt = reedline::DefaultPrompt::new(
        reedline::DefaultPromptSegment::Empty,
        reedline::DefaultPromptSegment::Empty,
    );
    run_repl_on_stdin_customized(ed, &prompt, run_line)
}

/// Runs a REPL using stdin/stdout with a customized `Reedline` and `Prompt`
///
/// See docs for [`run_repl_on_stdin`]
pub fn run_repl_on_stdin_customized(
    mut ed: Reedline,
    prompt: &dyn Prompt,
    mut run_line: impl FnMut(&str) -> anyhow::Result<String>,
) -> anyhow::Result<()> {
    // Ensure new Signal variants would trigger a compile error.
    #[allow(clippy::while_let_loop)]
    loop {
        match ed.read_line(prompt).context("unexpected error")? {
            Signal::Success(buffer) => process_entry(&buffer, &mut run_line),
            Signal::CtrlD | Signal::CtrlC => break,
        }
    }

    Ok(())
}

fn process_entry(
    entry: &str,
    run_line: &mut dyn FnMut(&str) -> anyhow::Result<String>,
) {
    let (text, shell_cmd) = split_markers(entry);

    // If no input was provided, take another lap (print the prompt and accept
    // another line). If there was no command but a pipe was present, run the
    // shell command with no input.
    if text.is_empty() {
        if let Some(shell_cmd) = shell_cmd {
            pipe_into_shell(shell_cmd, "");
        }
        return;
    }

    match (run_line(text), shell_cmd) {
        (Err(error), _) => println!("error: {:#}", error),
        // The shell command runs whenever the command succeeds, with whatever
        // output it produced, possibly none.
        (Ok(output), Some(shell_cmd)) => pipe_into_shell(shell_cmd, &output),
        (Ok(output), None) => {
            if !output.is_empty() {
                println!("{output}");
            }
        }
    }
}

/// Runs `shell_cmd` with `input` on its stdin, and waits for it to exit.
fn pipe_into_shell(shell_cmd: &str, input: &str) {
    let mut child_stdin =
        Exec::shell(shell_cmd).stream_stdin().expect("stdin opened");

    // Using `write_all` doesn't play nicely with the shell group leader,
    // likely due to blocking/signal behavior. We therefore manually loop over
    // calls to `write`.
    let mut written_bytes = 0;
    let to_write = input.len();
    while written_bytes < to_write {
        match child_stdin.write(&input.as_bytes()[written_bytes..]) {
            Ok(0) => break,
            Ok(n) => written_bytes += n,
            Err(_) => {
                // Broken pipe is a normal condition reflecting that the child
                // process exited early (e.g., as `head(1)` does).
                break;
            }
        }
    }
}

/// Splits a line at its first unquoted comment or pipe marker, if any.
///
/// When examining a raw line of input, we need to know where the command ends
/// and either a comment (indicated by '#') or a pipe to a shell command
/// (indicated by '!') begins. This is slightly delicate because these markers
/// can be quoted or escaped, so we need to be aware of these quoting rules
/// (see the `skip_*` helper functions below).
///
/// Returns the command text, and if there is a pipe marker, also returns the
/// shell command to pipe into.
fn split_markers(line: &str) -> (&str, Option<&str>) {
    let mut chars = line.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => skip_escaped(&mut chars),
            '\'' => skip_single_quoted(&mut chars),
            '"' => skip_double_quoted(&mut chars),
            // An unprotected `#` starts a comment: the command ends here.
            '#' => return (line[..i].trim(), None),
            // An unprotected `!` starts a shell command, which gets the rest
            // of the line.
            '!' => return (line[..i].trim(), Some(chars.as_str())),
            _ => {}
        }
    }
    (line.trim(), None)
}

/// Consumes the character immediately following a backslash, or to the end if
/// there is none.
fn skip_escaped(chars: &mut CharIndices<'_>) {
    chars.next();
}

/// Consumes through the single quote that closes a segment opened before
/// `chars`, or to the end if there is none.
fn skip_single_quoted(chars: &mut CharIndices<'_>) {
    chars.find(|&(_, c)| c == '\'');
}

/// Consumes through the double quote that closes a segment opened before
/// `chars`, or to the end if there is none. Within double quotes, a backslash
/// protects the character after it, so `\"` does not close the segment.
fn skip_double_quoted(chars: &mut CharIndices<'_>) {
    while let Some((_, c)) = chars.next() {
        match c {
            '"' => return,
            '\\' => skip_escaped(chars),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test clap command that just contains a list of arguments. This lets us
    /// test that `parsed`'s splitting (using shell_words::split) and
    /// `process_entry`'s handling of comments and pipes are doing what we
    /// expect.
    #[derive(Debug, Parser)]
    enum Command {
        Noop { args: Vec<String> },
    }

    /// Sends a line of input through `process_entry` with a handler that
    /// records the stripped text it was given. Returns `None` if the handler
    /// was not called.
    fn run_line(entry: &str) -> Option<String> {
        let mut captured = None;
        process_entry(entry, &mut |text: &str| {
            captured = Some(text.to_string());
            Ok(String::new())
        });
        captured
    }

    /// Sends a line of input through `process_entry` and the `parsed`
    /// adapter, and returns the parsed command's args.
    ///
    /// Note that `parsed` prints split and parse errors and continues, so we
    /// can't differentiate between an error and an empty result; we return
    /// `None` either way.
    fn run(entry: &str) -> Option<Vec<String>> {
        let mut captured = None;
        process_entry(
            entry,
            &mut parsed(|cmd: Command| {
                let Command::Noop { args } = cmd;
                captured = Some(args);
                Ok(None)
            }),
        );
        captured
    }

    fn run_ok(entry: &str) -> Vec<String> {
        run(entry).expect("command should parse")
    }

    fn run_err(entry: &str) {
        assert!(run(entry).is_none());
    }

    #[test]
    fn quoted_arguments_are_single_words() {
        assert_eq!(
            run_ok(r#"noop a 'b c' "d e" f\ g"#),
            ["a", "b c", "d e", "f g"]
        );
    }

    #[test]
    fn line_handler_receives_stripped_text() {
        assert_eq!(run_line("  noop a 'b c'  ").unwrap(), "noop a 'b c'");
        assert_eq!(run_line("noop a # comment").unwrap(), "noop a");
        assert_eq!(run_line("noop a ! true").unwrap(), "noop a");
        // The handler is not called for a blank line.
        assert_eq!(run_line("   "), None);
    }

    #[test]
    fn empty_words_and_trailing_backslashes_are_preserved() {
        assert_eq!(run_ok("noop ''"), [""]);
        assert_eq!(run_ok(r"noop a\"), [r"a\"]);
    }

    #[test]
    fn quoted_markers_are_ordinary_characters() {
        assert_eq!(
            run_ok(r#"noop 'a!b' "c#d" e\!f # 'comment"#),
            ["a!b", "c#d", "e!f"]
        );
        assert_eq!(
            run_line(r#"noop 'a!b' "c#d" e\!f # 'comment"#).unwrap(),
            r#"noop 'a!b' "c#d" e\!f"#
        );
    }

    #[test]
    fn split_markers_unquoted_bang_passes_the_rest_through_verbatim() {
        assert_eq!(split_markers("a ! b ! c # d"), ("a", Some(" b ! c # d")));
    }

    #[test]
    fn split_markers_first_marker_wins() {
        assert_eq!(split_markers("show # ! not a pipe"), ("show", None));
    }

    #[test]
    fn split_markers_double_quotes_protect_markers_and_escaped_quotes() {
        assert_eq!(
            split_markers(r##"emit "a\"#b!c" # note"##),
            (r##"emit "a\"#b!c""##, None)
        );
    }

    #[test]
    fn split_markers_escaped_backslash_does_not_escape_a_closing_double_quote()
    {
        assert_eq!(
            split_markers(r#"emit "a\\" # note"#),
            (r#"emit "a\\""#, None)
        );
    }

    #[test]
    fn split_markers_backslash_is_literal_in_single_quotes() {
        assert_eq!(split_markers(r"emit 'a\' # note"), (r"emit 'a\'", None));
    }

    #[test]
    fn bare_pipe_runs_shell_command_with_no_input() {
        let dir = camino_tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        // The handler is never invoked, but the shell command runs.
        assert_eq!(run_line(&format!("! cat > {path}")), None);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
    }

    #[test]
    fn pipe_into_shell_writes_the_input() {
        let dir = camino_tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        pipe_into_shell(&format!("cat > {path}"), "hello\nworld\n");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello\nworld\n");
    }

    /// The handler's output, when there is some, is what the pipe receives.
    #[test]
    fn pipe_receives_the_handler_output() {
        let dir = camino_tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        process_entry(&format!("noop ! cat > {path}"), &mut |_: &str| {
            Ok("hello".to_string())
        });
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello");
    }

    /// A handler that fails has no output to pipe, so the shell command does
    /// not run at all.
    #[test]
    fn a_failing_handler_does_not_run_its_pipe() {
        let dir = camino_tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        process_entry(&format!("noop ! cat > {path}"), &mut |_: &str| {
            Err(anyhow::anyhow!("nope"))
        });
        // `cat > path` would have created the file even with no input.
        assert!(!path.exists());
    }

    /// A child that exits without reading closes the pipe; writing to it
    /// must return rather than panic or hang.
    #[test]
    fn pipe_into_shell_tolerates_a_child_that_exits_early() {
        pipe_into_shell("true", &"x".repeat(1 << 20));
    }

    /// `parsed` does not call the callback on a split error or a parse error.
    #[test]
    fn parsed_skips_the_callback_on_split_and_parse_errors() {
        run_err("bogus"); // fails to parse as a command
        run_err("noop 'a b"); // fails to split due to unclosed quote
    }

    /// An unclosed quote is an error the handler returns, not one it prints.
    #[test]
    fn an_unclosed_quote_is_the_handlers_error() {
        let mut handler = parsed(|cmd: Command| {
            let Command::Noop { .. } = cmd;
            Ok(None)
        });
        assert!(handler("noop 'a b").is_err());
    }

    /// A line that does not parse still runs its pipe, on empty input.
    #[test]
    fn a_line_that_does_not_parse_still_runs_its_pipe() {
        let dir = camino_tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        process_entry(
            &format!("bogus ! cat > {path}"),
            &mut parsed(|cmd: Command| {
                let Command::Noop { .. } = cmd;
                Ok(None)
            }),
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
    }
}
