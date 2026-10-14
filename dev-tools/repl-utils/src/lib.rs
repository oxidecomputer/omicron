// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! helpers for running a REPL with reedline

use anyhow::Context;
use anyhow::anyhow;
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

/// The callback that runs one parsed command.
///
/// The callback receives the parsed command, along with the raw input it was
/// parsed from (excluding a trailing comment or pipe).
pub type RunOne<'a, C> =
    dyn FnMut(C, &str) -> anyhow::Result<Option<String>> + 'a;

/// Runs the same kind of REPL as `run_repl_on_stdin()`, but reads commands from
/// a file
///
/// Commands are printed to stdout before being executed.
///
/// This is useful for expectorate tests.  You can define a file of input
/// commands and check it against known output.  The output has the commands in
/// it so that it's easier to verify.
pub fn run_repl_from_file<C: Parser>(
    input_file: &Utf8Path,
    run_one: &mut RunOne<'_, C>,
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

        match process_entry(&line, run_one) {
            LoopResult::Continue => (),
            LoopResult::Bail(error) => return Err(error),
        }
        println!();
    }
    Ok(())
}

/// Runs a REPL using stdin/stdout
///
/// Behavior:
///
/// - Each input line is tokenized into an array of arguments, following a few
///   rules:
///   - We use shell-style quoting (single quotes, double quotes, backslash
///     escapes), so arguments can include spaces and special characters.
///   - An unquoted `#` character marks the beginning of a comment, which will
///     be ignored in the tokenized output.
///   - An unquoted `!` character is a pipe, indicating that the output of the
///     parsed command (described below) should be postprocessed by a given
///     shell command. With no command before it, the shell command runs with no
///     input.
///
/// - The tokenized arguments are treated as a REPL command.  They are parsed as
///   a `C`, which implements `clap::Parser`. In other words: you use `clap` to
///   define the commands supported in your REPL.
/// - On failure to parse each line as a `C` command, a usage message is
///   printed.
///
/// - `run_one` is invoked for each successfully parsed command.
///   - On success with `Some(string)`, the string is printed, followed by a
///     newline.
///   - On success with `None`, nothing is printed.
///   - On failure, the error and its cause chain are printed.
pub fn run_repl_on_stdin<C: Parser>(
    run_one: &mut RunOne<'_, C>,
) -> anyhow::Result<()> {
    let ed = Reedline::create();
    let prompt = reedline::DefaultPrompt::new(
        reedline::DefaultPromptSegment::Empty,
        reedline::DefaultPromptSegment::Empty,
    );
    run_repl_on_stdin_customized(ed, &prompt, run_one)
}

/// Runs a REPL using stdin/stdout with a customized `Reedline` and `Prompt`
///
/// See docs for [`run_repl_on_stdin`]
pub fn run_repl_on_stdin_customized<C: Parser>(
    mut ed: Reedline,
    prompt: &dyn Prompt,
    run_one: &mut RunOne<'_, C>,
) -> anyhow::Result<()> {
    // Ensure new Signal variants would trigger a compile error.
    #[allow(clippy::while_let_loop)]
    loop {
        match ed.read_line(prompt).context("unexpected error")? {
            Signal::Success(buffer) => match process_entry(&buffer, run_one) {
                LoopResult::Continue => (),
                LoopResult::Bail(error) => return Err(error),
            },
            Signal::CtrlD | Signal::CtrlC => break,
        }
    }

    Ok(())
}

fn process_entry<C: Parser>(
    entry: &str,
    run_one: &mut RunOne<'_, C>,
) -> LoopResult {
    let (text, shell_cmd) = split_markers(entry);

    // If no input was provided, take another lap (print the prompt and accept
    // another line).  This gets handled specially because otherwise clap would
    // treat this as a usage error and print a help message, which isn't what we
    // want here.  If there was no clap command but a pipe was present, run the
    // shell command with no input.
    if text.is_empty() {
        if let Some(shell_cmd) = shell_cmd {
            pipe_into_shell(shell_cmd, "");
        }
        return LoopResult::Continue;
    }

    let words = match shell_words::split(text) {
        Ok(words) => words,
        Err(error) => {
            println!("error: {error}");
            return LoopResult::Continue;
        }
    };

    let parsed_command = C::command()
        .multicall(true)
        .try_get_matches_from(words)
        .and_then(|matches| C::from_arg_matches(&matches));
    let command = match parsed_command {
        Err(error) => {
            // We failed to parse the command.  Print the error.
            return match error.print() {
                // Assuming that worked, just take another lap.
                Ok(_) => LoopResult::Continue,
                // If we failed to even print the error, that itself is a fatal
                // error.
                Err(error) => LoopResult::Bail(
                    anyhow!(error).context("printing previous error"),
                ),
            };
        }
        Ok(cmd) => cmd,
    };

    match (run_one(command, text), shell_cmd) {
        (Err(error), _) => println!("error: {:#}", error),
        // The shell command runs whenever the command succeeds, with whatever
        // output it produced, possibly none.
        (Ok(output), Some(shell_cmd)) => {
            pipe_into_shell(shell_cmd, output.as_deref().unwrap_or(""))
        }
        (Ok(Some(output)), None) => println!("{output}"),
        (Ok(None), None) => (),
    }

    LoopResult::Continue
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

/// Describes next steps after evaluating one "line" of user input
///
/// This could just be `Result`, but it's easy to misuse that here because
/// _commands_ might fail all the time without needing to bail out of the REPL.
/// We use a separate type for clarity about what success/failure actually
/// means.
enum LoopResult {
    /// Show the prompt and accept another command
    Continue,

    /// Exit the REPL with a fatal error
    Bail(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test clap command that just contains a list of arguments. This lets us
    /// test that our tokenization (using shell_words::split), and our handling
    /// of comments and pipes, are doing what we expect.
    #[derive(Debug, Parser)]
    enum Command {
        Noop { args: Vec<String> },
    }

    struct RunResult {
        args: Vec<String>,
        stripped: String,
    }

    /// Sends a line of input through `process_entry`, and returns the parsed
    /// command args and the stripped input text.
    ///
    /// Note that `process_entry` swallows parse errors and returns
    /// `LoopResult::Continue` on failure, so we can't differentiate between an
    /// error and an empty result; we return `None` either way.
    fn run(entry: &str) -> Option<RunResult> {
        let mut captured = None;
        let result = process_entry(entry, &mut |cmd: Command, text: &str| {
            let Command::Noop { args } = cmd;
            captured = Some(RunResult { args, stripped: text.to_string() });
            Ok(None)
        });
        assert!(matches!(result, LoopResult::Continue));
        captured
    }

    fn run_ok(entry: &str) -> RunResult {
        run(entry).expect("command should parse")
    }

    fn run_err(entry: &str) {
        assert!(run(entry).is_none());
    }

    #[test]
    fn quoted_arguments_are_single_words() {
        assert_eq!(
            run_ok(r#"noop a 'b c' "d e" f\ g"#).args,
            ["a", "b c", "d e", "f g"]
        );
    }

    #[test]
    fn callback_receives_command_text() {
        assert_eq!(run_ok("  noop a 'b c'  ").stripped, "noop a 'b c'");
        assert_eq!(run_ok("noop a # comment").stripped, "noop a");
        assert_eq!(run_ok("noop a ! true").stripped, "noop a");
    }

    #[test]
    fn empty_words_and_trailing_backslashes_are_preserved() {
        assert_eq!(run_ok("noop ''").args, [""]);
        assert_eq!(run_ok(r"noop a\").args, [r"a\"]);
    }

    #[test]
    fn quoted_markers_are_ordinary_characters() {
        let result = run_ok(r#"noop 'a!b' "c#d" e\!f # 'comment"#);
        assert_eq!(result.args, ["a!b", "c#d", "e!f"]);
        assert_eq!(result.stripped, r#"noop 'a!b' "c#d" e\!f"#);
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
        // The callback is never invoked, but the shell command runs.
        run_err(&format!("! cat > {path}"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
    }

    #[test]
    fn pipe_into_shell_writes_the_input() {
        let dir = camino_tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        pipe_into_shell(&format!("cat > {path}"), "hello\nworld\n");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello\nworld\n");
    }

    /// A child that exits without reading closes the pipe; writing to it
    /// must return rather than panic or hang.
    #[test]
    fn pipe_into_shell_tolerates_a_child_that_exits_early() {
        pipe_into_shell("true", &"x".repeat(1 << 20));
    }

    #[test]
    fn lines_that_skip_the_callback() {
        run_err("   "); // blank line
        run_err("bogus"); // fails to parse as a command
        run_err("noop 'a b"); // fails to parse due to unclosed quote
    }
}
