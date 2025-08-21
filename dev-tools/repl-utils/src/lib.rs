// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! helpers for running a REPL with reedline

use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use camino::Utf8Path;
use clap::Parser;
use reedline::Prompt;
use reedline::Reedline;
use reedline::Signal;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use subprocess::Exec;

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
    run_one: &mut dyn FnMut(C) -> anyhow::Result<Option<String>>,
) -> anyhow::Result<()> {
    let file = File::open(&input_file)
        .with_context(|| format!("open {:?}", &input_file))?;
    let bufread = BufReader::new(file);
    let mut lines = bufread.lines().peekable();
    while let Some(line_res) = lines.next() {
        let line =
            line_res.with_context(|| format!("read {:?}", &input_file))?;

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
                    .with_context(|| format!("read {:?}", &input_file))?;
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

        // Strip everything after '#' as a comment.
        let entry = match line.split_once('#') {
            Some((real, _comment)) => real,
            None => &line,
        };

        match process_entry(entry, run_one) {
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
/// - Each line is treated as a command.  It's parsed as a `C`, which is of type
///   `clap::Parser`.  In other words: you use `clap` to define the commands
///   supported in your REPL.
/// - On failure to parse each line as a `C` command, a usage message is
///   printed.
/// - '#' is a comment character.  In each line, the first '#' and any
///   subsequent characters are ignored.
/// - `run_one` is invoked for each successfully parsed command.
///   - On success with `Some(string)`, the string is printed, followed by a
///     newline.
///   - On success with `None`, nothing is printed.
///   - On failure, the error and its cause chain are printed.
///
/// Limitations:
///
/// - Arguments are currently split on whitespace, so commands, options, and
///   arguments cannot have whitespace in them.
pub fn run_repl_on_stdin<C: Parser>(
    run_one: &mut dyn FnMut(C) -> anyhow::Result<Option<String>>,
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
    run_one: &mut dyn FnMut(C) -> anyhow::Result<Option<String>>,
) -> anyhow::Result<()> {
    loop {
        match ed.read_line(prompt) {
            Ok(Signal::Success(buffer)) => {
                // Strip everything after '#' as a comment.
                let entry = match buffer.split_once('#') {
                    Some((real, _comment)) => real,
                    None => &buffer,
                };
                match process_entry(entry, run_one) {
                    LoopResult::Continue => (),
                    LoopResult::Bail(error) => return Err(error),
                }
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => break,
            Err(error) => {
                bail!("unexpected error: {:#}", error);
            }
        }
    }

    Ok(())
}

fn process_entry<C: Parser>(
    entry: &str,
    run_one: &mut dyn FnMut(C) -> anyhow::Result<Option<String>>,
) -> LoopResult {
    // If no input was provided, take another lap (print the prompt and accept
    // another line).  This gets handled specially because otherwise clap would
    // treat this as a usage error and print a help message, which isn't what we
    // want here.
    if entry.trim().is_empty() {
        return LoopResult::Continue;
    }

    // Split on the first pipe (`|`) character if it exists. Use the first
    // element of the iterator as the REPL command to parse via clap. Use the
    // second element, if it exists, as the shell command to pipe the output of
    // the REPL command into.
    let mut split = entry.splitn(2, '|');

    // Parse the line of input before any `!` as a REPL command.
    //
    // Using `split_whitespace()` like this is going to be a problem if we ever
    // want to support arguments with whitespace in them (using quotes).  But
    // it's good enough for now.
    let parts = split.next().unwrap().split_whitespace();

    let parsed_command = C::command()
        .multicall(true)
        .try_get_matches_from(parts)
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

    match run_one(command) {
        Err(error) => println!("error: {:#}", error),
        Ok(Some(repl_cmd_output)) => {
            if let Some(shell_cmd) = split.next() {
                let cmd = format!("echo '{repl_cmd_output}' | {shell_cmd}");
                Exec::shell(cmd).join().unwrap();
            } else {
                println!("{repl_cmd_output}");
            }
        }
        Ok(None) => (),
    }

    LoopResult::Continue
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
