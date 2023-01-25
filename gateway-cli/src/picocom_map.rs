// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! picocom-style character remapping; does not support the "... to hex" rules.

use std::{collections::VecDeque, str::FromStr};

use anyhow::{bail, ensure, Error, Result};

#[derive(Debug, Default, Clone, Copy)]
pub struct RemapRules {
    cr: Option<&'static [u8]>,
    lf: Option<&'static [u8]>,
    bsdel: bool,
    delbs: bool,
}

impl FromStr for RemapRules {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut rules = Self::default();

        for rule in s.split(',') {
            match rule {
                "crlf" => {
                    ensure!(rules.cr.is_none(), "multiple rules remapping cr");
                    rules.cr = Some(&[raw::LF]);
                }
                "crcrlf" => {
                    ensure!(rules.cr.is_none(), "multiple rules remapping cr");
                    rules.cr = Some(&[raw::CR, raw::LF]);
                }
                "igncr" => {
                    ensure!(rules.cr.is_none(), "multiple rules remapping cr");
                    rules.cr = Some(&[]);
                }
                "lfcr" => {
                    ensure!(rules.lf.is_none(), "multiple rules remapping lf");
                    rules.lf = Some(&[raw::CR]);
                }
                "lfcrlf" => {
                    ensure!(rules.lf.is_none(), "multiple rules remapping lf");
                    rules.lf = Some(&[raw::CR, raw::LF]);
                }
                "ignlf" => {
                    ensure!(rules.lf.is_none(), "multiple rules remapping lf");
                    rules.lf = Some(&[]);
                }
                "bsdel" => {
                    rules.bsdel = true;
                }
                "delbs" => {
                    rules.delbs = true;
                }
                _ => bail!("unknown or unsupported remap rule: {rule:?}"),
            }
        }

        Ok(rules)
    }
}

impl RemapRules {
    pub fn apply<I>(&self, bytes: I) -> RemapIter<I::IntoIter>
    where
        I: IntoIterator<Item = u8>,
    {
        RemapIter {
            inner: bytes.into_iter(),
            prev: VecDeque::new(),
            rules: *self,
        }
    }
}

pub struct RemapIter<I> {
    inner: I,
    // Some rules (e.g., `crcrlf`) map one input byte to two output bytes; we
    // can only return the first from one invocation to `next()`, so we stash
    // any remaining output bytes from a previous input byte into this deque.
    prev: VecDeque<u8>,
    rules: RemapRules,
}

impl<I> Iterator for RemapIter<I>
where
    I: Iterator<Item = u8>,
{
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Do we have any leftover bytes from a previous input byte
            // (possibly from a previous iteration of this loop)?
            if let Some(b) = self.prev.pop_front() {
                return Some(b);
            }

            match self.inner.next()? {
                raw::CR => {
                    if let Some(repl) = self.rules.cr {
                        // This does the right thing for all three possible
                        // cases of replacement length:
                        //
                        // 0. `repl.len() == 0`: we loop back, find nothing in
                        //    `prev`, and read the next byte from `self.inner`.
                        // 1. `repl.len() == 1`: we loop back, pop the one and
                        //    only byte out, and return it. On the next call to
                        //    `self.next()`, we have no bytes left in
                        //    `self.prev` and so read the next from
                        //    `self.inner`.
                        // 2. `repl.len() > 1`: we loop back, pop the first
                        //    replacement byte out and return it, leaving the
                        //    remaining replacement bytes in `self.prev` waiting
                        //    for future call(s) to `self.next().
                        self.prev.extend(repl);
                        continue;
                    } else {
                        return Some(raw::CR);
                    }
                }
                raw::LF => {
                    if let Some(repl) = self.rules.lf {
                        self.prev.extend(repl);
                        continue;
                    } else {
                        return Some(raw::LF);
                    }
                }
                raw::BS if self.rules.bsdel => return Some(raw::DEL),
                raw::DEL if self.rules.delbs => return Some(raw::BS),
                b => return Some(b),
            }
        }
    }
}

mod raw {
    pub(super) const CR: u8 = b'\r';
    pub(super) const LF: u8 = b'\n';
    pub(super) const BS: u8 = 0x08;
    pub(super) const DEL: u8 = 0x7f;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_invalid_rules() {
        // bogus rules
        for s in ["foo", "crlf,foo"] {
            assert!(
                RemapRules::from_str(s)
                    .unwrap_err()
                    .to_string()
                    .contains("unsupported remap rule"),
                "unexpected error message parsing {s:?}"
            );
        }

        // rule strings with duplicates for CR or LF
        for s in [
            "crlf,crlf",
            "crcrlf,crlf",
            "igncr,crlf",
            "lfcr,lfcr",
            "lfcr,lfcrlf",
            "lfcr,ignlf",
        ] {
            assert!(
                RemapRules::from_str(s)
                    .unwrap_err()
                    .to_string()
                    .contains("multiple rules remapping"),
                "unexpected error message parsing {s:?}"
            );
        }
    }

    #[test]
    fn remapping() {
        for (rules, input, expected) in [
            ("crlf", "foo\rbar\n", "foo\nbar\n"),
            ("crcrlf", "foo\rbar\rbaz\n", "foo\r\nbar\r\nbaz\n"),
            ("crlf,lfcr", "foo\rbar\n", "foo\nbar\r"),
            ("igncr,lfcr", "foo\rbar\n", "foobar\r"),
            ("ignlf,crcrlf", "foo\rbar\n", "foo\r\nbar"),
            ("crlf,delbs", "foo\rbar\x7fbaz", "foo\nbar\x08baz"),
            ("igncr,delbs,bsdel", "foo\rbar\x7fbaz\x08", "foobar\x08baz\x7f"),
        ] {
            let rules: RemapRules = rules.parse().unwrap();
            let output = rules
                .apply(input.as_bytes().iter().copied())
                .collect::<Vec<_>>();
            let output = std::str::from_utf8(&output).unwrap();
            assert_eq!(expected, output, "mismatch with rules {rules:?}");
        }
    }
}
