// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use helios_fusion::Input;

pub(crate) trait InputExt {
    /// Shifts out the program, putting the subsequent argument in its place.
    ///
    /// Returns the prior program value.
    fn shift_program(&mut self) -> Result<String, String>;
}

impl InputExt for Input {
    fn shift_program(&mut self) -> Result<String, String> {
        if self.args.is_empty() {
            return Err(format!("Failed to parse {self}"));
        }
        let new = self.args.remove(0);
        let old = std::mem::replace(&mut self.program, new);
        Ok(old)
    }
}

pub(crate) struct InputParser {
    input: Input,
    start: usize,
    end: usize,
}

impl InputParser {
    pub(crate) fn new(input: Input) -> Self {
        let end = input.args.len();
        Self { input, start: 0, end }
    }

    pub(crate) fn input(&self) -> &Input {
        &self.input
    }

    pub(crate) fn args(&self) -> &[String] {
        &self.input.args[self.start..self.end]
    }

    pub(crate) fn no_args_remaining(&self) -> Result<(), String> {
        if self.start < self.end {
            return Err(format!(
                "Unexpected extra arguments: {:?}",
                self.args()
            ));
        }
        Ok(())
    }

    /// Reemoves the last argument unconditionally.
    pub(crate) fn shift_last_arg(&mut self) -> Result<String, String> {
        if self.start >= self.end {
            return Err("Missing argument".to_string());
        }
        let arg = self
            .input
            .args
            .get(self.end - 1)
            .ok_or_else(|| "Missing argument")?;
        self.end -= 1;
        Ok(arg.to_string())
    }

    /// Removes the next argument unconditionally.
    pub(crate) fn shift_arg(&mut self) -> Result<String, String> {
        if self.start >= self.end {
            return Err("Missing argument".to_string());
        }
        let arg = self
            .input
            .args
            .get(self.start)
            .ok_or_else(|| "Missing argument")?;
        self.start += 1;
        Ok(arg.to_string())
    }

    /// Removes the next argument, which must equal the provided value.
    pub(crate) fn shift_arg_expect(
        &mut self,
        value: &str,
    ) -> Result<(), String> {
        let v = self.shift_arg()?;
        if value != v {
            return Err(format!("Unexpected argument {v} (expected: {value}"));
        }
        Ok(())
    }

    /// Removes the next argument if it equals `value`.
    ///
    /// Returns if it was equal.
    pub(crate) fn shift_arg_if(&mut self, value: &str) -> Result<bool, String> {
        let eq = self
            .input
            .args
            .get(self.start)
            .ok_or_else(|| "Missing argument")?
            == value;
        if eq {
            self.shift_arg()?;
        }
        Ok(eq)
    }
}
