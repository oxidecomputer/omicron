// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This module is an adaptation of textwrap's `Word` library to work with
//! `tui`'s `Span`s and lines.
//!
//! The code is mostly copy-pasted, with the following changes:
//!
//! * [`textwrap::core::Word`] is now [`StyledWord`].
//! * Hyphenation is no longer supported.
//!
//! Currently, each element of `tui`'s [`Text::lines`] is assumed to be a
//! separate line. We don't check for whether a line's content has embedded
//! newlines in it, but we could in the future if necessary. (Embedded newlines
//! won't break the output, but they might make the output look a bit weird.)

use itertools::{Itertools, Position};
use ratatui::text::{Line, Span, Text};
use textwrap::{
    core::{display_width, Fragment},
    wrap_algorithms::{wrap_optimal_fit, Penalties},
};

pub struct Options<'a> {
    /// The width in columns at which the text will be wrapped.
    pub width: usize,
    /// Indentation used for the first line of output.
    pub initial_indent: Span<'a>,
    /// Indentation used for subsequent lines of output.
    pub subsequent_indent: Span<'a>,
    /// Allow long words to be broken if they cannot fit on a line.
    /// When set to `false`, some lines may be longer than
    /// `self.width`.
    pub break_words: bool,
}

/// Wraps a [`Text`] block.
///
/// `text` should be broken up into lines at the time it's passed in.
pub(crate) fn wrap_text<'a>(
    text: &'a Text<'_>,
    options: Options<'a>,
) -> Text<'a> {
    let mut lines = Vec::new();
    // We currently assume that lines in text don't have embedded newlines in
    // them. This assumption might need to be revisited.
    for line in &text.lines {
        wrap_single_line(line, &options, &mut lines);
    }

    Text::from(lines)
}

/// Wraps a [`Line`] representing a single line.
///
/// If the text contains multiple lines, use [`wrap_text`] instead.
pub(crate) fn wrap_line<'a>(
    line: &'a Line<'_>,
    options: Options<'a>,
) -> Text<'a> {
    let mut lines = Vec::new();
    wrap_single_line(line, &options, &mut lines);
    Text::from(lines)
}

fn wrap_single_line<'a>(
    line: &'a Line<'_>,
    options: &Options<'a>,
    lines: &mut Vec<Line<'a>>,
) {
    let indent = if lines.is_empty() {
        options.initial_indent.clone()
    } else {
        options.subsequent_indent.clone()
    };
    if line.width() < options.width && indent.content.is_empty() {
        lines.push(borrow_line(line));
    } else {
        wrap_single_line_slow_path(line, options, lines)
    }
}

fn borrow_line<'a>(line: &'a Line<'_>) -> Line<'a> {
    let spans = line
        .spans
        .iter()
        .map(|span| Span::styled(span.content.as_ref(), span.style))
        .collect::<Vec<_>>();
    Line::from(spans)
}

fn wrap_single_line_slow_path<'a>(
    line: &'a Line<'_>,
    options: &Options<'a>,
    lines: &mut Vec<Line<'a>>,
) {
    // Span::width (options.initial_indent.width() etc) use the Unicode display
    // width, which is what we expect.
    let initial_width =
        options.width.saturating_sub(options.initial_indent.width());
    let subsequent_width =
        options.width.saturating_sub(options.subsequent_indent.width());
    let line_widths = [initial_width, subsequent_width];

    let split_words = find_words_in_line(&line);

    // We don't perform any word splitting.
    let broken_words = if options.break_words {
        let mut broken_words = break_words(split_words, line_widths[1]);
        if !options.initial_indent.content.is_empty() {
            // Without this, the first word will always go into the
            // first line. However, since we break words based on the
            // _second_ line width, it can be wrong to unconditionally
            // put the first word onto the first line. An empty
            // zero-width word fixed this.
            broken_words.insert(0, StyledWord::empty());
        }
        broken_words
    } else {
        split_words.collect::<Vec<_>>()
    };

    let f64_line_widths =
        line_widths.iter().map(|w| *w as f64).collect::<Vec<_>>();

    // The optimal fit wrap looks nicer, and we're wrapping pretty small amounts
    // of text so performance is unlikely to be an issue.
    let wrapped_lines =
        wrap_optimal_fit(&broken_words, &f64_line_widths, &Penalties::new())
            .expect("computation cannot overflow with restricted line widths");

    for words in wrapped_lines {
        let mut output_line = Vec::new();

        if lines.is_empty() && !options.initial_indent.content.is_empty() {
            output_line.push(options.initial_indent.clone());
        } else if !lines.is_empty()
            && !options.subsequent_indent.content.is_empty()
        {
            output_line.push(options.subsequent_indent.clone());
        }

        for (position, word) in words.into_iter().with_position() {
            match position {
                Position::First | Position::Middle => {
                    output_line.extend(word.word_span());
                    output_line.extend(word.whitespace_span());
                }
                Position::Last | Position::Only => {
                    // Don't add trailing whitespace, just the content.
                    output_line.extend(word.word_span());
                    // We don't support hyphenation at the moment, but if we
                    // did, this is where they would go.
                }
            }
        }

        lines.push(Line::from(output_line));
    }
}

fn find_words_in_line<'a>(
    line: &'a Line<'_>,
) -> impl Iterator<Item = StyledWord<'a>> {
    line.spans.iter().flat_map(|span| find_words_in_span(span))
}

/// Breaks this span into smaller words.
///
/// This assumes the only word breaks are ASCII spaces. In particular, it
/// assume that there are no newlines anywhere within a span.
fn find_words_in_span<'a>(
    span: &'a Span<'_>,
) -> impl Iterator<Item = StyledWord<'a>> {
    let mut start = 0;
    let mut in_whitespace = false;
    let mut char_indices = span.content.char_indices();

    std::iter::from_fn(move || {
        for (idx, ch) in char_indices.by_ref() {
            if in_whitespace && ch != ' ' {
                let word = StyledWord::new_sub_span(span, start, idx);
                start = idx;
                in_whitespace = ch == ' ';
                return Some(word);
            }

            in_whitespace = ch == ' ';
        }

        let content_len = span.content.len();
        if start < content_len {
            let word = StyledWord::new_sub_span(span, start, content_len);
            start = content_len;
            return Some(word);
        }

        None
    })
}

/// A word with a style associated with it.
///
/// This is similar to a [`textwrap::core::Word`], except each word also has a
/// style associated with it.
#[derive(Copy, Clone, Debug)]
struct StyledWord<'a> {
    word: &'a str,
    width: usize,
    whitespace: &'a str,
    style: ratatui::style::Style,
}

impl<'a> StyledWord<'a> {
    #[allow(unused)]
    fn new(span: &'a Span<'_>) -> Self {
        // We assume the whitespace consists of ' ' only. This allows us to
        // compute the display width in constant time.
        Self::new_impl(&span.content, span.style)
    }

    fn new_sub_span(span: &'a Span<'_>, start: usize, end: usize) -> Self {
        let content = &span.content[start..end];
        Self::new_impl(content, span.style)
    }

    fn new_impl(content: &'a str, style: ratatui::style::Style) -> Self {
        let trimmed = content.trim_end_matches(' ');
        Self {
            word: trimmed,
            width: display_width(trimmed),
            whitespace: &content[trimmed.len()..],
            style,
        }
    }

    fn empty() -> Self {
        Self {
            word: "",
            width: 0,
            whitespace: "",
            style: ratatui::style::Style::default(),
        }
    }

    fn word_span(&self) -> Option<Span<'a>> {
        (!self.word.is_empty()).then(|| Span::styled(self.word, self.style))
    }

    fn whitespace_span(&self) -> Option<Span<'a>> {
        (!self.whitespace.is_empty())
            .then(|| Span::styled(self.whitespace, self.style))
    }

    /// Break this span into smaller words with a width of at most `line_width`.
    /// The whitespace from this `SpanWord` is added to the last piece.
    fn break_apart<'b>(
        &'b self,
        line_width: usize,
    ) -> impl Iterator<Item = StyledWord<'a>> + 'b {
        let mut char_indices = self.word.char_indices();
        let mut offset = 0;
        let mut width = 0;

        std::iter::from_fn(move || {
            while let Some((idx, ch)) = char_indices.next() {
                if skip_ansi_escape_sequence(
                    ch,
                    &mut char_indices.by_ref().map(|(_, ch)| ch),
                ) {
                    continue;
                }

                if width > 0 && width + ch_width(ch) > line_width {
                    let word = StyledWord {
                        word: &self.word[offset..idx],
                        width,
                        whitespace: "",
                        style: self.style,
                    };
                    offset = idx;
                    width = ch_width(ch);
                    return Some(word);
                }

                width += ch_width(ch);
            }

            if offset < self.word.len() {
                let word = StyledWord {
                    word: &self.word[offset..],
                    width,
                    whitespace: self.whitespace,
                    style: self.style,
                };
                offset = self.word.len();
                return Some(word);
            }

            None
        })
    }
}

impl<'a> Fragment for StyledWord<'a> {
    fn width(&self) -> f64 {
        // self.width is the display width, which is what we care about here.
        self.width as f64
    }

    fn whitespace_width(&self) -> f64 {
        // Since whitespace is always ASCII spaces, this is equal to the number
        // of whitespace characters.
        self.whitespace.len() as f64
    }

    fn penalty_width(&self) -> f64 {
        // We don't insert hyphens or anything similar else -- just use 0.0
        // here.
        0.0
    }
}

/// Forcibly break spans wider than `line_width` into smaller spans.
///
/// This simply calls [`StyledWord::break_apart`] on spans that are too wide.
fn break_words<'a, I>(spans: I, line_width: usize) -> Vec<StyledWord<'a>>
where
    I: IntoIterator<Item = StyledWord<'a>>,
{
    let mut shortened_spans = Vec::new();
    for span in spans {
        if span.width() > line_width as f64 {
            shortened_spans.extend(span.break_apart(line_width));
        } else {
            shortened_spans.push(span);
        }
    }
    shortened_spans
}

/// The CSI or “Control Sequence Introducer” introduces an ANSI escape
/// sequence. This is typically used for colored text and will be
/// ignored when computing the text width.
const CSI: (char, char) = ('\x1b', '[');
/// The final bytes of an ANSI escape sequence must be in this range.
const ANSI_FINAL_BYTE: std::ops::RangeInclusive<char> = '\x40'..='\x7e';

/// Skip ANSI escape sequences. The `ch` is the current `char`, the
/// `chars` provide the following characters. The `chars` will be
/// modified if `ch` is the start of an ANSI escape sequence.
#[inline]
fn skip_ansi_escape_sequence<I: Iterator<Item = char>>(
    ch: char,
    chars: &mut I,
) -> bool {
    if ch == CSI.0 && chars.next() == Some(CSI.1) {
        // We have found the start of an ANSI escape code, typically
        // used for colored terminal text. We skip until we find a
        // "final byte" in the range 0x40–0x7E.
        for ch in chars {
            if ANSI_FINAL_BYTE.contains(&ch) {
                return true;
            }
        }
    }
    false
}

fn ch_width(ch: char) -> usize {
    unicode_width::UnicodeWidthChar::width(ch).unwrap_or(0)
}
