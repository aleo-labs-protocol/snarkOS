// Copyright (c) 2019-2026 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{self, Write};

use tracing::field::{Field, Visit};
use tracing_subscriber::{
    field::RecordFields,
    fmt::{FormatFields, format::Writer},
};

/// Formats the fields of a log event.
///
/// The `message` field is written first. A `dim = true` field dims the whole
/// line on a TTY and is not printed itself. Every other field is appended as
/// ` name=value`.
///
/// Styling codes are written by this formatter only. Field values pass through
/// [`Escaping`], so ANSI control characters inside a logged value, such as a
/// string received from a peer, are printed as escaped text.
pub struct SnarkosFields;

impl<'w> FormatFields<'w> for SnarkosFields {
    fn format_fields<R: RecordFields>(&self, mut writer: Writer<'w>, fields: R) -> fmt::Result {
        let mut collector = Collector::default();
        fields.record(&mut collector);

        let ansi = writer.has_ansi_escapes();
        let sanitize = writer.sanitizes_ansi_escapes();
        let mut out = Escaping { inner: &mut writer, sanitize };

        if ansi && collector.dim {
            out.raw(DIM)?;
        }
        out.write_str(&collector.message)?;
        for (name, value) in &collector.others {
            out.raw(" ")?;
            out.write_str(name)?;
            out.raw("=")?;
            out.write_str(value)?;
        }
        if ansi && collector.dim {
            out.raw(RESET)?;
        }
        Ok(())
    }
}

const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

/// Collects the fields of one event.
#[derive(Default)]
struct Collector {
    message: String,
    dim: bool,
    others: Vec<(&'static str, String)>,
}

impl Visit for Collector {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message.push_str(value),
            name => self.others.push((name, format!("{value:?}"))),
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        match field.name() {
            "dim" => self.dim = value,
            _ => self.record_debug(field, &value),
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "message" => {
                // Writing to a `String` cannot fail.
                let _ = write!(self.message, "{value:?}");
            }
            name => self.others.push((name, format!("{value:?}"))),
        }
    }
}

/// Writes text to the event writer. With `sanitize` set, the control characters
/// that terminals interpret are written as escaped text, following the rules of
/// tracing-subscriber's own message escaping.
struct Escaping<'a, 'w> {
    inner: &'a mut Writer<'w>,
    sanitize: bool,
}

impl Escaping<'_, '_> {
    /// Writes `s` without escaping. Only used for the styling codes above.
    fn raw(&mut self, s: &str) -> fmt::Result {
        self.inner.write_str(s)
    }
}

impl Write for Escaping<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        if !self.sanitize {
            return self.inner.write_str(s);
        }
        for ch in s.chars() {
            match ch {
                '\x1b' => self.inner.write_str("\\x1b")?,
                '\x07' => self.inner.write_str("\\x07")?,
                '\x08' => self.inner.write_str("\\x08")?,
                '\x0c' => self.inner.write_str("\\x0c")?,
                '\x7f' => self.inner.write_str("\\x7f")?,
                ch if (0x80..=0x9f).contains(&(ch as u32)) => write!(self.inner, "\\u{{{:x}}}", ch as u32)?,
                ch => self.inner.write_char(ch)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        io,
        sync::{Arc, Mutex},
    };
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Runs `log` under a subscriber that uses `SnarkosFields` and returns the output.
    fn capture(ansi: bool, log: impl FnOnce()) -> String {
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::Layer::default()
                .with_ansi(ansi)
                .without_time()
                .with_target(false)
                .fmt_fields(SnarkosFields)
                .with_writer(move || writer.clone()),
        );
        tracing::subscriber::with_default(subscriber, log);
        let bytes = buffer.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn dim_covers_the_whole_line() {
        let out = capture(true, || tracing::info!(dim = true, "  Connected to: {}", "1.2.3.4"));
        assert!(out.ends_with("\x1b[2m  Connected to: 1.2.3.4\x1b[0m\n"), "{out:?}");
    }

    #[test]
    fn plain_lines_are_not_styled() {
        let out = capture(true, || tracing::info!("Skipping round {}", 3));
        assert!(out.ends_with("\x1b[0m Skipping round 3\n"), "{out:?}");
        assert!(!out.contains(DIM), "{out:?}");
    }

    #[test]
    fn other_fields_are_appended() {
        let out = capture(false, || tracing::info!(dim = true, count = 3, "message"));
        assert!(out.ends_with("message count=3\n"), "{out:?}");
        assert!(!out.contains('\x1b'));
    }

    #[test]
    fn control_characters_in_values_are_escaped() {
        let out =
            capture(true, || tracing::info!(dim = true, peer = %"\x1b[31mred\x1b[0m", "title \x1b]0;owned\x07 set"));
        assert!(out.ends_with("\x1b[2mtitle \\x1b]0;owned\\x07 set peer=\\x1b[31mred\\x1b[0m\x1b[0m\n"), "{out:?}");
    }
}
