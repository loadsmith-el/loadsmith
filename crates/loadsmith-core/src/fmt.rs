//! Shared number/duration formatting for the human report.

use std::time::Duration;

/// Formats an integer with thousands separators: `100000` → `100,000`.
pub fn fmt_number(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

/// Formats a duration as `HH:MM:SS.mmm` — millisecond precision so sub-second
/// runs are still legible (a fast pump shows `00:00:00.412`, not `00:00:00`).
pub fn fmt_duration(d: Duration) -> String {
    let total = d.as_secs();
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    let ms = d.subsec_millis();
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

/// Formats a duration compactly for inline use: `3.2s`, `1m04s`.
pub fn fmt_duration_short(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 60.0 {
        format!("{secs:.1}s")
    } else {
        let total = d.as_secs();
        format!("{}m{:02}s", total / 60, total % 60)
    }
}
