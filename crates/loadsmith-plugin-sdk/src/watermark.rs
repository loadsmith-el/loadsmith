//! Tie-safe incremental watermarks.
//!
//! A resumed run reads strictly after the persisted watermark (`> value`). If
//! that value were the cursor of a batch's last row while the next batch starts
//! with more rows of the same value, the resume would skip those rows. So a
//! source reports, while it streams, only a value it knows is **complete**: one
//! followed by a strictly greater value. Within a batch ordered by the cursor,
//! that is the last value that differs from the batch's final one. Once the
//! stream has ended, the final value is complete too.
//!
//! The rows of the boundary value are read again on resume (at-least-once, as
//! the incremental contract already states).

/// Tracks a source's cursor values batch by batch. Feed every batch to
/// [`batch`](Self::batch) in stream order and report
/// [`watermark`](Self::watermark) from `current_watermark`.
#[derive(Debug, Clone, Default)]
pub struct TieSafeCursor<T> {
    complete: Option<T>,
    last: Option<T>,
    ended: bool,
}

impl<T: PartialEq + Clone> TieSafeCursor<T> {
    pub fn new() -> Self {
        Self { complete: None, last: None, ended: false }
    }

    /// One batch's cursor values, **newest first** (from its last row back),
    /// NULLs left out. Only as many are read as it takes to find a value
    /// different from the last one.
    pub fn batch(&mut self, mut newest_first: impl Iterator<Item = T>) {
        let Some(last) = newest_first.next() else { return };
        if let Some(earlier) = newest_first.find(|v| *v != last) {
            self.complete = Some(earlier);
        } else if self.last.as_ref().is_some_and(|prev| *prev != last) {
            // The whole batch holds one value, past the previous batch's
            // last: that one is complete now.
            self.complete = self.last.clone();
        }
        self.last = Some(last);
    }

    /// The stream ended: every value read is complete.
    pub fn end(&mut self) {
        self.ended = true;
    }

    /// The value safe to persist: the last complete one, or the last one read
    /// once the stream ended.
    pub fn watermark(&self) -> Option<&T> {
        if self.ended { self.last.as_ref().or(self.complete.as_ref()) } else { self.complete.as_ref() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(c: &mut TieSafeCursor<i32>, batch: &[i32]) {
        c.batch(batch.iter().rev().copied());
    }

    #[test]
    fn the_last_value_of_a_batch_waits_for_a_greater_one() {
        let mut c = TieSafeCursor::new();
        feed(&mut c, &[1, 2, 3, 3]);
        assert_eq!(c.watermark(), Some(&2), "3 may continue in the next batch");
        feed(&mut c, &[3, 3, 4]);
        assert_eq!(c.watermark(), Some(&3));
        c.end();
        assert_eq!(c.watermark(), Some(&4));
    }

    #[test]
    fn a_batch_of_one_value_completes_the_previous_one() {
        let mut c = TieSafeCursor::new();
        feed(&mut c, &[5, 5]);
        assert_eq!(c.watermark(), None, "nothing is complete yet");
        feed(&mut c, &[5, 5]);
        assert_eq!(c.watermark(), None);
        feed(&mut c, &[7, 7]);
        assert_eq!(c.watermark(), Some(&5));
        feed(&mut c, &[]);
        assert_eq!(c.watermark(), Some(&5), "an empty batch changes nothing");
        c.end();
        assert_eq!(c.watermark(), Some(&7));
    }

    #[test]
    fn only_the_tail_is_read() {
        let mut c = TieSafeCursor::new();
        let mut read = 0;
        c.batch([9, 9, 8, 1, 0].into_iter().inspect(|_| read += 1));
        assert_eq!((c.watermark(), read), (Some(&8), 3));
    }
}
