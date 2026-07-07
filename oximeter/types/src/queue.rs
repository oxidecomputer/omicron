// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::VecDeque;

#[derive(Debug)]
pub struct BoundedQueue<T> {
    queue: VecDeque<T>,
    max_size: usize,
}

impl<T> BoundedQueue<T> {
    pub fn new(max_size: usize) -> Self {
        Self { queue: VecDeque::new(), max_size }
    }

    pub fn extend(&mut self, samples: Vec<T>) -> usize {
        // Append the new samples, ensuring we never exceed `max_size`.
        let n_current_samples = self.queue.len();
        let n_new_samples = samples.len();
        let n_total_samples = n_current_samples + n_new_samples;

        // The easy case is when all the samples fit. Just append them and
        // return 0, since we've not dropped anything.
        let n_dropped = if n_total_samples <= self.max_size {
            self.queue.extend(samples);
            0
        } else {
            // Now, drop samples first from the queue, then the new set
            // of samples, until we're under our limit. Start by computing
            // the total number to be dropped.
            //
            // NOTE: This can't underflow, we're in the branch where
            // `n_total_samples > self.max_size`.
            let n_dropped = n_total_samples - self.max_size;

            // We want to drop from the queue first. We can drop the
            // minimum of the number to be dropped and the queue size. If
            // we're only dropping part of the queue, we'll take `n_dropped` off
            // the front. If we need to drop the whole queue and then some,
            // we'll take the queue size and drop the whole thing.
            let n_to_drop = n_dropped.min(self.queue.len());
            drop(self.queue.drain(..n_to_drop));

            // At this point, we've dropped something (potentially everything)
            // from the queue. We need to figure out how many, if any,
            // to drop from the _incoming_ set of samples. Subtract whatever we
            // dropped immediately above to compute the number left to drop.
            let n_left_to_drop = n_dropped - n_to_drop;

            // Append whatever we don't want to drop.
            self.queue.extend(samples.into_iter().skip(n_left_to_drop));
            n_dropped
        };
        n_dropped
    }

    pub fn iter(&self) -> std::collections::vec_deque::Iter<'_, T> {
        self.queue.iter()
    }

    pub fn drain(&mut self) -> VecDeque<T> {
        std::mem::take(&mut self.queue)
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounded_queue() {
        let mut bq = BoundedQueue::new(5);
        let n_dropped = bq.extend(vec![0, 1, 2, 3, 4]);
        assert_eq!(bq.len(), 5);
        assert_eq!(n_dropped, 0);
        let drained = bq.drain();
        assert_eq!(drained, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_bounded_queue_overflow() {
        let mut bq = BoundedQueue::new(5);
        bq.extend(vec![0, 1, 2, 3, 4]);
        let n_dropped = bq.extend(vec![5, 6, 7]);
        assert_eq!(n_dropped, 3);
        let drained = bq.drain();
        assert_eq!(drained, vec![3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_bounded_queue_sample_overflow() {
        let mut bq = BoundedQueue::new(5);
        let n_dropped = bq.extend(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(n_dropped, 3);
        let drained = bq.drain();
        assert_eq!(drained, vec![3, 4, 5, 6, 7]);
    }
}
