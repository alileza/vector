use std::{
    collections::VecDeque,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{task::AtomicWaker, Stream};
use leveldb::database::{
    batch::{Batch, Writebatch},
    compaction::Compaction,
    iterator::{Iterable, LevelDBIterator},
    options::{ReadOptions, WriteOptions},
    Database,
};
use parking_lot::Mutex;
use tokio::{task::JoinHandle, time::Instant};

use super::Key;
use crate::{buffer_usage_data::BufferUsageHandle, Bufferable};

/// How much time needs to pass between compaction to trigger new one.
const MIN_TIME_UNCOMPACTED: Duration = Duration::from_secs(60);

/// Minimal size of uncompacted for which a compaction can be triggered.
const MIN_UNCOMPACTED_SIZE: u64 = 4 * 1024 * 1024;

/// How often we flush deletes to the database.
pub const FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// Event count for a logical read.
///
/// As we track all reads, whether or not they can be successfully decoded, we need to track
/// undecodable reads such that we know that we have to do extra work to correctly figure out how
/// many events they represented.  This is required to ensure our acknowledgement accounting is
/// accurate so we can correctly delete only truly acknowledged (or undecodable) records.
#[derive(Clone, Debug)]
enum ItemEventCount {
    /// An undecodable item has no known event count until a read which occurs after it is processed,
    /// allowing the offsets to be calculated.
    Unknown,

    /// A successful read that has a known event count.
    Known(usize),
}

/// A deletion marker for a read that has not yet been fully acknowledged.
///
/// Used to provide in-order deletion of reads by carrying enough information to figure out when
/// we've fully acknowledged all events that compromise a given logical read from the buffer.
#[derive(Clone, Debug)]
pub struct PendingDelete {
    key: usize,
    item_event_count: ItemEventCount,
    item_bytes: usize,
}

/// A deletion marker for a read that is fully acknowledged and can be deleted.
#[derive(Clone, Debug)]
struct EligibleDelete {
    key: usize,
    item_event_count: usize,
    item_bytes: usize,
    acks_to_consume: Option<usize>,
}

/// The reader side of N to 1 channel through leveldb.
///
/// Reader maintains/manages events thorugh several stages.
/// Unread -> Read -> Deleted -> Compacted
///
/// So the disk buffer (indices/keys) is separated into following regions.
/// |--Compacted--|--Deleted--|--Read--|--Unread
///  ^             ^   ^       ^        ^
///  |             |   |-acked-|        |
///  0   `compacted_offset`    |        |
///                     `delete_offset` |
///                                `read_offset`
pub struct Reader<T> {
    /// Leveldb database.
    /// Shared with Writers.
    pub(crate) db: Arc<Database<Key>>,
    /// First unread key
    pub(crate) read_offset: usize,
    /// First uncompacted key
    pub(crate) compacted_offset: usize,
    /// First not deleted key
    pub(crate) delete_offset: usize,
    /// Number of acked events that haven't been deleted from
    /// database. Used for batching deletes.
    pub(crate) acked: usize,
    /// Reader is notified by Writers through this Waker.
    /// Shared with Writers.
    pub(crate) write_notifier: Arc<AtomicWaker>,
    /// Writers blocked by disk being full.
    /// Shared with Writers.
    pub(crate) blocked_write_tasks: Arc<Mutex<Vec<Waker>>>,
    /// Size of unread events in bytes.
    /// Shared with Writers.
    pub(crate) current_size: Arc<AtomicU64>,
    /// Number of oldest read, not deleted, events that have been acked by the consumer.
    /// Shared with consumer.
    pub(crate) ack_counter: Arc<AtomicUsize>,
    /// Size of deleted, not compacted, events in bytes.
    pub(crate) uncompacted_size: u64,
    /// Keys of unacked events.
    pub(crate) unacked_keys: VecDeque<PendingDelete>,
    /// Buffer for internal use.
    pub(crate) buffer: VecDeque<(Key, Vec<u8>)>,
    /// Limit on uncompacted_size after which we trigger compaction.
    pub(crate) max_uncompacted_size: u64,
    /// Last time that compaction was triggered.
    pub(crate) last_compaction: Instant,
    /// Last time that delete flush was triggered.
    pub(crate) last_flush: Instant,
    // Pending read from the LevelDB datasbase
    pub(crate) pending_read: Option<JoinHandle<Vec<(Key, Vec<u8>)>>>,
    // Buffer usage data.
    pub(crate) usage_handle: BufferUsageHandle,
    pub(crate) phantom: PhantomData<T>,
}

impl<T> Stream for Reader<T>
where
    T: Bufferable,
{
    type Item = T;

    #[cfg_attr(test, instrument(skip(self, cx), level = "debug"))]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // If there's no value at or beyond `read_offset`, we return `Poll::Pending` and rely on
        // `Writer` using `self.write_notifier` to wake this task up after the next write.
        this.write_notifier.register(cx.waker());

        // Check for any pending acknowledgements which may make a read eligible to finally be
        // deleted from the buffer entirely.
        this.try_flush();

        if this.buffer.is_empty() {
            // This will usually complete instantly, but in the case of a large
            // queue (or a fresh launch of the app), this will have to go to
            // disk.
            loop {
                match this.pending_read.take() {
                    None => {
                        // We have no pending read in-flight, so queue one up.
                        let db = Arc::clone(&this.db);
                        let read_offset = this.read_offset;
                        let handle = tokio::task::spawn_blocking(move || {
                            db.iter(ReadOptions::new())
                                .from(&Key(read_offset))
                                .take(1000)
                                .collect::<Vec<_>>()
                        });

                        // Store the handle, and let the loop come back around.
                        this.pending_read = Some(handle);
                    }
                    Some(mut handle) => match Pin::new(&mut handle).poll(cx) {
                        Poll::Ready(r) => {
                            match r {
                                Ok(items) => {
                                    trace!(batch_size = items.len(), "LevelDB read completed.");
                                    this.buffer.extend(items);
                                }
                                Err(error) => error!(%error, "Error during read."),
                            }

                            this.pending_read = None;

                            break;
                        }
                        Poll::Pending => {
                            this.pending_read = Some(handle);
                            return Poll::Pending;
                        }
                    },
                }
            }

            // If we've broke out of the loop, our read has completed.
            this.pending_read = None;
        }

        if let Some((key, item_bytes, decode_result)) = this.decode_next_record() {
            trace!(?key, item_bytes, "Got record decode attempt.");
            match decode_result {
                Ok(item) => {
                    this.track_unacked_read(
                        key.0,
                        ItemEventCount::Known(item.event_count()),
                        item_bytes,
                    );
                    Poll::Ready(Some(item))
                }
                Err(error) => {
                    error!(%error, "Error deserializing event.");

                    this.track_unacked_read(key.0, ItemEventCount::Unknown, item_bytes);
                    Pin::new(this).poll_next(cx)
                }
            }
        } else if Arc::strong_count(&this.db) == 1 {
            // There are no writers left
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for Reader<T> {
    fn drop(&mut self) {
        self.flush();
    }
}

impl<T: Bufferable> Reader<T> {
    /// Decodes the next buffered record, if one is available.
    #[cfg_attr(test, instrument(skip(self), level = "trace"))]
    fn decode_next_record(&mut self) -> Option<(Key, usize, Result<T, T::DecodeError>)> {
        self.buffer.pop_front().map(|(key, value)| {
            let item_bytes = value.len();
            let decode_buf = Bytes::from(value);
            (key, item_bytes, T::decode(T::get_metadata(), decode_buf))
        })
    }
}

impl<T> Reader<T> {
    /// Gets the current size of the buffer.
    fn get_buffer_size(&self) -> u64 {
        self.current_size.load(Ordering::Acquire)
    }

    /// Decreases the buffer size by the given amount.
    ///
    /// Returns the new size of the buffer.
    fn decrease_buffer_size(&mut self, amount: u64) -> u64 {
        self.uncompacted_size += amount;
        self.current_size.fetch_sub(amount, Ordering::Release) - amount
    }

    /// Tracks a read for pending deletion.
    ///
    /// Once all items of a read have been acknowledged, it will become eligible to be deleted.
    /// Additionally, reads which failed to decode will become eligible for deletion as soon as the
    /// next read occurs.
    fn track_unacked_read(
        &mut self,
        key: usize,
        item_event_count: ItemEventCount,
        item_bytes: usize,
    ) {
        // We handle two possible scenarios here: a successful read, or an undecodable read.
        //
        // When a read is successful, we know its actual size in terms of event count and bytes, but
        // with an undecodable read, we only know the size in bytes.  As such, we defer the logic of
        // updating metrics until we have enough data to figure out, for any given key, how many
        // items it represented.. which potentially means that we don't fully update the buffer
        // metrics when our last read was undecodable and we haven't yet had a successful read.

        self.read_offset = match item_event_count {
            // We adjust our read offset to be 1 ahead of this key, because we grab records in a
            // "get the next N records that come after key K", so we only need the offset to be
            // right ahead of this key... regardless of whether or not there _is_ something valid at
            // K+1 or the next key is actually K+7, etc.
            ItemEventCount::Unknown => key + 1,
            // Adjust our read offset based on the items within the read, as we rely on
            // the keys to track the number of _effective_ items (sum of "event count" from each item)
            // in the buffer using simple arithmetic between the first and last keys in the buffer.
            ItemEventCount::Known(len) => key + len,
        };

        trace!(key, item_len = ?item_event_count, item_bytes, read_offset = self.read_offset,
            "Tracking unacknowledged read.");

        // Now store a pending delete marker that will eventually be drained in our `try_flush` routine.
        self.unacked_keys.push_back(PendingDelete {
            key,
            item_event_count,
            item_bytes,
        });
    }

    /// Gets the next unacknowledged key which now qualifies for deletion.
    ///
    /// An unacknowledged key qualifies for deletion when the acknowledged offset of events is at or
    /// past the key plus the number of events within the item.
    ///
    /// For unacknowledged keys with an unknown event count (items that failed to decode), another
    /// unacknowledged key must be present after it in order to calculate the key offsets and
    /// determine the item length.
    #[cfg_attr(test, instrument(skip(self), level = "trace"))]
    fn get_next_eligible_delete(&mut self) -> Option<EligibleDelete> {
        let acked_offset = self.delete_offset + self.acked;

        trace!(
            delete_offset = self.delete_offset,
            acked = self.acked,
            pending_deletes = self.unacked_keys.len(),
            "Searching for eligible delete."
        );

        let maybe_marker =
            self.unacked_keys
                .front()
                .cloned()
                .and_then(|marker| match marker.item_event_count {
                    // If the acked offset is ahead of this key, plus its length, it's been fully
                    // acknowledged and we can consume and yield the marker.
                    ItemEventCount::Known(len) => {
                        if marker.key + len <= acked_offset {
                            Some(EligibleDelete {
                                key: marker.key,
                                item_event_count: len,
                                item_bytes: marker.item_bytes,
                                acks_to_consume: Some(len),
                            })
                        } else {
                            None
                        }
                    }
                    // We don't yet know what the item event count is for this key, so we speculatively
                    // peek at the next unacked key, and if one exists, we can determine the item event
                    // count simply by taking the difference between the keys themselves.
                    //
                    // You may also notice that we do no sort of check here about our acknowledged
                    // offset, etc.  That is because this key generated no items that will ever be
                    // acknowledged, so we know we can simply delete this key without breaking anything
                    // else.  We just needed to wait for another read to figure out how many items it
                    // accounted for in the buffer.
                    ItemEventCount::Unknown => self.unacked_keys.get(1).map(|next_marker| {
                        let len = next_marker.key - marker.key;

                        // Because we're artificially driving this deletion, there are no
                        // acknowledgements to actually consume for it.  Doing so would mess up the
                        // balance between `self.delete_offset`/`self.acked`.
                        EligibleDelete {
                            key: marker.key,
                            item_event_count: len,
                            item_bytes: marker.item_bytes,
                            acks_to_consume: None,
                        }
                    }),
                });

        // If we actually got an eligible marker, properly remove it from `self.unacked_keys`.
        if maybe_marker.is_some() {
            let _ = self.unacked_keys.pop_front();
        }

        maybe_marker
    }

    /// Attempt to flush any pending deletes to the database.
    ///
    /// Flushes are driven based on elapsed time to coalsece operations that require modifying the database.
    #[cfg_attr(test, instrument(skip(self), level = "trace"))]
    fn try_flush(&mut self) {
        // Don't flush unless we've overrun our flush interval.
        if self.last_flush.elapsed() < FLUSH_INTERVAL {
            trace!("Last flush was too recent to run again.");
            return;
        }
        self.last_flush = Instant::now();

        self.flush();
    }

    /// Flushes all eligible deletes to the database.
    #[cfg_attr(test, instrument(skip(self), level = "trace"))]
    fn flush(&mut self) {
        debug!("Running flush.");

        // Consume any pending acknowledgements.
        let num_to_delete = self.ack_counter.swap(0, Ordering::Relaxed);
        if num_to_delete > 0 {
            debug!("Consumed {} acknowledgements.", num_to_delete);
            self.acked += num_to_delete;
        }

        // See if any pending deletes actually qualify for deletion, and if so, capture them and
        // actually execute a batch delete operation.
        let mut delete_batch = Writebatch::new();
        let mut total_keys = 0;
        let mut total_items_len = 0;
        let mut total_item_bytes = 0;
        while let Some(eligible_delete) = self.get_next_eligible_delete() {
            let EligibleDelete {
                key,
                item_event_count,
                item_bytes,
                acks_to_consume,
            } = eligible_delete;

            // Add this key to our delete batch.
            delete_batch.delete(Key(key));

            // Adjust our delete offset, and if need be, the amount of remaining acknowledgements.
            //
            // We adjust the delete offset/remaining acks here so that the next call to
            // `get_next_eligible_delete` has updated offsets so we can optimally drain as many
            // eligible deletes as possible in one go.
            self.delete_offset = key.wrapping_add(item_event_count);
            if let Some(amount) = acks_to_consume {
                self.acked -= amount;
            }

            total_keys += 1;
            total_items_len += item_event_count;
            total_item_bytes += item_bytes;
        }

        // If we actually found anything that was ready to be deleted, execute our delete batch
        // and update our buffer usage metrics.
        if total_keys > 0 {
            debug!(
                delete_offset = self.delete_offset,
                "Deleting {} keys from buffer: {} items, {} bytes.",
                total_keys,
                total_items_len,
                total_item_bytes
            );
            self.db.write(WriteOptions::new(), &delete_batch).unwrap();

            assert!(
                self.delete_offset <= self.read_offset,
                "tried to ack beyond read offset"
            );

            // Update our buffer size and buffer usage metrics.
            self.decrease_buffer_size(total_item_bytes as u64);
            self.usage_handle.increment_sent_event_count_and_byte_size(
                total_items_len as u64,
                total_item_bytes as u64,
            );

            // Now that we've actually deleted some items, notify any blocked writers that progress
            // has been made so they can continue writing.
            for task in self.blocked_write_tasks.lock().drain(..) {
                task.wake();
            }
        }

        // Attempt to run a compaction if we've met the criteria to trigger one.
        self.try_compact();
    }

    /// Attempt to trigger a compaction.
    ///
    /// Compaction will only be triggered if certain criteria are met, which are specifically
    /// documented below.
    pub(super) fn try_compact(&mut self) {
        // Compaction can be triggered in two ways:
        //  1. When size of uncompacted is a percentage of total allowed size.
        //     Managed by MAX_UNCOMPACTED. This is to limit the size of disk buffer
        //     under configured max size.
        let max_trigger = self.uncompacted_size > self.max_uncompacted_size;
        //  2. When the size of uncompacted buffer is larger than unread buffer.
        //     If the sink is able to keep up with the sources, this will trigger
        //     with MIN_TIME_UNCOMPACTED interval. And if it's not keeping up,
        //     this won't trigger hence it won't slow it down, which will allow it
        //     to grow until it either hits max_uncompacted_size or manages to catch up.
        //     This is to keep the size of the disk buffer low in idle and up to date
        //     cases.
        let timed_trigger = self.last_compaction.elapsed() >= MIN_TIME_UNCOMPACTED
            && self.uncompacted_size > self.get_buffer_size();

        // Basic requirement to avoid leaving ldb files behind.
        // See:
        // Vector  https://github.com/vectordotdev/vector/issues/7425#issuecomment-849522738
        // leveldb https://github.com/google/leveldb/issues/783
        //         https://github.com/syndtr/goleveldb/issues/166
        let min_size = self.uncompacted_size >= MIN_UNCOMPACTED_SIZE;

        if min_size && (max_trigger || timed_trigger) {
            self.uncompacted_size = 0;

            debug!("Compacting disk buffer.");
            self.db
                .compact(&Key(self.compacted_offset), &Key(self.delete_offset));

            self.compacted_offset = self.delete_offset;
            self.last_compaction = Instant::now();
        }
    }
}
