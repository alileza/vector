use std::path::Path;

use futures::StreamExt;
use tokio_test::{assert_pending, task::spawn};

use crate::{buffer_usage_data::BufferUsageHandle, Acker, Bufferable, WhenFull, variants::disk_v1::reader::FLUSH_INTERVAL};

use super::{open, Reader, Writer};

mod acknowledgements;
mod basic;
mod event_count;
mod naming;

// Default of 1GB.
const DEFAULT_DISK_BUFFER_V1_SIZE_BYTES: u64 = 1024 * 1024 * 1024;

pub(crate) async fn create_default_buffer_v1<P, R>(data_dir: P) -> (Writer<R>, Reader<R>, Acker)
where
    P: AsRef<Path>,
    R: Bufferable + Clone,
{
    let usage_handle = BufferUsageHandle::noop(WhenFull::Block);
    open(
        data_dir.as_ref(),
        "disk_buffer_v1",
        DEFAULT_DISK_BUFFER_V1_SIZE_BYTES,
        usage_handle,
    )
    .expect("should not fail to create buffer")
}

async fn drive_reader_to_flush<T: Bufferable>(reader: &mut Reader<T>) {
    tokio::time::pause();
    tokio::time::advance(FLUSH_INTERVAL).await;

    let mut staged_read = spawn(async { reader.next().await });
    assert_pending!(staged_read.poll());
    drop(staged_read);
}

#[macro_export]
macro_rules! assert_reader_writer_v1_positions {
    ($reader:expr, $writer:expr, $expected_reader:expr, $expected_writer:expr) => {{
        let reader_offset = $reader.read_offset;
        let writer_offset = $writer.offset.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            reader_offset, $expected_reader,
            "expected reader offset of {}, got {} instead",
            $expected_reader, reader_offset
        );
        assert_eq!(
            writer_offset, $expected_writer,
            "expected writer offset of {}, got {} instead",
            $expected_writer, writer_offset
        );
    }};
}

#[macro_export]
macro_rules! assert_reader_v1_delete_position {
    ($reader:expr, $expected_reader:expr) => {{
        let delete_offset = $reader.delete_offset;
        assert_eq!(
            delete_offset, $expected_reader,
            "expected delete offset of {}, got {} instead",
            $expected_reader, delete_offset
        );
    }};
}
