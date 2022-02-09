// TODO tests:
// - test unacked keys when we have one undecodable read only, then undecodable + valid read, then
//   undecodable + undecodable
// - test initial size is valid with multi-event records

use std::sync::atomic::Ordering;

use futures::{SinkExt, StreamExt};

use super::create_default_buffer_v1;
use crate::{
    test::common::{with_temp_dir, SizedRecord, MultiEventRecord},
    EventCount, assert_reader_writer_v1_positions, variants::disk_v1::tests::drive_reader_to_flush,
};

#[tokio::test]
async fn basic_read_write_loop() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a regular buffer, no customizations required.
            let (mut writer, mut reader, acker) = create_default_buffer_v1(data_dir).await;
            assert_reader_writer_v1_positions!(reader, writer, 0, 0);

            let expected_items = (512..768)
                .into_iter()
                .cycle()
                .take(2000)
                .map(SizedRecord)
                .collect::<Vec<_>>();
            let input_items = expected_items.clone();
            let expected_position = expected_items.iter()
                .map(EventCount::event_count)
                .sum::<usize>();

            // Now create a reader and writer task that will take a set of input messages, buffer
            // them, read them out, and then make sure nothing was missed.
            let write_task = tokio::spawn(async move {
                for item in input_items {
                    trace!("writing record {:?}", item);
                    writer.send(item).await.expect("write should not fail");
                }
                writer.flush().await.expect("writer flush should not fail");
                writer.close().await.expect("writer close should not fail");
                writer.offset.load(Ordering::SeqCst)
            });

            let read_task = tokio::spawn(async move {
                let mut items = Vec::new();
                while let Some(record) = reader.next().await {
                    let events_len = record.event_count();
                    trace!("read record {:?}, len {}", record, events_len);

                    items.push(record);
                    acker.ack(events_len);
                }
                (reader, items)
            });

            // Wait for both tasks to complete.
            let writer_position = write_task.await.expect("write task should not panic");
            let (mut reader, actual_items) = read_task.await.expect("read task should not panic");

            // Make sure we got the right items.
            assert_eq!(actual_items, expected_items);

            // Drive the reader with one final read which should ensure all acknowledged reads are
            // now flushed, before we check the final reader/writer offsets:
            drive_reader_to_flush(&mut reader).await;

            let reader_position = reader.read_offset;
            let delete_position = reader.delete_offset;
            assert_eq!(expected_position, writer_position,
                "expected writer offset of {}, got {}", expected_position, writer_position);
            assert_eq!(expected_position, reader_position,
                "expected reader offset of {}, got {}", expected_position, reader_position);
            assert_eq!(expected_position, delete_position,
                "expected delete offset of {}, got {}", expected_position, delete_position);
        }
    })
    .await;
}

#[tokio::test]
async fn basic_read_write_loop_batched() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a regular buffer, no customizations required.
            let (mut writer, mut reader, acker) = create_default_buffer_v1(data_dir).await;

            let expected_items = (512..768)
                .into_iter()
                .cycle()
                .take(2000)
                .map(MultiEventRecord)
                .collect::<Vec<_>>();
            let input_items = expected_items.clone();
            let expected_position = expected_items.iter()
                .map(EventCount::event_count)
                .sum::<usize>();

            // Now create a reader and writer task that will take a set of input messages, buffer
            // them, read them out, and then make sure nothing was missed.
            let write_task = tokio::spawn(async move {
                for item in input_items {
                    writer.send(item).await.expect("write should not fail");
                }
                writer.flush().await.expect("writer flush should not fail");
                writer.close().await.expect("writer close should not fail");
                writer.offset.load(Ordering::SeqCst)
            });

            let read_task = tokio::spawn(async move {
                let mut items = Vec::new();
                while let Some(record) = reader.next().await {
                    let events_len = record.event_count();
                    items.push(record);
                    acker.ack(events_len);
                }
                (reader, items)
            });

            // Wait for both tasks to complete.
            let writer_position = write_task.await.expect("write task should not panic");
            let (mut reader, actual_items) = read_task.await.expect("read task should not panic");

            // Make sure we got the right items.
            assert_eq!(actual_items, expected_items);

            // Drive the reader with one final read which should ensure all acknowledged reads are
            // now flushed, before we check the final reader/writer offsets:
            drive_reader_to_flush(&mut reader).await;

            let reader_position = reader.read_offset;
            let delete_position = reader.delete_offset;
            assert_eq!(expected_position, writer_position,
                "expected writer offset of {}, got {}", expected_position, writer_position);
            assert_eq!(expected_position, reader_position,
                "expected reader offset of {}, got {}", expected_position, reader_position);
            assert_eq!(expected_position, delete_position,
                "expected delete offset of {}, got {}", expected_position, delete_position);
        }
    })
    .await;
}
