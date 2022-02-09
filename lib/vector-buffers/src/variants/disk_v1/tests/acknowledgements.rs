use futures::{SinkExt, StreamExt};
use tokio_test::{assert_pending, task::spawn};

use super::create_default_buffer_v1;
use crate::{
    assert_reader_v1_delete_position, assert_reader_writer_v1_positions,
    test::common::{with_temp_dir, SizedRecord, install_tracing_helpers, MultiEventRecord},
    EventCount, variants::disk_v1::reader::FLUSH_INTERVAL,
};

#[tokio::test]
async fn acking_single_event_advances_delete_offset() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a regular buffer, no customizations required.
            let (mut writer, mut reader, acker) = create_default_buffer_v1(data_dir).await;
            assert_reader_writer_v1_positions!(reader, writer, 0, 0);

            // Write a simple single-event record and make writer offset moves forward by the
            // expected amount, since the entry key should be increment by event count:
            let record = SizedRecord(360);
            assert_eq!(record.event_count(), 1);
            writer
                .send(record.clone())
                .await
                .expect("write should not fail");
            assert_reader_writer_v1_positions!(reader, writer, 0, record.event_count());

            // And now read it out which should give us a matching record, while our delete offset
            // is still lagging behind the read offset since we haven't yet acknowledged the record:
            let read_record = reader.next().await.expect("read should not fail");
            assert_reader_writer_v1_positions!(reader, writer, record.event_count(), record.event_count());

            assert_eq!(record, read_record);

            // Now acknowledge the record by using an amount equal to the record's event count, which should
            // be one but we're just trying to exercise the codepaths to make sure single-event records
            // work the same as multi-event records.
            //
            // Since the logic to acknowledge records is driven by trying to read, we have to
            // initiate a read first and then do our checks, but it also has to be fake spawned
            // since there's nothing else to read and we'd be awaiting forever.
			//
			// Additionally -- I know, I know -- we have to advance time to clear the flush
			// interval, since we only flush after a certain amount of time has elapsed to batch
			// deletes to the database:
            assert_reader_v1_delete_position!(reader, 0);
            assert_eq!(read_record.event_count(), 1);
            acker.ack(record.event_count());

			tokio::time::pause();

            let mut staged_read = spawn(async { reader.next().await });
            assert_pending!(staged_read.poll());
            drop(staged_read);

            assert_reader_v1_delete_position!(reader, 0);
			
			tokio::time::advance(FLUSH_INTERVAL).await;

			let mut staged_read = spawn(async { reader.next().await });
            assert_pending!(staged_read.poll());
            drop(staged_read);

            assert_reader_v1_delete_position!(reader, record.event_count());
        }
    })
    .await;
}

#[tokio::test]
async fn acking_multi_event_advances_delete_offset() {
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a regular buffer, no customizations required.
            let (mut writer, mut reader, acker) = create_default_buffer_v1(data_dir).await;
            assert_reader_writer_v1_positions!(reader, writer, 0, 0);

            // Write a simple multi-event record and make writer offset moves forward by the
            // expected amount, since the entry key should be increment by event count:
            let record = MultiEventRecord(14);
            assert_eq!(record.event_count(), 14);
            writer
                .send(record.clone())
                .await
                .expect("write should not fail");
            assert_reader_writer_v1_positions!(reader, writer, 0, record.event_count());

            // And now read it out which should give us a matching record, while our delete offset
            // is still lagging behind the read offset since we haven't yet acknowledged the record:
            let read_record = reader.next().await.expect("read should not fail");
            assert_reader_writer_v1_positions!(reader, writer, record.event_count(), record.event_count());

            assert_eq!(record, read_record);

            // Now acknowledge the record by using an amount equal to the record's event count.
            //
            // Since the logic to acknowledge records is driven by trying to read, we have to
            // initiate a read first and then do our checks, but it also has to be fake spawned
            // since there's nothing else to read and we'd be awaiting forever.
			//
			// Additionally -- I know, I know -- we have to advance time to clear the flush
			// interval, since we only flush after a certain amount of time has elapsed to batch
			// deletes to the database:
            assert_reader_v1_delete_position!(reader, 0);
            assert_eq!(read_record.event_count(), 14);
            acker.ack(record.event_count());

			tokio::time::pause();

            let mut staged_read = spawn(async { reader.next().await });
            assert_pending!(staged_read.poll());
            drop(staged_read);

            assert_reader_v1_delete_position!(reader, 0);
			
			tokio::time::advance(FLUSH_INTERVAL).await;

			let mut staged_read = spawn(async { reader.next().await });
            assert_pending!(staged_read.poll());
            drop(staged_read);

            assert_reader_v1_delete_position!(reader, record.event_count());
        }
    })
    .await;
}

#[tokio::test]
async fn acking_multi_event_advances_delete_offset_incremental() {
	let _ = install_tracing_helpers();
    with_temp_dir(|dir| {
        let data_dir = dir.to_path_buf();

        async move {
            // Create a regular buffer, no customizations required.
            let (mut writer, mut reader, acker) = create_default_buffer_v1(data_dir).await;
            assert_reader_writer_v1_positions!(reader, writer, 0, 0);

            // Write a simple multi-event record and make writer offset moves forward by the
            // expected amount, since the entry key should be increment by event count:
            let record = MultiEventRecord(14);
            assert_eq!(record.event_count(), 14);
            writer
                .send(record.clone())
                .await
                .expect("write should not fail");
            assert_reader_writer_v1_positions!(reader, writer, 0, record.event_count());

            // And now read it out which should give us a matching record, while our delete offset
            // is still lagging behind the read offset since we haven't yet acknowledged the record:
            let read_record = reader.next().await.expect("read should not fail");
            assert_reader_writer_v1_positions!(reader, writer, record.event_count(), record.event_count());

            assert_eq!(record, read_record);

            // Now ack the record by using an amount equal to the record's event count, but do it incrementally.
            //
            // Since the logic to acknowledge records is driven by trying to read, we have to
            // initiate a read first and then do our checks, but it also has to be fake spawned
            // since there's nothing else to read and we'd be awaiting forever.
			//
			// Additionally -- I know, I know -- we have to advance time to clear the flush
			// interval, since we only flush after a certain amount of time has elapsed to batch
			// deletes to the database:
            assert_reader_v1_delete_position!(reader, 0);
            assert_eq!(read_record.event_count(), 14);

			let increments = &[4, 7, 2, 1];
			assert_eq!(read_record.event_count(), increments.iter().sum::<usize>());

			tokio::time::pause();

			for increment in increments {
				assert_reader_v1_delete_position!(reader, 0);

				acker.ack(*increment);

				let mut staged_read = spawn(async { reader.next().await });
				assert_pending!(staged_read.poll());
				drop(staged_read);

				tokio::time::advance(FLUSH_INTERVAL).await;
			}
			
			// Now we've acknowledged all events comprising the record, so our next read should
			// actually drive the flush logic to delete it:
			let mut staged_read = spawn(async { reader.next().await });
            assert_pending!(staged_read.poll());
            drop(staged_read);

            assert_reader_v1_delete_position!(reader, record.event_count());
        }
    })
    .await;
}
