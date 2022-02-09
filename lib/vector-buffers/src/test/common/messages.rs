use std::{error, fmt, io, mem};

use bytes::{Buf, BufMut};
use quickcheck::{Arbitrary, Gen};
use vector_common::byte_size_of::ByteSizeOf;

use crate::{encoding::FixedEncodable, EventCount};

#[derive(Debug)]
pub struct EncodeError;

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for EncodeError {}

#[derive(Debug)]
pub struct DecodeError;

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for DecodeError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    id: u64,
}

impl Message {
    pub(crate) fn new(id: u64) -> Self {
        Message { id }
    }
}

impl ByteSizeOf for Message {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl EventCount for Message {
    fn event_count(&self) -> usize {
        1
    }
}

impl Arbitrary for Message {
    fn arbitrary(g: &mut Gen) -> Self {
        Message {
            id: u64::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.id.shrink().map(|id| Message { id }))
    }
}

impl FixedEncodable for Message {
    type EncodeError = EncodeError;
    type DecodeError = DecodeError;

    fn encode<B>(self, buffer: &mut B) -> Result<(), Self::EncodeError>
    where
        B: BufMut,
        Self: Sized,
    {
        buffer.put_u64(self.id);
        Ok(())
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(mem::size_of::<u64>())
    }

    fn decode<B>(mut buffer: B) -> Result<Self, Self::DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        let id = buffer.get_u64();
        Ok(Message::new(id))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SizedRecord(pub u32);

impl ByteSizeOf for SizedRecord {
    fn allocated_bytes(&self) -> usize {
        self.0 as usize
    }
}

impl EventCount for SizedRecord {
    fn event_count(&self) -> usize {
        1
    }
}

impl FixedEncodable for SizedRecord {
    type EncodeError = io::Error;
    type DecodeError = io::Error;

    fn encode<B>(self, buffer: &mut B) -> Result<(), Self::EncodeError>
    where
        B: BufMut,
    {
        if buffer.remaining_mut() < self.0 as usize + 4 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "not enough capacity to encode record",
            ));
        }

        buffer.put_u32(self.0);
        buffer.put_bytes(0x42, self.0 as usize);
        Ok(())
    }

    fn decode<B>(mut buffer: B) -> Result<SizedRecord, Self::DecodeError>
    where
        B: Buf,
    {
        let buf_len = buffer.get_u32();
        buffer.advance(buf_len as usize);
        Ok(SizedRecord(buf_len))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct UndecodableRecord;

impl ByteSizeOf for UndecodableRecord {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl EventCount for UndecodableRecord {
    fn event_count(&self) -> usize {
        1
    }
}

impl FixedEncodable for UndecodableRecord {
    type EncodeError = io::Error;
    type DecodeError = io::Error;

    fn encode<B>(self, buffer: &mut B) -> Result<(), Self::EncodeError>
    where
        B: BufMut,
    {
        if buffer.remaining_mut() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "not enough capacity to encode record",
            ));
        }

        buffer.put_u32(42);
        Ok(())
    }

    fn decode<B>(_buffer: B) -> Result<UndecodableRecord, Self::DecodeError>
    where
        B: Buf,
    {
        Err(io::Error::new(io::ErrorKind::Other, "failed to decode"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MultiEventRecord(pub usize);

impl ByteSizeOf for MultiEventRecord {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl EventCount for MultiEventRecord {
    fn event_count(&self) -> usize {
        self.0
    }
}

impl FixedEncodable for MultiEventRecord {
    type EncodeError = io::Error;
    type DecodeError = io::Error;

    fn encode<B>(self, buffer: &mut B) -> Result<(), Self::EncodeError>
    where
        B: BufMut,
    {
        if buffer.remaining_mut() < self.0 as usize + 4 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "not enough capacity to encode record",
            ));
        }

        buffer.put_u32(self.0 as u32);
        buffer.put_bytes(0x42, self.0 as usize);
        Ok(())
    }

    fn decode<B>(mut buffer: B) -> Result<MultiEventRecord, Self::DecodeError>
    where
        B: Buf,
    {
        let event_count = buffer.get_u32();
        buffer.advance(event_count as usize);
        Ok(MultiEventRecord(event_count as usize))
    }
}
