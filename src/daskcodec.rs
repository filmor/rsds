use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use tokio_util::codec::{Decoder, Encoder};
use bytes::{Buf, Bytes, BytesMut};

use crate::prelude::*;

#[derive(Debug)]
pub struct DaskMessage {
    pub message: Bytes,
    pub additional_frames: Vec<Bytes>,
}


impl DaskMessage {
    pub fn new(message: Bytes, additional_frames: Vec<Bytes>) -> Self {
        DaskMessage {
            message,
            additional_frames,
        }
    }
}

impl<T: Into<Bytes>> From<T> for DaskMessage {
    fn from(message: T) -> Self {
        Self {
            message: message.into(),
            additional_frames: Default::default(),
        }
    }
}

pub struct DaskCodec {
    sizes: Option<(u64, Vec<u64>)>,
    main_message: Option<Bytes>,
    other_messages: Vec<Bytes>,
}

impl DaskCodec {
    pub fn new() -> Self {
        DaskCodec {
            sizes: None,
            main_message: None,
            other_messages: Default::default(),
        }
    }
}

impl Decoder for DaskCodec {
    type Item = DaskMessage;
    type Error = crate::DsError;

    fn decode(&mut self, src: &mut BytesMut) -> crate::Result<Option<DaskMessage>> {
        let src = if self.sizes.is_none() {
            let size = src.len() as u64;
            if size < 8 {
                return Ok(None);
            }
            let mut cursor = Cursor::new(src);
            // Following read_u64 cannot failed, hence do not propagate and leave .unwrap() here
            let count: u64 = cursor.read_u64::<LittleEndian>().unwrap();
            let header_size = (count + 1) * 8;
            if size < header_size {
                return Ok(None);
            }
            let first_size = cursor.read_u64::<LittleEndian>().unwrap();
            assert_eq!(first_size, 0);
            let main_size = cursor.read_u64::<LittleEndian>().unwrap();
            let mut sizes = Vec::new();
            for _ in 2..count {
                sizes.push(cursor.read_u64::<LittleEndian>().unwrap());
            }
            self.sizes = Some((main_size, sizes));
            let src = cursor.into_inner();
            src.advance(header_size as usize);
            src
        } else {
            src
        };

        let (main_size, sizes) = self.sizes.as_ref().unwrap();
        if self.main_message.is_none() {
            let size = src.len() as u64;
            if *main_size > size {
                return Ok(None);
            }
            self.main_message = Some(src.split_to(*main_size as usize).freeze());
        }

        for i in self.other_messages.len()..sizes.len() {
            let size = src.len() as u64;
            let frame_size = sizes[i];
            if frame_size > size {
                return Ok(None);
            }
            self.other_messages
                .push(src.split_to(frame_size as usize).freeze());
        }
        self.sizes = None;
        Ok(Some(DaskMessage {
            message: self.main_message.take().unwrap(),
            additional_frames: std::mem::replace(&mut self.other_messages, Vec::new()),
        }))
    }
}

impl Encoder for DaskCodec {
    type Item = DaskMessage;
    type Error = crate::DsError;

    fn encode(&mut self, data: DaskMessage, dst: &mut BytesMut) -> crate::Result<()> {
        let frames = 2 + data.additional_frames.len();

        let n = 8 * (frames + 1)
            + data.message.len()
            + data.additional_frames.iter().map(|i| i.len()).sum::<usize>();
        dst.reserve(n);
        dst.put_u64_le(frames as u64);
        dst.put_u64_le(0);
        dst.put_u64_le(data.message.len() as u64);
        for frame in &data.additional_frames {
            dst.put_u64_le(frame.len() as u64);
        }
        dst.extend_from_slice(&data.message);
        for frame in &data.additional_frames {
            dst.extend_from_slice(&frame);
        }
        Ok(())
    }
}
