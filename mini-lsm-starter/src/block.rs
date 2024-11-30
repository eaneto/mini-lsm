#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

pub const SIZE_OF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        bytes.put_slice(&self.data);
        for offset in &self.offsets {
            bytes.put_u16(*offset);
        }

        bytes.put_u16(self.offsets.len() as u16);

        bytes.copy_to_bytes(bytes.len())
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let offsets_length = (&data[data.len() - SIZE_OF_U16..]).get_u16() as usize;

        let offsets = &data
            [(data.len() - offsets_length * SIZE_OF_U16 - SIZE_OF_U16)..(data.len() - SIZE_OF_U16)];

        let offsets = offsets
            .chunks(SIZE_OF_U16)
            .map(|mut chunk| chunk.get_u16())
            .collect();

        let data_slice = &data[0..(data.len() - offsets_length * SIZE_OF_U16 - SIZE_OF_U16)];

        Block {
            data: data_slice.to_vec(),
            offsets,
        }
    }
}
