use bytes::BufMut;

use crate::key::{Key, KeySlice, KeyVec};

use super::{Block, SIZE_OF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: Key::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if key.is_empty() {
            panic!("Cannot add value with empty key");
        }
        let data_size = self.data.len();
        let offsets_size = SIZE_OF_U16 * self.offsets.len() + SIZE_OF_U16;
        let current_block_size = data_size + offsets_size;
        let entry_size = key.len() + value.len() + SIZE_OF_U16 * 3;
        if current_block_size + entry_size > self.block_size && !self.is_empty() {
            return false;
        }

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        self.offsets.push(self.data.len() as u16);

        let key_length = key.len() as u16;
        let value_length = value.len() as u16;

        self.data.put_u16(key_length);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16(value_length);
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
