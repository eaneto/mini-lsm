use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZE_OF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let key_length = &block.data[0..SIZE_OF_U16];
        let key_length = u16::from_be_bytes(key_length.try_into().unwrap()) as usize;

        let first_key =
            KeyVec::from_vec(block.data[SIZE_OF_U16..(key_length + SIZE_OF_U16)].to_vec());

        let value_length = &block.data[(key_length + SIZE_OF_U16)..(key_length + SIZE_OF_U16 * 2)];
        let value_length = u16::from_be_bytes(value_length.try_into().unwrap()) as usize;

        BlockIterator {
            block,
            key: first_key.clone(),
            value_range: (
                key_length + SIZE_OF_U16 * 2,
                key_length + SIZE_OF_U16 * 2 + value_length,
            ),
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let key_length = &block.data[0..SIZE_OF_U16];
        let key_length = u16::from_be_bytes(key_length.try_into().unwrap()) as usize;

        let first_key =
            KeyVec::from_vec(block.data[SIZE_OF_U16..(key_length + SIZE_OF_U16)].to_vec());

        let value_length = &block.data[(key_length + SIZE_OF_U16)..(key_length + SIZE_OF_U16 * 2)];
        let value_length = u16::from_be_bytes(value_length.try_into().unwrap()) as usize;

        let mut idx = 1;
        let mut current_key = first_key.clone();
        let mut current_key_position = key_length;
        let mut current_value_length = value_length;

        while first_key.as_key_slice() < key {
            let key_length = &block.data[(current_key_position + SIZE_OF_U16 * 2)..SIZE_OF_U16];
            current_key_position = u16::from_be_bytes(key_length.try_into().unwrap()) as usize;

            current_key = KeyVec::from_vec(
                block.data[SIZE_OF_U16..(current_key_position + SIZE_OF_U16)].to_vec(),
            );

            let value_length = &block.data
                [(current_key_position + SIZE_OF_U16)..(current_key_position + SIZE_OF_U16 * 2)];
            current_value_length = u16::from_be_bytes(value_length.try_into().unwrap()) as usize;

            idx += 1;
        }

        BlockIterator {
            block,
            key: current_key,
            value_range: (
                current_key_position + SIZE_OF_U16 * 2,
                current_key_position + SIZE_OF_U16 * 2 + current_value_length,
            ),
            idx: idx - 1,
            first_key,
        }
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let key_length = &self.block.data[0..SIZE_OF_U16];
        let key_length = u16::from_be_bytes(key_length.try_into().unwrap()) as usize;

        self.key =
            KeyVec::from_vec(self.block.data[SIZE_OF_U16..(key_length + SIZE_OF_U16)].to_vec());

        let value_length =
            &self.block.data[(key_length + SIZE_OF_U16)..(key_length + SIZE_OF_U16 * 2)];
        let value_length = u16::from_be_bytes(value_length.try_into().unwrap()) as usize;

        self.value_range = (
            key_length + SIZE_OF_U16 * 2,
            key_length + SIZE_OF_U16 * 2 + value_length,
        );

        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }

        if self.at_last_key() {
            self.key = KeyVec::new();
            return;
        }

        let key_length = &self.block.data[self.value_range.1..(self.value_range.1 + SIZE_OF_U16)];

        let key_length = u16::from_be_bytes(key_length.try_into().unwrap()) as usize;

        self.key = KeyVec::from_vec(
            self.block.data[(self.value_range.1 + SIZE_OF_U16)
                ..(self.value_range.1 + SIZE_OF_U16 + key_length)]
                .to_vec(),
        );

        let value_length = &self.block.data[(self.value_range.1 + SIZE_OF_U16 + key_length)
            ..(self.value_range.1 + SIZE_OF_U16 * 2 + key_length)];

        let value_length = u16::from_be_bytes(value_length.try_into().unwrap()) as usize;

        self.value_range = (
            self.value_range.1 + SIZE_OF_U16 * 2 + key_length,
            self.value_range.1 + SIZE_OF_U16 * 2 + key_length + value_length,
        );
        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && self.key.as_key_slice() < key {
            self.next();
        }
    }

    fn at_last_key(&self) -> bool {
        self.idx == self.block.offsets.len() - 1
    }
}
