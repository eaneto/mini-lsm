use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block(1)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(block);
        Ok(SsTableIterator {
            table,
            blk_iter: block_iterator,
            blk_idx: 1,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block(1)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        self.blk_idx = 1;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut table_iterator = SsTableIterator::create_and_seek_to_first(table)?;
        table_iterator.seek_to_key(key)?;
        Ok(table_iterator)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        self.blk_idx = 1;
        let block = self.table.read_block(self.blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        while !self.blk_iter.is_valid() && self.blk_idx < self.table.block_meta.len() {
            self.blk_idx += 1;
            let block = self.table.read_block(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        }

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() && self.blk_idx < self.table.block_meta.len() {
            println!("Invalid block");
            self.blk_idx += 1;
            println!("updated block id {}", self.blk_idx);
            let block = self.table.read_block(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }
}
