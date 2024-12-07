use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let key_bytes = key.to_key_vec().into_inner();
        if self.first_key.is_empty() {
            self.first_key = key_bytes.clone();
        }

        if self.builder.add(key, value) {
            self.last_key = key_bytes.clone();
            return;
        }

        self.build_block();

        if !self.builder.add(key, value) {
            panic!("The new builder should be empty, so it should be possible to add new data");
        }
        self.first_key = key_bytes.clone();
        self.last_key = key_bytes.clone();
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.build_block();

        let mut table = Vec::new();
        let block_meta_offset = self.data.len();

        table.extend(self.data);
        BlockMeta::encode_block_meta(&self.meta, &mut table);
        table.put_u32(block_meta_offset as u32);

        let file = FileObject::create(path.as_ref(), table)?;

        Ok(SsTable {
            file,
            block_meta_offset,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

    fn build_block(&mut self) {
        let new_builder = BlockBuilder::new(self.block_size);
        let old_block_builder = std::mem::replace(&mut self.builder, new_builder);
        let encoded_block = old_block_builder.build().encode();

        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(std::mem::take(&mut self.first_key).into()),
            last_key: KeyBytes::from_bytes(std::mem::take(&mut self.last_key).into()),
        });

        self.data.extend(&encoded_block);
    }
}
