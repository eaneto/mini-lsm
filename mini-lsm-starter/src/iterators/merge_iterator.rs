#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return MergeIterator {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        if iters.iter().all(|iter| !iter.is_valid()) {
            let mut iters = iters;
            return MergeIterator {
                iters: heap,
                current: Some(HeapWrapper(
                    0,
                    iters
                        .pop()
                        .expect("Iterators should have at least on element"),
                )),
            };
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap
            .pop()
            .expect("The heap should have at least on element by this point");
        MergeIterator {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .expect("Current item should be present")
            .1
            .key()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("Current item should be present")
            .1
            .value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|wrapper| wrapper.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self
            .current
            .as_mut()
            .expect("Current value should be defined in the iterator");

        while let Some(mut iter) = self.iters.peek_mut() {
            if iter.1.key() == current.1.key() {
                if let Err(e) = iter.1.next() {
                    PeekMut::pop(iter);
                    return Err(e);
                }

                if !iter.1.is_valid() {
                    PeekMut::pop(iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        if let Some(mut iter) = self.iters.peek_mut() {
            if *current < *iter {
                std::mem::swap(&mut *iter, current);
            }
        }

        Ok(())
    }
}
