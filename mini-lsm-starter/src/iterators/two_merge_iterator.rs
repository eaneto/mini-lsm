use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    pick_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iterator = TwoMergeIterator { a, b, pick_a: true };
        iterator.skip_b()?;
        iterator.pick_a = TwoMergeIterator::should_choose_a(&iterator.a, &iterator.b);
        Ok(iterator)
    }

    fn skip_b(&mut self) -> Result<()> {
        let should_skip_b = self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key();
        if should_skip_b {
            self.b.next()?
        }
        Ok(())
    }

    fn should_choose_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            return false;
        }
        if !b.is_valid() {
            return true;
        }
        a.key() < b.key()
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.pick_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.pick_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.pick_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.pick_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }

        self.skip_b()?;

        self.pick_a = TwoMergeIterator::should_choose_a(&self.a, &self.b);

        Ok(())
    }
}
