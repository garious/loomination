use crate::blockstore::*;
use solana_sdk::{clock::Slot, hash::Hash};

pub struct AncestorIterator<'a> {
    current: Option<Slot>,
    blockstore: &'a Blockstore,
}

impl<'a> AncestorIterator<'a> {
    pub fn new(start_slot: Slot, blockstore: &'a Blockstore) -> Self {
        let current = blockstore.meta(start_slot).unwrap().and_then(|slot_meta| {
            if slot_meta.is_parent_set() && start_slot != 0 {
                Some(slot_meta.parent_slot)
            } else {
                None
            }
        });
        Self {
            current,
            blockstore,
        }
    }

    pub fn new_inclusive(start_slot: Slot, blockstore: &'a Blockstore) -> Self {
        Self {
            current: blockstore.meta(start_slot).unwrap().map(|_| start_slot),
            blockstore,
        }
    }
}
impl<'a> Iterator for AncestorIterator<'a> {
    type Item = Slot;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current;
        current.map(|slot| {
            if slot != 0 {
                self.current = self.blockstore.meta(slot).unwrap().and_then(|slot_meta| {
                    if slot_meta.is_parent_set() {
                        Some(slot_meta.parent_slot)
                    } else {
                        None
                    }
                });
            } else {
                self.current = None;
            }
            slot
        })
    }
}

pub struct AncestorIteratorWithHash<'a> {
    ancestor_iterator: AncestorIterator<'a>,
}
impl<'a> From<AncestorIterator<'a>> for AncestorIteratorWithHash<'a> {
    fn from(ancestor_iterator: AncestorIterator<'a>) -> Self {
        Self { ancestor_iterator }
    }
}
impl<'a> Iterator for AncestorIteratorWithHash<'a> {
    type Item = (Slot, Hash);
    fn next(&mut self) -> Option<Self::Item> {
        self.ancestor_iterator
            .next()
            .and_then(|next_ancestor_slot| {
                self.ancestor_iterator
                    .blockstore
                    .get_bank_hash(next_ancestor_slot)
                    .map(|next_ancestor_hash| (next_ancestor_slot, next_ancestor_hash))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::hash::Hash;
    use std::{collections::HashMap, path::Path};
    use trees::tr;

    fn setup_forks(blockstore_path: &Path) -> Blockstore {
        let blockstore = Blockstore::open(blockstore_path).unwrap();
        /*
            Build fork structure:

                slot 0
                    |
                slot 1
                /    \
            slot 2    |
                |       |
            slot 3    |
                        |
                        |
                    slot 4
        */
        let tree = tr(0) / (tr(1) / (tr(2) / (tr(3))) / (tr(4)));
        blockstore.add_tree(tree, true, true, 2, Hash::default());
        blockstore
    }

    #[test]
    fn test_ancestor_iterator() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = setup_forks(&blockstore_path);

            // Test correctness
            assert!(AncestorIterator::new(0, &blockstore).next().is_none());
            assert_eq!(
                AncestorIterator::new(4, &blockstore).collect::<Vec<Slot>>(),
                vec![1, 0]
            );
            assert_eq!(
                AncestorIterator::new(3, &blockstore).collect::<Vec<Slot>>(),
                vec![2, 1, 0]
            );
        }
        Blockstore::destroy(&blockstore_path).unwrap();
    }

    #[test]
    fn test_ancestor_iterator_inclusive() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();

        let (shreds, _) = make_slot_entries(0, 0, 42);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        let (shreds, _) = make_slot_entries(1, 0, 42);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        let (shreds, _) = make_slot_entries(2, 1, 42);
        blockstore.insert_shreds(shreds, None, false).unwrap();

        assert_eq!(
            AncestorIterator::new(2, &blockstore).collect::<Vec<Slot>>(),
            vec![1, 0]
        );
        // existing start_slot
        assert_eq!(
            AncestorIterator::new_inclusive(2, &blockstore).collect::<Vec<Slot>>(),
            vec![2, 1, 0]
        );

        // non-existing start_slot
        assert_eq!(
            AncestorIterator::new_inclusive(3, &blockstore).collect::<Vec<Slot>>(),
            vec![] as Vec<Slot>
        );
    }

    #[test]
    fn test_ancestor_iterator_with_hash() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = setup_forks(&blockstore_path);

            // Insert frozen hashes
            let mut slot_to_bank_hash = HashMap::new();
            for slot in 0..=4 {
                let bank_hash = Hash::new_unique();
                slot_to_bank_hash.insert(slot, bank_hash);
                blockstore.insert_bank_hash(slot, bank_hash, false);
            }

            // Test correctness
            assert!(
                AncestorIteratorWithHash::from(AncestorIterator::new(0, &blockstore))
                    .next()
                    .is_none()
            );
            assert_eq!(
                AncestorIteratorWithHash::from(AncestorIterator::new(4, &blockstore))
                    .collect::<Vec<(Slot, Hash)>>(),
                vec![(1, slot_to_bank_hash[&1]), (0, slot_to_bank_hash[&0])]
            );
            assert_eq!(
                AncestorIteratorWithHash::from(AncestorIterator::new(3, &blockstore))
                    .collect::<Vec<(Slot, Hash)>>(),
                vec![
                    (2, slot_to_bank_hash[&2]),
                    (1, slot_to_bank_hash[&1]),
                    (0, slot_to_bank_hash[&0])
                ]
            );
        }
        Blockstore::destroy(&blockstore_path).unwrap();
    }
}
