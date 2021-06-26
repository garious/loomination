//! BucketMap is a mostly contention free concurrent map backed by MmapMut

use crate::accounts_db::AccountInfo;
use crate::data_bucket::DataBucket;
use log::*;
use rand::thread_rng;
use rand::Rng;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::slot_history::Slot;
use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

pub type SlotInfo = (Slot, AccountInfo);
pub type SlotSlice = [SlotInfo];

pub struct BucketMap {
    buckets: Vec<RwLock<Option<Bucket>>>,
    drives: Arc<Vec<PathBuf>>,
    bits: u64,
}

#[derive(Debug)]
pub enum BucketMapError {
    DataNoSpace((u64, u8)),
    IndexNoSpace(u8),
}

impl BucketMap {
    pub fn new(num_buckets_pow2: u64, drives: Arc<Vec<PathBuf>>) -> Self {
        let mut buckets = Vec::with_capacity(1 << num_buckets_pow2);
        buckets.resize_with(1 << num_buckets_pow2, || RwLock::new(None));
        Self {
            buckets,
            drives,
            bits: num_buckets_pow2,
        }
    }
    pub fn read_value(&self, key: &Pubkey) -> Option<Vec<SlotInfo>> {
        let ix = self.bucket_ix(key);
        self.buckets[ix].read().unwrap().as_ref().and_then(|x| {
            let slice = x.read_value(key)?;
            let mut vec = Vec::with_capacity(slice.len());
            vec.extend_from_slice(slice);
            Some(vec)
        })
    }

    pub fn delete_key(&self, key: &Pubkey) {
        let ix = self.bucket_ix(key);
        let mut bucket = self.buckets[ix].write().unwrap();
        if !bucket.is_none() {
            bucket.as_mut().unwrap().delete_key(key);
        }
    }

    pub fn update<F>(&self, key: &Pubkey, updatefn: F) -> ()
    where
        F: Fn(Option<&[SlotInfo]>) -> Option<Vec<SlotInfo>>,
    {
        let ix = self.bucket_ix(key);
        let mut bucket = self.buckets[ix].write().unwrap();
        if bucket.is_none() {
            *bucket = Some(Bucket::new(self.drives.clone()));
        }
        let bucket = bucket.as_mut().unwrap();
        let current = bucket.read_value(key);
        let new = updatefn(current);
        if new.is_none() {
            bucket.delete_key(key);
            return;
        }
        let new = new.unwrap();
        loop {
            let rv = bucket.try_write(key, &new);
            match rv {
                Err(BucketMapError::DataNoSpace(sz)) => {
                    debug!("GROWING SPACE {:?}", sz);
                    bucket.grow_data(sz);
                    continue;
                }
                Err(BucketMapError::IndexNoSpace(sz)) => {
                    debug!("GROWING INDEX {}", sz);
                    bucket.grow_index(sz);
                    continue;
                }
                Ok(()) => return,
            }
        }
    }
    fn bucket_ix(&self, key: &Pubkey) -> usize {
        let location = read_be_u64(key.as_ref());
        (location >> (64 - self.bits)) as usize
    }
}

struct Bucket {
    drives: Arc<Vec<PathBuf>>,
    //index
    index: DataBucket,
    //random offset for the index
    random: u64,
    //data buckets to store SlotSlice up to a power of 2 in len
    data: Vec<DataBucket>,
}

#[repr(C)]
#[derive(Debug, PartialEq, Clone, Copy)]
struct IndexEntry {
    key: Pubkey,
    data_bucket: u64,
    data_location: u64,
    //if the bucket doubled, the index can be recomputed
    create_bucket_capacity: u8,
    num_slots: u64,
}

impl IndexEntry {
    fn data_loc(&self, bucket: &DataBucket) -> u64 {
        self.data_location << (bucket.capacity - self.create_bucket_capacity)
    }
}

const MAX_SEARCH: u64 = 16;

impl Bucket {
    fn new(drives: Arc<Vec<PathBuf>>) -> Self {
        let index = DataBucket::new(drives.clone(), 1, std::mem::size_of::<IndexEntry>() as u64);
        Self {
            random: thread_rng().gen(),
            drives,
            index,
            data: vec![],
        }
    }

    fn find_entry(&self, key: &Pubkey) -> Option<(&IndexEntry, u64)> {
        Self::bucket_find_entry(&self.index, key, self.random)
    }

    fn bucket_find_entry<'a>(
        index: &'a DataBucket,
        key: &Pubkey,
        random: u64,
    ) -> Option<(&'a IndexEntry, u64)> {
        let ix = Self::bucket_index_ix(index, key, random);
        for i in ix..ix + MAX_SEARCH {
            let ii = i % index.capacity();
            if index.uid(ii) == 0 {
                continue;
            }
            let elem: &IndexEntry = index.get(ii);
            if elem.key == *key {
                return Some((elem, ii));
            }
        }
        None
    }

    fn bucket_create_key(
        index: &DataBucket,
        key: &Pubkey,
        elem_uid: u64,
        random: u64,
    ) -> Result<u64, BucketMapError> {
        let ix = Self::bucket_index_ix(index, key, random);
        for i in ix..ix + MAX_SEARCH {
            let ii = i as u64 % index.capacity();
            if index.uid(ii) != 0 {
                continue;
            }
            index.allocate(ii, elem_uid).unwrap();
            let mut elem: &mut IndexEntry = index.get_mut(ii);
            elem.key = *key;
            elem.data_bucket = 0;
            elem.data_location = 0;
            elem.create_bucket_capacity = 0;
            elem.num_slots = 0;
            debug!(
                "INDEX ALLOC {:?} {} {} {}",
                key, ii, index.capacity, elem_uid
            );
            return Ok(ii);
        }
        Err(BucketMapError::IndexNoSpace(index.capacity))
    }

    fn create_key(&self, key: &Pubkey) -> Result<u64, BucketMapError> {
        Self::bucket_create_key(&self.index, key, Self::key_uid(key), self.random)
    }

    pub fn read_value(&self, key: &Pubkey) -> Option<&[SlotInfo]> {
        debug!("READ_VALUE: {:?}", key);
        let (elem, ix) = self.find_entry(key)?;
        let uid = self.index.uid(ix);
        let data_bucket = &self.data[elem.data_bucket as usize];
        let loc = elem.data_loc(data_bucket);
        debug!("READ_VALUE: {:?} {} {}", key, ix, loc);
        assert_eq!(uid, self.data[elem.data_bucket as usize].uid(loc));
        let slice = self.data[elem.data_bucket as usize].get_cell_slice(loc, elem.num_slots);
        Some(slice)
    }

    fn try_write(&mut self, key: &Pubkey, data: &SlotSlice) -> Result<(), BucketMapError> {
        let index_entry = self.find_entry(key);
        let (elem, elem_ix) = if index_entry.is_none() {
            let ii = self.create_key(key)?;
            let elem = self.index.get(ii);
            (elem, ii)
        } else {
            index_entry.unwrap()
        };
        let elem_uid = self.index.uid(elem_ix);
        let best_fit_bucket = Self::best_fit(data.len() as u64);
        if self.data.get(best_fit_bucket as usize).is_none() {
            return Err(BucketMapError::DataNoSpace((best_fit_bucket, 0)));
        }
        let current_bucket = &self.data[elem.data_bucket as usize];
        if best_fit_bucket == elem.data_bucket && elem.num_slots > 0 {
            //in place update
            let elem_loc = elem.data_loc(current_bucket);
            let slice: &mut [SlotInfo] =
                current_bucket.get_mut_cell_slice(elem_loc, data.len() as u64);
            let elem: &mut IndexEntry = self.index.get_mut(elem_ix);
            assert!(current_bucket.uid(elem_loc) == elem_uid);
            elem.num_slots = data.len() as u64;
            slice.clone_from_slice(data);
            return Ok(());
        } else {
            //need to move the allocation to a best fit spot
            let best_bucket = &self.data[best_fit_bucket as usize];
            let cap_power = best_bucket.capacity;
            let cap = best_bucket.capacity();
            let pos = thread_rng().gen_range(0, cap);
            for i in pos..pos + MAX_SEARCH as u64 {
                let ix = i % cap;
                if best_bucket.uid(ix) == 0 {
                    let elem_loc = elem.data_loc(current_bucket);
                    if elem.num_slots > 0 {
                        current_bucket.free(elem_loc, elem_uid).unwrap();
                    }
                    let elem: &mut IndexEntry = self.index.get_mut(elem_ix);
                    elem.data_bucket = best_fit_bucket;
                    elem.data_location = ix;
                    elem.create_bucket_capacity = best_bucket.capacity;
                    elem.num_slots = data.len() as u64;
                    debug!(
                        "DATA ALLOC {:?} {} {} {}",
                        key, elem.data_location, best_bucket.capacity, elem_uid
                    );
                    if elem.num_slots > 0 {
                        best_bucket.allocate(ix, elem_uid).unwrap();
                        let slice = best_bucket.get_mut_cell_slice(ix, data.len() as u64);
                        slice.clone_from_slice(data);
                    }
                    return Ok(());
                }
            }
            Err(BucketMapError::DataNoSpace((best_fit_bucket, cap_power)))
        }
    }

    fn delete_key(&mut self, key: &Pubkey) {
        if let Some((elem, elem_ix)) = self.find_entry(key) {
            let elem_uid = self.index.uid(elem_ix);
            if elem.num_slots > 0 {
                let data_bucket = &self.data[elem.data_bucket as usize];
                let loc = elem.data_loc(data_bucket);
                debug!(
                    "DATA FREE {:?} {} {} {}",
                    key, elem.data_location, data_bucket.capacity, elem_uid
                );
                data_bucket.free(loc, elem_uid).unwrap();
            }
            debug!("INDEX FREE {:?} {}", key, elem_uid);
            self.index.free(elem_ix, elem_uid).unwrap();
        }
    }

    fn grow_index(&mut self, sz: u8) {
        if self.index.capacity == sz {
            debug!("GROW_INDEX: {}", sz);
            for i in 1.. {
                //increasing the capacity by ^4 reduces the
                //likelyhood of a re-index collision of 2^(max_search)^2
                //1 in 2^32
                let index = DataBucket::new_with_capacity(
                    self.drives.clone(),
                    1,
                    std::mem::size_of::<IndexEntry>() as u64,
                    self.index.capacity + i * 2,
                );
                let random = thread_rng().gen();
                let rvs: Vec<bool> = (0..self.index.capacity())
                    .into_iter()
                    .map(|ix| {
                        if 0 != self.index.uid(ix) {
                            let elem: &IndexEntry = self.index.get(ix);
                            let uid = self.index.uid(ix);
                            let new_ix = Self::bucket_create_key(&index, &elem.key, uid, random);
                            if new_ix.is_err() {
                                return false;
                            }
                            let new_ix = new_ix.unwrap();
                            let new_elem: &mut IndexEntry = index.get_mut(new_ix);
                            *new_elem = *elem;
                            let dbg_elem: IndexEntry = *new_elem;
                            assert_eq!(
                                Self::bucket_find_entry(&index, &elem.key, random).unwrap(),
                                (&dbg_elem, new_ix)
                            );
                        }
                        true
                    })
                    .collect();
                if rvs.into_iter().all(|x| x) {
                    self.index = index;
                    self.random = random;
                    break;
                }
            }
        }
    }

    fn best_fit(sz: u64) -> u64 {
        (sz as f64).log2().ceil() as u64
    }

    fn grow_data(&mut self, sz: (u64, u8)) {
        if self.data.get(sz.0 as usize).is_none() {
            for i in self.data.len() as u64..(sz.0 + 1) {
                self.data.push(DataBucket::new(
                    self.drives.clone(),
                    1 << i,
                    std::mem::size_of::<SlotInfo>() as u64,
                ))
            }
        }
        if self.data[sz.0 as usize].capacity == sz.1 {
            debug!("GROW_DATA: {} {}", sz.0, sz.1);
            self.data[sz.0 as usize].grow();
        }
    }

    fn bucket_index_ix(index: &DataBucket, key: &Pubkey, random: u64) -> u64 {
        let uid = Self::key_uid(key);
        let location = uid.overflowing_add(random).0 % index.capacity();
        debug!(
            "INDEX_IX: {:?} uid:{} loc: {} cap:{}",
            key,
            uid,
            location,
            index.capacity()
        );
        location
    }

    fn key_uid(key: &Pubkey) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish().max(1u64)
    }
}

fn read_be_u64(input: &[u8]) -> u64 {
    assert!(input.len() >= std::mem::size_of::<u64>());
    u64::from_be_bytes(input[0..std::mem::size_of::<u64>()].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_map_test_insert() {
        let key = Pubkey::new_unique();
        let tmpdir = std::env::temp_dir().join("bucket_map_test_insert");
        std::fs::create_dir_all(tmpdir.clone()).unwrap();
        let drives = Arc::new(vec![tmpdir.clone()]);
        let index = BucketMap::new(1, drives);
        index.update(&key, |_| Some(vec![(0, AccountInfo::default())]));
        assert_eq!(
            index.read_value(&key),
            Some(vec![(0, AccountInfo::default())])
        );
        std::fs::remove_dir_all(tmpdir).unwrap();
    }

    #[test]
    fn bucket_map_test_update() {
        let key = Pubkey::new_unique();
        let tmpdir = std::env::temp_dir().join("bucket_map_test_update");
        std::fs::create_dir_all(tmpdir.clone()).unwrap();
        let drives = Arc::new(vec![tmpdir.clone()]);
        let index = BucketMap::new(1, drives);
        index.update(&key, |_| Some(vec![(0, AccountInfo::default())]));
        assert_eq!(
            index.read_value(&key),
            Some(vec![(0, AccountInfo::default())])
        );
        index.update(&key, |_| Some(vec![(1, AccountInfo::default())]));
        assert_eq!(
            index.read_value(&key),
            Some(vec![(1, AccountInfo::default())])
        );
        std::fs::remove_dir_all(tmpdir).unwrap();
    }

    #[test]
    fn bucket_map_test_delete() {
        let tmpdir = std::env::temp_dir().join("bucket_map_test_delete");
        std::fs::create_dir_all(tmpdir.clone()).unwrap();
        let drives = Arc::new(vec![tmpdir.clone()]);
        let index = BucketMap::new(1, drives);
        for i in 0..10 {
            let key = Pubkey::new_unique();
            assert_eq!(index.read_value(&key), None);

            index.update(&key, |_| Some(vec![(i, AccountInfo::default())]));
            assert_eq!(
                index.read_value(&key),
                Some(vec![(i, AccountInfo::default())])
            );

            index.delete_key(&key);
            assert_eq!(index.read_value(&key), None);

            index.update(&key, |_| Some(vec![(i, AccountInfo::default())]));
            assert_eq!(
                index.read_value(&key),
                Some(vec![(i, AccountInfo::default())])
            );
            index.delete_key(&key);
        }
        std::fs::remove_dir_all(tmpdir).unwrap();
    }

    #[test]
    fn bucket_map_test_delete_2() {
        let tmpdir = std::env::temp_dir().join("bucket_map_test_delete_2");
        std::fs::create_dir_all(tmpdir.clone()).unwrap();
        let drives = Arc::new(vec![tmpdir.clone()]);
        let index = BucketMap::new(2, drives);
        for i in 0..100 {
            let key = Pubkey::new_unique();
            assert_eq!(index.read_value(&key), None);

            index.update(&key, |_| Some(vec![(i, AccountInfo::default())]));
            assert_eq!(
                index.read_value(&key),
                Some(vec![(i, AccountInfo::default())])
            );

            index.delete_key(&key);
            assert_eq!(index.read_value(&key), None);

            index.update(&key, |_| Some(vec![(i, AccountInfo::default())]));
            assert_eq!(
                index.read_value(&key),
                Some(vec![(i, AccountInfo::default())])
            );
            index.delete_key(&key);
        }
        std::fs::remove_dir_all(tmpdir).unwrap();
    }

    #[test]
    fn bucket_map_test_n_drives() {
        let tmpdir = std::env::temp_dir().join("bucket_map_test_n_drives");
        std::fs::create_dir_all(tmpdir.clone()).unwrap();
        let drives = Arc::new(vec![tmpdir.clone()]);
        let index = BucketMap::new(2, drives);
        for i in 0..100 {
            let key = Pubkey::new_unique();
            index.update(&key, |_| Some(vec![(i, AccountInfo::default())]));
            assert_eq!(
                index.read_value(&key),
                Some(vec![(i, AccountInfo::default())])
            );
        }
        std::fs::remove_dir_all(tmpdir).unwrap();
    }
    #[test]
    fn bucket_map_test_grow_read() {
        let tmpdir = std::env::temp_dir().join("bucket_map_test_grow_read");
        std::fs::create_dir_all(tmpdir.clone()).unwrap();
        let drives = Arc::new(vec![tmpdir.clone()]);
        let index = BucketMap::new(2, drives);
        let keys: Vec<Pubkey> = (0..100).into_iter().map(|_| Pubkey::new_unique()).collect();
        for k in 0..keys.len() {
            let key = &keys[k];
            let i = read_be_u64(key.as_ref());
            index.update(&key, |_| Some(vec![(i, AccountInfo::default())]));
            assert_eq!(
                index.read_value(&key),
                Some(vec![(i, AccountInfo::default())])
            );
            for j in 0..k {
                let key = &keys[j];
                let i = read_be_u64(key.as_ref());
                debug!("READ: {:?} {}", key, i);
                assert_eq!(
                    index.read_value(&key),
                    Some(vec![(i, AccountInfo::default())])
                );
            }
        }
        std::fs::remove_dir_all(tmpdir).unwrap();
    }

    #[test]
    fn bucket_map_test_n_delete() {
        let tmpdir = std::env::temp_dir().join("bucket_map_test_n_delete");
        std::fs::create_dir_all(tmpdir.clone()).unwrap();
        let drives = Arc::new(vec![tmpdir.clone()]);
        let index = BucketMap::new(2, drives);
        let keys: Vec<Pubkey> = (0..20).into_iter().map(|_| Pubkey::new_unique()).collect();
        for key in keys.iter() {
            let i = read_be_u64(key.as_ref());
            index.update(&key, |_| Some(vec![(i, AccountInfo::default())]));
            assert_eq!(
                index.read_value(&key),
                Some(vec![(i, AccountInfo::default())])
            );
        }
        for key in keys.iter() {
            let i = read_be_u64(key.as_ref());
            debug!("READ: {:?} {}", key, i);
            assert_eq!(
                index.read_value(&key),
                Some(vec![(i, AccountInfo::default())])
            );
        }
        for k in 0..keys.len() {
            let key = &keys[k];
            index.delete_key(&key);
            assert_eq!(index.read_value(&key), None);
            for k in (k + 1)..keys.len() {
                let key = &keys[k];
                let i = read_be_u64(key.as_ref());
                assert_eq!(
                    index.read_value(&key),
                    Some(vec![(i, AccountInfo::default())])
                );
            }
        }
        std::fs::remove_dir_all(tmpdir).unwrap();
    }
}
