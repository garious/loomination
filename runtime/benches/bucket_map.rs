#![feature(test)]

extern crate test;
use rayon::prelude::*;
use solana_runtime::accounts_db::AccountInfo;
use solana_runtime::bucket_map::BucketMap;
use solana_sdk::pubkey::Pubkey;
use std::collections::hash_map::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use test::Bencher;

#[bench]
fn bucket_map_bench_hashmap_baseline(bencher: &mut Bencher) {
    let mut index = HashMap::new();
    bencher.iter(|| {
        let key = Pubkey::new_unique();
        index.insert(key, vec![(0, AccountInfo::default())]);
    });
}

#[bench]
fn bucket_map_bench_insert_1(bencher: &mut Bencher) {
    let tmpdir = std::env::temp_dir().join("bucket_map_bench_insert_1");
    std::fs::create_dir_all(tmpdir.clone()).unwrap();
    let drives = Arc::new(vec![tmpdir.clone()]);
    let index = BucketMap::new(1, drives);
    bencher.iter(|| {
        let key = Pubkey::new_unique();
        index.update(&key, |_| Some(vec![(0, AccountInfo::default())]));
    });
    std::fs::remove_dir_all(tmpdir).unwrap();
}

#[bench]
fn bucket_map_bench_insert_16x32_baseline(bencher: &mut Bencher) {
    let index = RwLock::new(HashMap::new());
    (0..16).into_iter().into_par_iter().for_each(|_| {
        let key = Pubkey::new_unique();
        index
            .write()
            .unwrap()
            .insert(key, vec![(0, AccountInfo::default())]);
    });
    bencher.iter(|| {
        (0..16).into_iter().into_par_iter().for_each(|_| {
            for _ in 0..32 {
                let key = Pubkey::new_unique();
                index
                    .write()
                    .unwrap()
                    .insert(key, vec![(0, AccountInfo::default())]);
            }
        })
    });
}

#[bench]
fn bucket_map_bench_insert_16x32(bencher: &mut Bencher) {
    let tmpdir = std::env::temp_dir().join("bucket_map_bench_insert_16x32");
    std::fs::create_dir_all(tmpdir.clone()).unwrap();
    let drives = Arc::new(vec![tmpdir.clone()]);
    let index = BucketMap::new(4, drives);
    (0..16).into_iter().into_par_iter().for_each(|_| {
        let key = Pubkey::new_unique();
        index.update(&key, |_| Some(vec![(0, AccountInfo::default())]));
    });
    bencher.iter(|| {
        (0..16).into_iter().into_par_iter().for_each(|_| {
            for _ in 0..32 {
                let key = Pubkey::new_unique();
                index.update(&key, |_| Some(vec![(0, AccountInfo::default())]));
            }
        })
    });
    std::fs::remove_dir_all(tmpdir).unwrap();
}

//#[bench]
//fn bucket_map_bench_insert_32x32(bencher: &mut Bencher) {
//    let tmpdir1 = std::env::temp_dir().join("bucket_map_bench_insert_32x32");
//    let tmpdir2 = PathBuf::from("/mnt/data/aeyakovenko").join("bucket_map_bench_insert_32x32");
//    let drives = Arc::new(vec![tmpdir1, tmpdir2]);
//    for tmpdir in drives.iter() {
//        std::fs::create_dir_all(tmpdir.clone()).unwrap();
//    }
//    let index = BucketMap::new(8, drives.clone());
//    (0..32).into_iter().into_par_iter().for_each(|_| {
//        let key = Pubkey::new_unique();
//        index.update(&key, |_| Some(vec![(0, AccountInfo::default())]));
//    });
//    bencher.iter(|| {
//        (0..32).into_iter().into_par_iter().for_each(|_| {
//            for _ in 0..32 {
//                let key = Pubkey::new_unique();
//                index.update(&key, |_| Some(vec![(0, AccountInfo::default())]));
//            }
//        })
//    });
//    for tmpdir in drives.iter() {
//        std::fs::remove_dir_all(tmpdir).unwrap();
//    }
//}
