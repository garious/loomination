// Blockstore functions specific to the storage of shreds

// TODO: Think about limiting cache size; this comment should probably go with others in ledger_cleanup_service.rs
// - Shreds are at most 1228 bytes, use 1500 bytes for margin
// - 5k shreds/slot (50k TPS) * 1500 bytes = 7.5 MB / slot
// - 2 GB cache limit / 7.5 MB = 266 slots

use super::*;
use crate::shred::SHRED_PAYLOAD_SIZE;
use std::{fs, io::Read, io::Seek, io::SeekFrom, io::Write};
use std::time::{SystemTime, UNIX_EPOCH};

// TODO: revisit this value / correlate it to the cache size and/or flush interval
// Shreds are 1228 bytes, so WAL filesize will be at most 1024 * 512 * 1228 = 614 MB
pub const DEFAULT_MAX_WAL_SHREDS: usize = 1024 * 512;

// The WAL (Write Ahead Log) provides persistent backing for data that is written into
// cache. The WAL is used to recover data held in memory in (that hasn't been pushed)
// to disk in the event of process termination.
//
// The WAL will only be used to recover state at startup, or to record shred insertion
// in Blockstore::insert_shreds(). The former is a single threaded scenario; the
// latter is protected by write-lock, so we don't need to worry about any sync in here
pub struct WAL {
    // Directory where WAL file(s) will be stored
    wal_path: PathBuf,
    // The maximum number of shreds allowed in a single WAL file
    max_shreds: usize,
    // The number of shreds written to current WAL file
    cur_shreds: usize,
    // ID to current WAL file
    id: Option<u64>,
}

impl WAL {
    pub fn new(shred_db_path: &Path, max_shreds: usize) -> Result<WAL> {
        let wal_path = shred_db_path.join("wal");
        fs::create_dir_all(&wal_path)?;
        let wal = Self {
            wal_path,
            max_shreds,
            cur_shreds: 0,
            id: None,
        };
        Ok(wal)
    }

    // Recover shreds from log files at specified path
    fn recover(wal_path: &Path) -> Result<Vec<Shred>> {
        assert!(wal_path.is_dir());
        let mut buffers = vec![];
        let dir = fs::read_dir(wal_path)?;
        for log in dir {
            // Log filenames are the timestamp at which they're created
            let log = log?;
            // TODO: better error handling below line? We can probably fail
            // if there is some unknown file in this directory
            let id: u64 = log.file_name().to_str().unwrap().parse().unwrap();

            let path = Path::new(wal_path).join(id.to_string());
            let mut file = fs::File::open(path)?;
            loop {
                let mut buffer = vec![0; SHRED_PAYLOAD_SIZE];
                match file.read_exact(&mut buffer).ok() {
                    Some(_) => buffers.push(buffer),
                    None => break,
                };
            }
        }

        let shreds: ShredResult<Vec<_>> = buffers
            .into_iter()
            .map(move |shred| Shred::new_from_serialized_shred(shred))
            .collect();
        let shreds = shreds.map_err(|err| {
            BlockstoreError::InvalidShredData(Box::new(bincode::ErrorKind::Custom(format!(
                "Could not reconstruct shred from shred payload: {:?}",
                err
            ))))
        })?;
        Ok(shreds)
    }

    // Write the supplied shred payloads into the log
    pub fn write(&mut self, shreds: &HashMap<(u64, u64), Shred>) -> Result<()> {
        // Check if write would push WAL size over limit
        let mut file = if self.id.is_none() || self.cur_shreds + shreds.len() > self.max_shreds {
            self.id = Some(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs());
            self.cur_shreds = 0;
            let path = Path::new(&self.wal_path).join(self.id.unwrap().to_string());
            fs::File::create(path)
        } else {
            let path = Path::new(&self.wal_path).join(self.id.unwrap().to_string());
            fs::OpenOptions::new().append(true).open(path)
        }?;

        let result: Result<Vec<_>> = shreds
            .iter()
            .map(|((slot, index), shred)| {
                file.write_all(&shred.payload).map_err(|err| {
                    // TODO: slot / index possibly not relevant, also should we panic?
                    BlockstoreError::Io(IoError::new(
                        ErrorKind::Other,
                        format!(
                            "Unable to write shred (slot {}, index {}) to wal: {}",
                            slot, index, err
                        ),
                    ))
                })
            })
            .collect();
        // Check that all of the individual writes succeeded
        let _result = result?;
        self.cur_shreds += shreds.len();
        Ok(())
    }
}

impl Blockstore {
    pub(crate) fn get_data_shred_from_cache(
        &self,
        slot: Slot,
        index: u64,
    ) -> Result<Option<Vec<u8>>> {
        let payload = self.data_slot_cache(slot).and_then(|slot_cache| {
            slot_cache
                .read()
                .unwrap()
                .get(&index)
                .map(|shred_ref| shred_ref.clone())
        });
        Ok(payload)
    }

    pub(crate) fn get_data_shred_from_fs(&self, slot: Slot, index: u64) -> Result<Option<Vec<u8>>> {
        let path = self.slot_data_shreds_path(slot);
        Self::get_shred_from_fs(&path, index)
    }

    pub(crate) fn get_data_shreds_for_slot_from_cache(
        &self,
        slot: Slot,
        start_index: u64,
    ) -> Option<ShredResult<Vec<Shred>>> {
        // First, check if we have a cache for this slot
        let slot_cache = match self.data_slot_cache(slot) {
            Some(slot_cache) => slot_cache,
            None => return None,
        };
        // Cache exists, grab and hold read lock while we iterate through
        let slot_cache = slot_cache.read().unwrap();
        Some(
            slot_cache
                .iter()
                .filter_map(|(shred_index, shred)| {
                    // TODO: a quick search showed we exclusively use get_data_shreds_for_slot()
                    // with index = 0. As such, at the cost of flexibility to get partial slots
                    // (not sure of the use case for this), we could remove this index check
                    if shred_index >= &start_index {
                        Some(Shred::new_from_serialized_shred(shred.to_vec()))
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }

    pub(crate) fn get_data_shreds_for_slot_from_fs(
        &self,
        slot: Slot,
        start_index: u64,
    ) -> Option<ShredResult<Vec<Shred>>> {
        let path = self.slot_data_shreds_path(slot);
        Self::get_shreds_for_slot_from_fs(&path, start_index)
    }

    pub(crate) fn insert_data_shred_into_cache(&self, slot: Slot, index: u64, shred: &Shred) {
        let data_slot_cache = self.data_slot_cache(slot).unwrap_or_else(||
            // Inner map for slot does not exist, let's create it
            // DashMap .entry().or_insert() returns a RefMut, essentially a write lock,
            // which is dropped after this block ends, minimizing time held by the lock.
            // We still need a reference to the `ShredCache` behind the lock, hence, we
            // clone it out (`ShredCache` is an Arc so it is cheap to clone).
            self.data_shred_cache.entry(slot).or_insert(Arc::new(RwLock::new(BTreeMap::new()))).clone());
        data_slot_cache
            .write()
            .unwrap()
            .insert(index, shred.payload.clone());
    }

    pub(crate) fn flush_data_shreds_for_slot_to_fs(&self, slot: Slot) -> Result<()> {
        {
            // First, check if we have a cache for this slot
            let slot_cache = match self.data_slot_cache(slot) {
                Some(slot_cache) => slot_cache,
                // TODO: depending on how caller uses this, may want to return SlotUnavailable
                None => return Ok(()),
            };

            let path = self.slot_data_shreds_path(slot);
            let temp_path = format!("{}.tmp", &path);
            let temp_path = Path::new(&temp_path);

            // Write contents to a temporary file first. We will later rename the file;
            // in this way, we approximate an atomic file write
            // TODO: Should we make this check if file already exists instead of clobbering it
            let mut file = fs::File::create(temp_path)?;
            let result: Result<Vec<_>> = slot_cache
                .read()
                .unwrap()
                .iter()
                .map(|(_, shred)| {
                    file.write_all(&shred).map_err(|err| {
                        BlockstoreError::Io(IoError::new(
                            ErrorKind::Other,
                            format!("Unable to write slot {}: {}", slot, err),
                        ))
                    })
                })
                .collect();
            // Check that all of the individual writes succeeded
            let _result = result?;
            let path = Path::new(&path);
            fs::rename(temp_path, path)?;
        }
        // TODO: ENABLE DROP FROM CACHE ONCE DONE TESTING
        // self.data_shred_cache.remove(&slot);
        Ok(())
    }

    pub(crate) fn purge_data_shreds(&self, slot: Slot) {
        self.data_shred_cache.remove(&slot);
        // Could get errors such as file doesn't exist; we don't care so just eat the error
        let _ = fs::remove_file(self.slot_data_shreds_path(slot));
    }

    fn slot_data_shreds_path(&self, slot: Slot) -> String {
        Path::new(&self.data_shred_path)
            .join(slot.to_string())
            .to_str()
            .unwrap()
            .to_string()
    }

    // TODO: move stuff around such that this function doesn't have to be public;
    // find_missing_data_indexes() uses this at the moment
    pub(crate) fn data_slot_cache(&self, slot: Slot) -> Option<Arc<RwLock<ShredCache>>> {
        self.data_shred_cache
            .get(&slot)
            .map(|res| res.value().clone())
    }

    fn get_shred_from_fs(slot_path: &str, index: u64) -> Result<Option<Vec<u8>>> {
        // Shreds are grouped into files by slot, so pull out only the relevant shred from file
        let path = Path::new(slot_path);
        let mut file = match fs::File::open(path) {
            Ok(file) => file,
            Err(_err) => return Ok(None),
        };
        // Shreds are stored end to end, so the ith shred will occupy
        // bytes [i * SHRED_PAYLOAD_SIZE, (i + 1) * SHRED_PAYLOAD_SIZE)
        let payload_size = SHRED_PAYLOAD_SIZE as u64;
        let metadata = fs::metadata(path)?;
        // Ensure the file is long enough to contain desired shred
        if metadata.len() < (index + 1) * payload_size {
            // TODO: Double check that returning None instead of erroring is correct behavior
            return Ok(None);
        }
        file.seek(SeekFrom::Start(index * payload_size))?;
        let mut buffer = vec![0; SHRED_PAYLOAD_SIZE];
        file.read_exact(&mut buffer)?;
        Ok(Some(buffer))
    }

    fn get_shreds_for_slot_from_fs(
        slot_path: &str,
        start_index: u64,
    ) -> Option<ShredResult<Vec<Shred>>> {
        let path = Path::new(slot_path);
        let mut file = match fs::File::open(path) {
            Ok(file) => file,
            Err(_err) => return Some(Ok(vec![])),
        };
        // Shreds are stored end to end, so the ith shred will occupy
        // bytes [i * SHRED_PAYLOAD_SIZE, (i + 1) * SHRED_PAYLOAD_SIZE)
        let payload_size = SHRED_PAYLOAD_SIZE as u64;
        let metadata = fs::metadata(path).ok()?;
        // Ensure the file will contain at least one shred so we don't seek past end of file
        if metadata.len() < (start_index + 1) * payload_size {
            // TODO: Double check that empty list and not an error is correct behavior
            return Some(Ok(vec![]));
        }
        // TODO: Check metadata.len() % payload_size == 0 ?
        let num_shreds: usize = ((metadata.len() - (start_index * payload_size)) / payload_size)
            .try_into()
            .unwrap();
        let mut buffers = vec![];
        buffers.reserve(num_shreds);
        // Move the cursor up to proper start position so first read grabs start_index
        file.seek(SeekFrom::Start(start_index * payload_size))
            .ok()?;
        for i in 0..num_shreds {
            let mut buffer = vec![0; SHRED_PAYLOAD_SIZE];
            file.read_exact(&mut buffer).ok()?;
            buffers.insert(i, buffer);
        }

        Some(
            buffers
                .into_iter()
                .map(move |shred| Shred::new_from_serialized_shred(shred))
                .collect(),
        )
    }
}
