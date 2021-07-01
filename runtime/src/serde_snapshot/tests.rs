#[cfg(test)]
use {
    super::*,
    crate::{
        accounts::{create_test_accounts, Accounts},
        accounts_db::{get_temp_accounts_paths, AccountShrinkThreshold},
        bank::{Bank, StatusCacheRc},
        hardened_unpack::UnpackedAppendVecMap,
    },
    bincode::serialize_into,
    rand::{thread_rng, Rng},
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
        genesis_config::{create_genesis_config, ClusterType},
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    },
    std::{
        io::{BufReader, Cursor},
        path::Path,
    },
    tempfile::TempDir,
};

#[cfg(test)]
fn copy_append_vecs<P: AsRef<Path>>(
    accounts_db: &AccountsDb,
    output_dir: P,
) -> std::io::Result<UnpackedAppendVecMap> {
    let storage_entries = accounts_db.get_snapshot_storages(Slot::max_value(), None).0;
    let mut unpacked_append_vec_map = UnpackedAppendVecMap::new();
    for storage in storage_entries.iter().flatten() {
        let storage_path = storage.get_path();
        let file_name = AppendVec::file_name(storage.slot(), storage.append_vec_id());
        let output_path = output_dir.as_ref().join(&file_name);
        std::fs::copy(&storage_path, &output_path)?;
        unpacked_append_vec_map.insert(file_name, output_path);
    }

    Ok(unpacked_append_vec_map)
}

#[cfg(test)]
fn check_accounts(accounts: &Accounts, pubkeys: &[Pubkey], num: usize) {
    for _ in 1..num {
        let idx = thread_rng().gen_range(0, num - 1);
        let ancestors = vec![(0, 0)].into_iter().collect();
        let account = accounts.load_without_fixed_root(&ancestors, &pubkeys[idx]);
        let account1 = Some((
            AccountSharedData::new((idx + 1) as u64, 0, AccountSharedData::default().owner()),
            0,
        ));
        assert_eq!(account, account1);
    }
}

#[cfg(test)]
fn context_accountsdb_from_stream<'a, C, R>(
    stream: &mut BufReader<R>,
    account_paths: &[PathBuf],
    unpacked_append_vec_map: UnpackedAppendVecMap,
) -> Result<AccountsDb, Error>
where
    C: TypeContext<'a>,
    R: Read,
{
    // read and deserialise the accounts database directly from the stream
    reconstruct_accountsdb_from_fields(
        C::deserialize_accounts_db_fields(stream)?,
        account_paths,
        unpacked_append_vec_map,
        &ClusterType::Development,
        AccountSecondaryIndexes::default(),
        false,
        None,
        AccountShrinkThreshold::default(),
        false,
    )
}

#[cfg(test)]
fn accountsdb_from_stream<R>(
    serde_style: SerdeStyle,
    stream: &mut BufReader<R>,
    account_paths: &[PathBuf],
    unpacked_append_vec_map: UnpackedAppendVecMap,
) -> Result<AccountsDb, Error>
where
    R: Read,
{
    match serde_style {
        SerdeStyle::Newer => context_accountsdb_from_stream::<TypeContextFuture, R>(
            stream,
            account_paths,
            unpacked_append_vec_map,
        ),
    }
}

#[cfg(test)]
fn accountsdb_to_stream<W>(
    serde_style: SerdeStyle,
    stream: &mut W,
    accounts_db: &AccountsDb,
    slot: Slot,
    account_storage_entries: &[SnapshotStorage],
) -> Result<(), Error>
where
    W: Write,
{
    match serde_style {
        SerdeStyle::Newer => serialize_into(
            stream,
            &SerializableAccountsDb::<TypeContextFuture> {
                accounts_db,
                slot,
                account_storage_entries,
                phantom: std::marker::PhantomData::default(),
            },
        ),
    }
}

#[cfg(test)]
fn test_accounts_serialize_style(serde_style: SerdeStyle) {
    solana_logger::setup();
    let (_accounts_dir, paths) = get_temp_accounts_paths(4).unwrap();
    let accounts = Accounts::new_with_config(
        paths,
        &ClusterType::Development,
        AccountSecondaryIndexes::default(),
        false,
        AccountShrinkThreshold::default(),
    );

    let mut pubkeys: Vec<Pubkey> = vec![];
    create_test_accounts(&accounts, &mut pubkeys, 100, 0);
    check_accounts(&accounts, &pubkeys, 100);
    accounts.add_root(0);

    let mut writer = Cursor::new(vec![]);
    accountsdb_to_stream(
        serde_style,
        &mut writer,
        &*accounts.accounts_db,
        0,
        &accounts.accounts_db.get_snapshot_storages(0, None).0,
    )
    .unwrap();

    let copied_accounts = TempDir::new().unwrap();

    // Simulate obtaining a copy of the AppendVecs from a tarball
    let unpacked_append_vec_map =
        copy_append_vecs(&accounts.accounts_db, copied_accounts.path()).unwrap();

    let buf = writer.into_inner();
    let mut reader = BufReader::new(&buf[..]);
    let (_accounts_dir, daccounts_paths) = get_temp_accounts_paths(2).unwrap();
    let daccounts = Accounts::new_empty(
        accountsdb_from_stream(
            serde_style,
            &mut reader,
            &daccounts_paths,
            unpacked_append_vec_map,
        )
        .unwrap(),
    );
    check_accounts(&daccounts, &pubkeys, 100);
    assert_eq!(accounts.bank_hash_at(0), daccounts.bank_hash_at(0));
}

#[cfg(test)]
fn test_bank_serialize_style(serde_style: SerdeStyle) {
    solana_logger::setup();
    let (genesis_config, _) = create_genesis_config(500);
    let bank0 = Arc::new(Bank::new(&genesis_config));
    let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
    bank0.squash();

    // Create an account on a non-root fork
    let key1 = Keypair::new();
    bank1.deposit(&key1.pubkey(), 5).unwrap();

    let bank2 = Bank::new_from_parent(&bank0, &Pubkey::default(), 2);

    // Test new account
    let key2 = Keypair::new();
    bank2.deposit(&key2.pubkey(), 10).unwrap();
    assert_eq!(bank2.get_balance(&key2.pubkey()), 10);

    let key3 = Keypair::new();
    bank2.deposit(&key3.pubkey(), 0).unwrap();

    bank2.freeze();
    bank2.squash();
    bank2.force_flush_accounts_cache();

    let snapshot_storages = bank2.get_snapshot_storages();
    let mut buf = vec![];
    let mut writer = Cursor::new(&mut buf);
    crate::serde_snapshot::bank_to_stream(
        serde_style,
        &mut std::io::BufWriter::new(&mut writer),
        &bank2,
        &snapshot_storages,
    )
    .unwrap();

    let rdr = Cursor::new(&buf[..]);
    let mut reader = std::io::BufReader::new(&buf[rdr.position() as usize..]);

    // Create a new set of directories for this bank's accounts
    let (_accounts_dir, dbank_paths) = get_temp_accounts_paths(4).unwrap();
    let ref_sc = StatusCacheRc::default();
    ref_sc.status_cache.write().unwrap().add_root(2);
    // Create a directory to simulate AppendVecs unpackaged from a snapshot tar
    let copied_accounts = TempDir::new().unwrap();
    let unpacked_append_vec_map =
        copy_append_vecs(&bank2.rc.accounts.accounts_db, copied_accounts.path()).unwrap();
    let mut dbank = crate::serde_snapshot::bank_from_stream(
        serde_style,
        &mut reader,
        &dbank_paths,
        unpacked_append_vec_map,
        &genesis_config,
        &[],
        None,
        None,
        AccountSecondaryIndexes::default(),
        false,
        None,
        AccountShrinkThreshold::default(),
        false,
    )
    .unwrap();
    dbank.src = ref_sc;
    assert_eq!(dbank.get_balance(&key1.pubkey()), 0);
    assert_eq!(dbank.get_balance(&key2.pubkey()), 10);
    assert_eq!(dbank.get_balance(&key3.pubkey()), 0);
    assert!(bank2 == dbank);
}

#[cfg(test)]
pub(crate) fn reconstruct_accounts_db_via_serialization(
    accounts: &AccountsDb,
    slot: Slot,
) -> AccountsDb {
    let mut writer = Cursor::new(vec![]);
    let snapshot_storages = accounts.get_snapshot_storages(slot, None).0;
    accountsdb_to_stream(
        SerdeStyle::Newer,
        &mut writer,
        accounts,
        slot,
        &snapshot_storages,
    )
    .unwrap();

    let buf = writer.into_inner();
    let mut reader = BufReader::new(&buf[..]);
    let copied_accounts = TempDir::new().unwrap();

    // Simulate obtaining a copy of the AppendVecs from a tarball
    let unpacked_append_vec_map = copy_append_vecs(accounts, copied_accounts.path()).unwrap();
    let mut accounts_db =
        accountsdb_from_stream(SerdeStyle::Newer, &mut reader, &[], unpacked_append_vec_map)
            .unwrap();

    // The append vecs will be used from `copied_accounts` directly by the new AccountsDb so keep
    // its TempDir alive
    accounts_db
        .temp_paths
        .as_mut()
        .unwrap()
        .push(copied_accounts);

    accounts_db
}

#[test]
fn test_accounts_serialize_newer() {
    test_accounts_serialize_style(SerdeStyle::Newer)
}

#[test]
fn test_bank_serialize_newer() {
    test_bank_serialize_style(SerdeStyle::Newer)
}

#[cfg(all(test, RUSTC_WITH_SPECIALIZATION))]
mod test_bank_serialize {
    use super::*;

    // This some what long test harness is required to freeze the ABI of
    // Bank's serialization due to versioned nature
    #[frozen_abi(digest = "6msodEzE7YzFtorBhiP6ax4PKBaPZTkmYdGAdpoxLCvV")]
    #[derive(Serialize, AbiExample)]
    pub struct BankAbiTestWrapperFuture {
        #[serde(serialize_with = "wrapper_future")]
        bank: Bank,
    }

    pub fn wrapper_future<S>(bank: &Bank, s: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let snapshot_storages = bank
            .rc
            .accounts
            .accounts_db
            .get_snapshot_storages(0, None)
            .0;
        // ensure there is a single snapshot storage example for ABI digesting
        assert_eq!(snapshot_storages.len(), 1);

        (SerializableBankAndStorage::<future::Context> {
            bank,
            snapshot_storages: &snapshot_storages,
            phantom: std::marker::PhantomData::default(),
        })
        .serialize(s)
    }
}
