use log::*;
use solana_sdk::account::KeyedAccount;
use solana_sdk::instruction::InstructionError;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_instruction::{SystemError, SystemInstruction};
use solana_sdk::system_program;
use solana_sdk::sysvar;

fn create_system_account(
    from: &mut KeyedAccount,
    to: &mut KeyedAccount,
    lamports: u64,
    space: u64,
    program_id: &Pubkey,
) -> Result<(), InstructionError> {
    if from.signer_key().is_none() {
        debug!("from is unsigned");
        return Err(InstructionError::MissingRequiredSignature);
    }

    if !system_program::check_id(&from.account.owner) {
        debug!(
            "CreateAccount: invalid account[from] owner {} ",
            &from.account.owner
        );
        return Err(SystemError::SourceNotSystemAccount.into());
    }

    if !to.account.data.is_empty() || !system_program::check_id(&to.account.owner) {
        debug!(
            "CreateAccount: invalid argument; account {} already in use",
            to.unsigned_key()
        );
        return Err(SystemError::AccountAlreadyInUse.into());
    }

    if sysvar::check_id(&program_id) {
        debug!(
            "CreateAccount: invalid argument; program id {} invalid",
            program_id
        );
        return Err(SystemError::InvalidProgramId.into());
    }

    if sysvar::is_sysvar_id(&to.unsigned_key()) {
        debug!(
            "CreateAccount: invalid argument; account id {} invalid",
            program_id
        );
        return Err(SystemError::InvalidAccountId.into());
    }

    if lamports > from.account.lamports {
        debug!(
            "CreateAccount: insufficient lamports ({}, need {})",
            from.account.lamports, lamports
        );
        return Err(SystemError::ResultWithNegativeLamports.into());
    }
    from.account.lamports -= lamports;
    to.account.lamports += lamports;
    to.account.owner = *program_id;
    to.account.data = vec![0; space as usize];
    to.account.executable = false;
    Ok(())
}

fn assign_account_to_program(
    account: &mut KeyedAccount,
    program_id: &Pubkey,
) -> Result<(), InstructionError> {
    if !system_program::check_id(&account.account.owner) {
        return Err(InstructionError::IncorrectProgramId);
    }

    if account.signer_key().is_none() {
        debug!("account is unsigned");
        return Err(InstructionError::MissingRequiredSignature);
    }

    account.account.owner = *program_id;
    Ok(())
}
fn transfer_lamports(
    from: &mut KeyedAccount,
    to: &mut KeyedAccount,
    lamports: u64,
) -> Result<(), InstructionError> {
    if from.signer_key().is_none() {
        debug!("from is unsigned");
        return Err(InstructionError::MissingRequiredSignature);
    }

    if lamports > from.account.lamports {
        debug!(
            "Transfer: insufficient lamports ({}, need {})",
            from.account.lamports, lamports
        );
        return Err(SystemError::ResultWithNegativeLamports.into());
    }
    from.account.lamports -= lamports;
    to.account.lamports += lamports;
    Ok(())
}

macro_rules! count_tts {
    () => {0usize};
    ($_head:tt $($tail:tt)*) => {1usize + count_tts!($($tail)*)};
}

#[macro_export]
macro_rules! with_keyed_accounts {
    ($keyed_accounts:ident, ( $($x:tt),+ ), $do:expr) => (
     {
        let xs = count_tts!($( $x )*);
        if $keyed_accounts.len() < xs {
            Err(InstructionError::InvalidInstructionData)
        } else if let &mut [ $( ref mut $x, )* ] = &mut $keyed_accounts[..xs] {
            $do
        } else {
           Err(InstructionError::InvalidInstructionData)
        }
    }
    )
}

pub fn process_instruction(
    _program_id: &Pubkey,
    keyed_accounts: &mut [KeyedAccount],
    data: &[u8],
) -> Result<(), InstructionError> {
    if let Ok(instruction) = bincode::deserialize(data) {
        trace!("process_instruction: {:?}", instruction);
        trace!("keyed_accounts: {:?}", keyed_accounts);

        #[allow(clippy::match_ref_pats)]
        match instruction {
            SystemInstruction::CreateAccount {
                lamports,
                space,
                program_id,
            } => with_keyed_accounts!(
                keyed_accounts,
                (from, to),
                create_system_account(from, to, lamports, space, &program_id)
            ),
            SystemInstruction::Assign { program_id } => with_keyed_accounts!(
                keyed_accounts,
                (account),
                assign_account_to_program(account, &program_id)
            ),
            SystemInstruction::Transfer { lamports } => with_keyed_accounts!(
                keyed_accounts,
                (from, to),
                transfer_lamports(from, to, lamports)
            ),
        }
    } else {
        debug!("Invalid instruction data: {:?}", data);
        Err(InstructionError::InvalidInstructionData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bank::Bank;
    use crate::bank_client::BankClient;
    use bincode::serialize;
    use solana_sdk::account::Account;
    use solana_sdk::client::SyncClient;
    use solana_sdk::genesis_block::create_genesis_block;
    use solana_sdk::instruction::{AccountMeta, Instruction, InstructionError};
    use solana_sdk::signature::{Keypair, KeypairUtil};
    use solana_sdk::system_program;
    use solana_sdk::transaction::TransactionError;

    #[test]
    fn test_create_system_account() {
        let new_program_owner = Pubkey::new(&[9; 32]);
        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &system_program::id());

        let to = Pubkey::new_rand();
        let mut to_account = Account::new(0, 0, &Pubkey::default());

        create_system_account(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new(&to, false, &mut to_account),
            50,
            2,
            &new_program_owner,
        )
        .unwrap();
        let from_lamports = from_account.lamports;
        let to_lamports = to_account.lamports;
        let to_owner = to_account.owner;
        let to_data = to_account.data.clone();
        assert_eq!(from_lamports, 50);
        assert_eq!(to_lamports, 50);
        assert_eq!(to_owner, new_program_owner);
        assert_eq!(to_data, [0, 0]);
    }

    #[test]
    fn test_create_negative_lamports() {
        // Attempt to create account with more lamports than remaining in from_account
        let new_program_owner = Pubkey::new(&[9; 32]);
        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &system_program::id());

        let to = Pubkey::new_rand();
        let mut to_account = Account::new(0, 0, &Pubkey::default());
        let unchanged_account = to_account.clone();

        let result = create_system_account(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new(&to, false, &mut to_account),
            150,
            2,
            &new_program_owner,
        );
        assert_eq!(result, Err(SystemError::ResultWithNegativeLamports.into()));
        let from_lamports = from_account.lamports;
        assert_eq!(from_lamports, 100);
        assert_eq!(to_account, unchanged_account);
    }

    #[test]
    fn test_create_already_owned() {
        // Attempt to create system account in account already owned by another program
        let new_program_owner = Pubkey::new(&[9; 32]);
        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &system_program::id());

        let original_program_owner = Pubkey::new(&[5; 32]);
        let owned_key = Pubkey::new_rand();
        let mut owned_account = Account::new(0, 0, &original_program_owner);
        let unchanged_account = owned_account.clone();

        let result = create_system_account(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new(&owned_key, false, &mut owned_account),
            50,
            2,
            &new_program_owner,
        );
        assert_eq!(result, Err(SystemError::AccountAlreadyInUse.into()));
        let from_lamports = from_account.lamports;
        assert_eq!(from_lamports, 100);
        assert_eq!(owned_account, unchanged_account);
    }

    #[test]
    fn test_create_sysvar_invalid_id() {
        // Attempt to create system account in account already owned by another program
        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &system_program::id());

        let to = Pubkey::new_rand();
        let mut to_account = Account::default();

        // fail to create a sysvar::id() owned account
        let result = create_system_account(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new(&to, false, &mut to_account),
            50,
            2,
            &sysvar::id(),
        );
        assert_eq!(result, Err(SystemError::InvalidProgramId.into()));

        let to = sysvar::fees::id();
        let mut to_account = Account::default();

        // fail to create an account with a sysvar id
        let result = create_system_account(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new(&to, false, &mut to_account),
            50,
            2,
            &system_program::id(),
        );
        assert_eq!(result, Err(SystemError::InvalidAccountId.into()));

        let from_lamports = from_account.lamports;
        assert_eq!(from_lamports, 100);
    }

    #[test]
    fn test_create_data_populated() {
        // Attempt to create system account in account with populated data
        let new_program_owner = Pubkey::new(&[9; 32]);
        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &system_program::id());

        let populated_key = Pubkey::new_rand();
        let mut populated_account = Account {
            data: vec![0, 1, 2, 3],
            ..Account::default()
        };
        let unchanged_account = populated_account.clone();

        let result = create_system_account(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new(&populated_key, false, &mut populated_account),
            50,
            2,
            &new_program_owner,
        );
        assert_eq!(result, Err(SystemError::AccountAlreadyInUse.into()));
        assert_eq!(from_account.lamports, 100);
        assert_eq!(populated_account, unchanged_account);
    }

    #[test]
    fn test_create_not_system_account() {
        let other_program = Pubkey::new(&[9; 32]);

        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &other_program);
        let to = Pubkey::new_rand();
        let mut to_account = Account::new(0, 0, &Pubkey::default());
        let result = create_system_account(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new(&to, false, &mut to_account),
            50,
            2,
            &other_program,
        );
        assert_eq!(result, Err(SystemError::SourceNotSystemAccount.into()));
    }

    #[test]
    fn test_assign_account_to_program() {
        let new_program_owner = Pubkey::new(&[9; 32]);

        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &system_program::id());
        assign_account_to_program(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &new_program_owner,
        )
        .unwrap();
        let from_owner = from_account.owner;
        assert_eq!(from_owner, new_program_owner);

        // Attempt to assign account not owned by system program
        let another_program_owner = Pubkey::new(&[8; 32]);
        let mut keyed_accounts = [KeyedAccount::new(&from, true, &mut from_account)];
        let instruction = SystemInstruction::Assign {
            program_id: another_program_owner,
        };
        let data = serialize(&instruction).unwrap();
        let result = process_instruction(&system_program::id(), &mut keyed_accounts, &data);
        assert_eq!(result, Err(InstructionError::IncorrectProgramId));
        assert_eq!(from_account.owner, new_program_owner);
    }

    #[test]
    fn test_process_bogus_instruction() {
        // Attempt to assign with no accounts
        let instruction = SystemInstruction::Assign {
            program_id: Pubkey::new_rand(),
        };
        let data = serialize(&instruction).unwrap();
        let result = process_instruction(&system_program::id(), &mut [], &data);
        assert_eq!(result, Err(InstructionError::InvalidInstructionData));

        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &system_program::id());
        // Attempt to transfer with no destination
        let instruction = SystemInstruction::Transfer { lamports: 0 };
        let data = serialize(&instruction).unwrap();
        let result = process_instruction(
            &system_program::id(),
            &mut [KeyedAccount::new(&from, true, &mut from_account)],
            &data,
        );
        assert_eq!(result, Err(InstructionError::InvalidInstructionData));
    }

    #[test]
    fn test_transfer_lamports() {
        let from = Pubkey::new_rand();
        let mut from_account = Account::new(100, 0, &Pubkey::new(&[2; 32])); // account owner should not matter
        let to = Pubkey::new_rand();
        let mut to_account = Account::new(1, 0, &Pubkey::new(&[3; 32])); // account owner should not matter
        transfer_lamports(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new_credit_only(&to, false, &mut to_account),
            50,
        )
        .unwrap();
        let from_lamports = from_account.lamports;
        let to_lamports = to_account.lamports;
        assert_eq!(from_lamports, 50);
        assert_eq!(to_lamports, 51);

        // Attempt to move more lamports than remaining in from_account
        let result = transfer_lamports(
            &mut KeyedAccount::new(&from, true, &mut from_account),
            &mut KeyedAccount::new_credit_only(&to, false, &mut to_account),
            100,
        );
        assert_eq!(result, Err(SystemError::ResultWithNegativeLamports.into()));
        assert_eq!(from_account.lamports, 50);
        assert_eq!(to_account.lamports, 51);
    }

    #[test]
    fn test_system_unsigned_transaction() {
        let (genesis_block, alice_keypair) = create_genesis_block(100);
        let alice_pubkey = alice_keypair.pubkey();
        let mallory_keypair = Keypair::new();
        let mallory_pubkey = mallory_keypair.pubkey();

        // Fund to account to bypass AccountNotFound error
        let bank = Bank::new(&genesis_block);
        let bank_client = BankClient::new(bank);
        bank_client
            .transfer(50, &alice_keypair, &mallory_pubkey)
            .unwrap();

        // Erroneously sign transaction with recipient account key
        // No signature case is tested by bank `test_zero_signatures()`
        let account_metas = vec![
            AccountMeta::new(alice_pubkey, false),
            AccountMeta::new(mallory_pubkey, true),
        ];
        let malicious_instruction = Instruction::new(
            system_program::id(),
            &SystemInstruction::Transfer { lamports: 10 },
            account_metas,
        );
        assert_eq!(
            bank_client
                .send_instruction(&mallory_keypair, malicious_instruction)
                .unwrap_err()
                .unwrap(),
            TransactionError::InstructionError(0, InstructionError::MissingRequiredSignature)
        );
        assert_eq!(bank_client.get_balance(&alice_pubkey).unwrap(), 50);
        assert_eq!(bank_client.get_balance(&mallory_pubkey).unwrap(), 50);
    }
}
