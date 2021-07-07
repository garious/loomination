use solana_cli::{
    cli::{process_command, request_and_confirm_airdrop, CliCommand, CliConfig},
    spend_utils::SpendAmount,
    stake::StakeAuthorizationIndexed,
    test_utils::{check_ready, check_recent_balance},
};
use solana_cli_output::{parse_sign_only_reply_string, OutputFormat};
use solana_client::{
    blockhash_query::{self, BlockhashQuery},
    nonce_utils,
    rpc_client::RpcClient,
};
use solana_core::test_validator::TestValidator;
use solana_faucet::faucet::run_local_faucet;
use solana_sdk::{
    account_utils::StateMut,
    commitment_config::CommitmentConfig,
    nonce::State as NonceState,
    pubkey::Pubkey,
    signature::{keypair_from_seed, Keypair, Signer},
    stake::{
        self,
        instruction::LockupArgs,
        state::{Lockup, StakeAuthorize, StakeState},
    },
};

#[test]
fn test_stake_delegation_force() {
    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_no_fees(mint_pubkey, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());
    let default_signer = Keypair::new();

    let mut config = CliConfig::recent_for_tests();
    config.json_rpc_url = test_validator.rpc_url();
    config.signers = vec![&default_signer];

    request_and_confirm_airdrop(&rpc_client, &config, &config.signers[0].pubkey(), 100_000)
        .unwrap();

    // Create vote account
    let vote_keypair = Keypair::new();
    config.signers = vec![&default_signer, &vote_keypair];
    config.command = CliCommand::CreateVoteAccount {
        vote_account: 1,
        seed: None,
        identity_account: 0,
        authorized_voter: None,
        authorized_withdrawer: None,
        commission: 0,
        memo: None,
    };
    process_command(&config).unwrap();

    // Create stake account
    let stake_keypair = Keypair::new();
    config.signers = vec![&default_signer, &stake_keypair];
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: None,
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();

    // Delegate stake fails (vote account had never voted)
    config.signers = vec![&default_signer];
    config.command = CliCommand::DelegateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        vote_account_pubkey: vote_keypair.pubkey(),
        stake_authority: 0,
        force: false,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap_err();

    // But if we force it, it works anyway!
    config.command = CliCommand::DelegateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        vote_account_pubkey: vote_keypair.pubkey(),
        stake_authority: 0,
        force: true,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();
}

#[test]
fn test_seed_stake_delegation_and_deactivation() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_no_fees(mint_pubkey, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());

    let validator_keypair = keypair_from_seed(&[0u8; 32]).unwrap();
    let mut config_validator = CliConfig::recent_for_tests();
    config_validator.json_rpc_url = test_validator.rpc_url();
    config_validator.signers = vec![&validator_keypair];

    request_and_confirm_airdrop(
        &rpc_client,
        &config_validator,
        &config_validator.signers[0].pubkey(),
        100_000,
    )
    .unwrap();
    check_recent_balance(100_000, &rpc_client, &config_validator.signers[0].pubkey());

    let stake_address = Pubkey::create_with_seed(
        &config_validator.signers[0].pubkey(),
        "hi there",
        &stake::program::id(),
    )
    .expect("bad seed");

    // Create stake account with a seed, uses the validator config as the base,
    //   which is nice ;)
    config_validator.command = CliCommand::CreateStakeAccount {
        stake_account: 0,
        seed: Some("hi there".to_string()),
        staker: None,
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config_validator).unwrap();

    // Delegate stake
    config_validator.command = CliCommand::DelegateStake {
        stake_account_pubkey: stake_address,
        vote_account_pubkey: test_validator.vote_account_address(),
        stake_authority: 0,
        force: true,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config_validator).unwrap();

    // Deactivate stake
    config_validator.command = CliCommand::DeactivateStake {
        stake_account_pubkey: stake_address,
        stake_authority: 0,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        seed: None,
        fee_payer: 0,
    };
    process_command(&config_validator).unwrap();
}

#[test]
fn test_stake_delegation_and_deactivation() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_no_fees(mint_pubkey, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());
    let validator_keypair = Keypair::new();

    let mut config_validator = CliConfig::recent_for_tests();
    config_validator.json_rpc_url = test_validator.rpc_url();
    config_validator.signers = vec![&validator_keypair];

    let stake_keypair = keypair_from_seed(&[0u8; 32]).unwrap();

    request_and_confirm_airdrop(
        &rpc_client,
        &config_validator,
        &config_validator.signers[0].pubkey(),
        100_000,
    )
    .unwrap();
    check_recent_balance(100_000, &rpc_client, &config_validator.signers[0].pubkey());

    // Create stake account
    config_validator.signers.push(&stake_keypair);
    config_validator.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: None,
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config_validator).unwrap();

    // Delegate stake
    config_validator.signers.pop();
    config_validator.command = CliCommand::DelegateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        vote_account_pubkey: test_validator.vote_account_address(),
        stake_authority: 0,
        force: true,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config_validator).unwrap();

    // Deactivate stake
    config_validator.command = CliCommand::DeactivateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        stake_authority: 0,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        seed: None,
        fee_payer: 0,
    };
    process_command(&config_validator).unwrap();
}

#[test]
fn test_offline_stake_delegation_and_deactivation() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_no_fees(mint_pubkey, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());

    let mut config_validator = CliConfig::recent_for_tests();
    config_validator.json_rpc_url = test_validator.rpc_url();
    let validator_keypair = Keypair::new();
    config_validator.signers = vec![&validator_keypair];

    let mut config_payer = CliConfig::recent_for_tests();
    config_payer.json_rpc_url = test_validator.rpc_url();

    let stake_keypair = keypair_from_seed(&[0u8; 32]).unwrap();

    let mut config_offline = CliConfig::recent_for_tests();
    config_offline.json_rpc_url = String::default();
    config_offline.command = CliCommand::ClusterVersion;
    let offline_keypair = Keypair::new();
    config_offline.signers = vec![&offline_keypair];
    // Verify that we cannot reach the cluster
    process_command(&config_offline).unwrap_err();

    request_and_confirm_airdrop(
        &rpc_client,
        &config_validator,
        &config_validator.signers[0].pubkey(),
        100_000,
    )
    .unwrap();
    check_recent_balance(100_000, &rpc_client, &config_validator.signers[0].pubkey());

    request_and_confirm_airdrop(
        &rpc_client,
        &config_offline,
        &config_offline.signers[0].pubkey(),
        100_000,
    )
    .unwrap();
    check_recent_balance(100_000, &rpc_client, &config_offline.signers[0].pubkey());

    // Create stake account
    config_validator.signers.push(&stake_keypair);
    config_validator.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: Some(config_offline.signers[0].pubkey()),
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config_validator).unwrap();

    // Delegate stake offline
    let (blockhash, _) = rpc_client.get_recent_blockhash().unwrap();
    config_offline.command = CliCommand::DelegateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        vote_account_pubkey: test_validator.vote_account_address(),
        stake_authority: 0,
        force: true,
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(blockhash),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    config_offline.output_format = OutputFormat::JsonCompact;
    let sig_response = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sig_response);
    assert!(sign_only.has_all_signers());
    let offline_presigner = sign_only
        .presigner_of(&config_offline.signers[0].pubkey())
        .unwrap();
    config_payer.signers = vec![&offline_presigner];
    config_payer.command = CliCommand::DelegateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        vote_account_pubkey: test_validator.vote_account_address(),
        stake_authority: 0,
        force: true,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(blockhash_query::Source::Cluster, blockhash),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config_payer).unwrap();

    // Deactivate stake offline
    let (blockhash, _) = rpc_client.get_recent_blockhash().unwrap();
    config_offline.command = CliCommand::DeactivateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        stake_authority: 0,
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(blockhash),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        seed: None,
        fee_payer: 0,
    };
    let sig_response = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sig_response);
    assert!(sign_only.has_all_signers());
    let offline_presigner = sign_only
        .presigner_of(&config_offline.signers[0].pubkey())
        .unwrap();
    config_payer.signers = vec![&offline_presigner];
    config_payer.command = CliCommand::DeactivateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        stake_authority: 0,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(blockhash_query::Source::Cluster, blockhash),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        seed: None,
        fee_payer: 0,
    };
    process_command(&config_payer).unwrap();
}

#[test]
fn test_nonced_stake_delegation_and_deactivation() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_no_fees(mint_pubkey, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());

    let config_keypair = keypair_from_seed(&[0u8; 32]).unwrap();
    let mut config = CliConfig::recent_for_tests();
    config.signers = vec![&config_keypair];
    config.json_rpc_url = test_validator.rpc_url();

    let minimum_nonce_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(NonceState::size())
        .unwrap();

    request_and_confirm_airdrop(&rpc_client, &config, &config.signers[0].pubkey(), 100_000)
        .unwrap();

    // Create stake account
    let stake_keypair = Keypair::new();
    config.signers.push(&stake_keypair);
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: None,
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();

    // Create nonce account
    let nonce_account = Keypair::new();
    config.signers[1] = &nonce_account;
    config.command = CliCommand::CreateNonceAccount {
        nonce_account: 1,
        seed: None,
        nonce_authority: Some(config.signers[0].pubkey()),
        memo: None,
        amount: SpendAmount::Some(minimum_nonce_balance),
    };
    process_command(&config).unwrap();

    // Fetch nonce hash
    let nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;

    // Delegate stake
    config.signers = vec![&config_keypair];
    config.command = CliCommand::DelegateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        vote_account_pubkey: test_validator.vote_account_address(),
        stake_authority: 0,
        force: true,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(
            blockhash_query::Source::NonceAccount(nonce_account.pubkey()),
            nonce_hash,
        ),
        nonce_account: Some(nonce_account.pubkey()),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();

    // Fetch nonce hash
    let nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;

    // Deactivate stake
    config.command = CliCommand::DeactivateStake {
        stake_account_pubkey: stake_keypair.pubkey(),
        stake_authority: 0,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(
            blockhash_query::Source::NonceAccount(nonce_account.pubkey()),
            nonce_hash,
        ),
        nonce_account: Some(nonce_account.pubkey()),
        nonce_authority: 0,
        memo: None,
        seed: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();
}

#[test]
fn test_stake_authorize() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_no_fees(mint_pubkey, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());
    let default_signer = Keypair::new();

    let mut config = CliConfig::recent_for_tests();
    config.json_rpc_url = test_validator.rpc_url();
    config.signers = vec![&default_signer];

    request_and_confirm_airdrop(&rpc_client, &config, &config.signers[0].pubkey(), 100_000)
        .unwrap();

    let offline_keypair = keypair_from_seed(&[0u8; 32]).unwrap();
    let mut config_offline = CliConfig::recent_for_tests();
    config_offline.signers = vec![&offline_keypair];
    config_offline.json_rpc_url = String::default();
    let offline_authority_pubkey = config_offline.signers[0].pubkey();
    config_offline.command = CliCommand::ClusterVersion;
    // Verify that we cannot reach the cluster
    process_command(&config_offline).unwrap_err();

    request_and_confirm_airdrop(
        &rpc_client,
        &config_offline,
        &config_offline.signers[0].pubkey(),
        100_000,
    )
    .unwrap();

    // Create stake account, identity is authority
    let stake_keypair = Keypair::new();
    let stake_account_pubkey = stake_keypair.pubkey();
    config.signers.push(&stake_keypair);
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: None,
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();

    // Assign new online stake authority
    let online_authority = Keypair::new();
    let online_authority_pubkey = online_authority.pubkey();
    config.signers.pop();
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: online_authority_pubkey,
            authority: 0,
            new_authority_signer: None,
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_authority = match stake_state {
        StakeState::Initialized(meta) => meta.authorized.staker,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(current_authority, online_authority_pubkey);

    // Assign new online stake and withdraw authorities
    let online_authority2 = Keypair::new();
    let online_authority2_pubkey = online_authority2.pubkey();
    let withdraw_authority = Keypair::new();
    let withdraw_authority_pubkey = withdraw_authority.pubkey();
    config.signers.push(&online_authority);
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![
            StakeAuthorizationIndexed {
                authorization_type: StakeAuthorize::Staker,
                new_authority_pubkey: online_authority2_pubkey,
                authority: 1,
                new_authority_signer: None,
            },
            StakeAuthorizationIndexed {
                authorization_type: StakeAuthorize::Withdrawer,
                new_authority_pubkey: withdraw_authority_pubkey,
                authority: 0,
                new_authority_signer: None,
            },
        ],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let (current_staker, current_withdrawer) = match stake_state {
        StakeState::Initialized(meta) => (meta.authorized.staker, meta.authorized.withdrawer),
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(current_staker, online_authority2_pubkey);
    assert_eq!(current_withdrawer, withdraw_authority_pubkey);

    // Assign new offline stake authority
    config.signers.pop();
    config.signers.push(&online_authority2);
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: offline_authority_pubkey,
            authority: 1,
            new_authority_signer: None,
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_authority = match stake_state {
        StakeState::Initialized(meta) => meta.authorized.staker,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(current_authority, offline_authority_pubkey);

    // Offline assignment of new nonced stake authority
    let nonced_authority = Keypair::new();
    let nonced_authority_pubkey = nonced_authority.pubkey();
    let (blockhash, _) = rpc_client.get_recent_blockhash().unwrap();
    config_offline.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: nonced_authority_pubkey,
            authority: 0,
            new_authority_signer: None,
        }],
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(blockhash),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    config_offline.output_format = OutputFormat::JsonCompact;
    let sign_reply = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sign_reply);
    assert!(sign_only.has_all_signers());
    let offline_presigner = sign_only.presigner_of(&offline_authority_pubkey).unwrap();
    config.signers = vec![&offline_presigner];
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: nonced_authority_pubkey,
            authority: 0,
            new_authority_signer: None,
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(blockhash_query::Source::Cluster, blockhash),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_authority = match stake_state {
        StakeState::Initialized(meta) => meta.authorized.staker,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(current_authority, nonced_authority_pubkey);

    // Create nonce account
    let minimum_nonce_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(NonceState::size())
        .unwrap();
    let nonce_account = Keypair::new();
    config.signers = vec![&default_signer, &nonce_account];
    config.command = CliCommand::CreateNonceAccount {
        nonce_account: 1,
        seed: None,
        nonce_authority: Some(offline_authority_pubkey),
        memo: None,
        amount: SpendAmount::Some(minimum_nonce_balance),
    };
    process_command(&config).unwrap();

    // Fetch nonce hash
    let nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;

    // Nonced assignment of new online stake authority
    let online_authority = Keypair::new();
    let online_authority_pubkey = online_authority.pubkey();
    config_offline.signers.push(&nonced_authority);
    config_offline.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: online_authority_pubkey,
            authority: 1,
            new_authority_signer: None,
        }],
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(nonce_hash),
        nonce_account: Some(nonce_account.pubkey()),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    let sign_reply = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sign_reply);
    assert!(sign_only.has_all_signers());
    assert_eq!(sign_only.blockhash, nonce_hash);
    let offline_presigner = sign_only.presigner_of(&offline_authority_pubkey).unwrap();
    let nonced_authority_presigner = sign_only.presigner_of(&nonced_authority_pubkey).unwrap();
    config.signers = vec![&offline_presigner, &nonced_authority_presigner];
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: online_authority_pubkey,
            authority: 1,
            new_authority_signer: None,
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(
            blockhash_query::Source::NonceAccount(nonce_account.pubkey()),
            sign_only.blockhash,
        ),
        nonce_account: Some(nonce_account.pubkey()),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_authority = match stake_state {
        StakeState::Initialized(meta) => meta.authorized.staker,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(current_authority, online_authority_pubkey);

    let new_nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;
    assert_ne!(nonce_hash, new_nonce_hash);
}

#[test]
fn test_stake_authorize_with_fee_payer() {
    solana_logger::setup();
    const SIG_FEE: u64 = 42;

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_custom_fees(mint_pubkey, SIG_FEE, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());
    let default_signer = Keypair::new();
    let default_pubkey = default_signer.pubkey();

    let mut config = CliConfig::recent_for_tests();
    config.json_rpc_url = test_validator.rpc_url();
    config.signers = vec![&default_signer];

    let payer_keypair = keypair_from_seed(&[0u8; 32]).unwrap();
    let mut config_payer = CliConfig::recent_for_tests();
    config_payer.signers = vec![&payer_keypair];
    config_payer.json_rpc_url = test_validator.rpc_url();
    let payer_pubkey = config_payer.signers[0].pubkey();

    let mut config_offline = CliConfig::recent_for_tests();
    let offline_signer = Keypair::new();
    config_offline.signers = vec![&offline_signer];
    config_offline.json_rpc_url = String::new();
    let offline_pubkey = config_offline.signers[0].pubkey();
    // Verify we're offline
    config_offline.command = CliCommand::ClusterVersion;
    process_command(&config_offline).unwrap_err();

    request_and_confirm_airdrop(&rpc_client, &config, &default_pubkey, 100_000).unwrap();
    check_recent_balance(100_000, &rpc_client, &config.signers[0].pubkey());

    request_and_confirm_airdrop(&rpc_client, &config_payer, &payer_pubkey, 100_000).unwrap();
    check_recent_balance(100_000, &rpc_client, &payer_pubkey);

    request_and_confirm_airdrop(&rpc_client, &config_offline, &offline_pubkey, 100_000).unwrap();
    check_recent_balance(100_000, &rpc_client, &offline_pubkey);

    check_ready(&rpc_client);

    // Create stake account, identity is authority
    let stake_keypair = Keypair::new();
    let stake_account_pubkey = stake_keypair.pubkey();
    config.signers.push(&stake_keypair);
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: None,
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();
    // `config` balance should be 50,000 - 1 stake account sig - 1 fee sig
    check_recent_balance(50_000 - SIG_FEE - SIG_FEE, &rpc_client, &default_pubkey);

    // Assign authority with separate fee payer
    config.signers = vec![&default_signer, &payer_keypair];
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: offline_pubkey,
            authority: 0,
            new_authority_signer: None,
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 1,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    // `config` balance has not changed, despite submitting the TX
    check_recent_balance(50_000 - SIG_FEE - SIG_FEE, &rpc_client, &default_pubkey);
    // `config_payer` however has paid `config`'s authority sig
    // and `config_payer`'s fee sig
    check_recent_balance(100_000 - SIG_FEE - SIG_FEE, &rpc_client, &payer_pubkey);

    // Assign authority with offline fee payer
    let (blockhash, _) = rpc_client.get_recent_blockhash().unwrap();
    config_offline.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: payer_pubkey,
            authority: 0,
            new_authority_signer: None,
        }],
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(blockhash),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    config_offline.output_format = OutputFormat::JsonCompact;
    let sign_reply = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sign_reply);
    assert!(sign_only.has_all_signers());
    let offline_presigner = sign_only.presigner_of(&offline_pubkey).unwrap();
    config.signers = vec![&offline_presigner];
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: payer_pubkey,
            authority: 0,
            new_authority_signer: None,
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(blockhash_query::Source::Cluster, blockhash),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    // `config`'s balance again has not changed
    check_recent_balance(50_000 - SIG_FEE - SIG_FEE, &rpc_client, &default_pubkey);
    // `config_offline` however has paid 1 sig due to being both authority
    // and fee payer
    check_recent_balance(100_000 - SIG_FEE, &rpc_client, &offline_pubkey);
}

#[test]
fn test_stake_split() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_custom_fees(mint_pubkey, 1, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());
    let default_signer = Keypair::new();
    let offline_signer = Keypair::new();

    let mut config = CliConfig::recent_for_tests();
    config.json_rpc_url = test_validator.rpc_url();
    config.signers = vec![&default_signer];

    let mut config_offline = CliConfig::recent_for_tests();
    config_offline.json_rpc_url = String::default();
    config_offline.signers = vec![&offline_signer];
    let offline_pubkey = config_offline.signers[0].pubkey();
    // Verify we're offline
    config_offline.command = CliCommand::ClusterVersion;
    process_command(&config_offline).unwrap_err();

    request_and_confirm_airdrop(&rpc_client, &config, &config.signers[0].pubkey(), 500_000)
        .unwrap();
    check_recent_balance(500_000, &rpc_client, &config.signers[0].pubkey());

    request_and_confirm_airdrop(&rpc_client, &config_offline, &offline_pubkey, 100_000).unwrap();
    check_recent_balance(100_000, &rpc_client, &offline_pubkey);

    // Create stake account, identity is authority
    let minimum_stake_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(std::mem::size_of::<StakeState>())
        .unwrap();
    let stake_keypair = keypair_from_seed(&[0u8; 32]).unwrap();
    let stake_account_pubkey = stake_keypair.pubkey();
    config.signers.push(&stake_keypair);
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: Some(offline_pubkey),
        withdrawer: Some(offline_pubkey),
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(10 * minimum_stake_balance),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();
    check_recent_balance(
        10 * minimum_stake_balance,
        &rpc_client,
        &stake_account_pubkey,
    );

    // Create nonce account
    let minimum_nonce_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(NonceState::size())
        .unwrap();
    let nonce_account = keypair_from_seed(&[1u8; 32]).unwrap();
    config.signers = vec![&default_signer, &nonce_account];
    config.command = CliCommand::CreateNonceAccount {
        nonce_account: 1,
        seed: None,
        nonce_authority: Some(offline_pubkey),
        memo: None,
        amount: SpendAmount::Some(minimum_nonce_balance),
    };
    process_command(&config).unwrap();
    check_recent_balance(minimum_nonce_balance, &rpc_client, &nonce_account.pubkey());

    // Fetch nonce hash
    let nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;

    // Nonced offline split
    let split_account = keypair_from_seed(&[2u8; 32]).unwrap();
    check_recent_balance(0, &rpc_client, &split_account.pubkey());
    config_offline.signers.push(&split_account);
    config_offline.command = CliCommand::SplitStake {
        stake_account_pubkey,
        stake_authority: 0,
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(nonce_hash),
        nonce_account: Some(nonce_account.pubkey()),
        nonce_authority: 0,
        memo: None,
        split_stake_account: 1,
        seed: None,
        lamports: 2 * minimum_stake_balance,
        fee_payer: 0,
    };
    config_offline.output_format = OutputFormat::JsonCompact;
    let sig_response = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sig_response);
    assert!(sign_only.has_all_signers());
    let offline_presigner = sign_only.presigner_of(&offline_pubkey).unwrap();
    config.signers = vec![&offline_presigner, &split_account];
    config.command = CliCommand::SplitStake {
        stake_account_pubkey,
        stake_authority: 0,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(
            blockhash_query::Source::NonceAccount(nonce_account.pubkey()),
            sign_only.blockhash,
        ),
        nonce_account: Some(nonce_account.pubkey()),
        nonce_authority: 0,
        memo: None,
        split_stake_account: 1,
        seed: None,
        lamports: 2 * minimum_stake_balance,
        fee_payer: 0,
    };
    process_command(&config).unwrap();
    check_recent_balance(
        8 * minimum_stake_balance,
        &rpc_client,
        &stake_account_pubkey,
    );
    check_recent_balance(
        2 * minimum_stake_balance,
        &rpc_client,
        &split_account.pubkey(),
    );
}

#[test]
fn test_stake_set_lockup() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_custom_fees(mint_pubkey, 1, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());
    let default_signer = Keypair::new();
    let offline_signer = Keypair::new();

    let mut config = CliConfig::recent_for_tests();
    config.json_rpc_url = test_validator.rpc_url();
    config.signers = vec![&default_signer];

    let mut config_offline = CliConfig::recent_for_tests();
    config_offline.json_rpc_url = String::default();
    config_offline.signers = vec![&offline_signer];
    let offline_pubkey = config_offline.signers[0].pubkey();
    // Verify we're offline
    config_offline.command = CliCommand::ClusterVersion;
    process_command(&config_offline).unwrap_err();

    request_and_confirm_airdrop(&rpc_client, &config, &config.signers[0].pubkey(), 500_000)
        .unwrap();
    check_recent_balance(500_000, &rpc_client, &config.signers[0].pubkey());

    request_and_confirm_airdrop(&rpc_client, &config_offline, &offline_pubkey, 100_000).unwrap();
    check_recent_balance(100_000, &rpc_client, &offline_pubkey);

    // Create stake account, identity is authority
    let minimum_stake_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(std::mem::size_of::<StakeState>())
        .unwrap();

    let stake_keypair = keypair_from_seed(&[0u8; 32]).unwrap();
    let stake_account_pubkey = stake_keypair.pubkey();

    let lockup = Lockup {
        custodian: config.signers[0].pubkey(),
        ..Lockup::default()
    };

    config.signers.push(&stake_keypair);
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: Some(offline_pubkey),
        withdrawer: Some(config.signers[0].pubkey()),
        withdrawer_signer: None,
        lockup,
        amount: SpendAmount::Some(10 * minimum_stake_balance),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();
    check_recent_balance(
        10 * minimum_stake_balance,
        &rpc_client,
        &stake_account_pubkey,
    );

    // Online set lockup
    let lockup = LockupArgs {
        unix_timestamp: Some(1_581_534_570),
        epoch: Some(200),
        custodian: None,
    };
    config.signers.pop();
    config.command = CliCommand::StakeSetLockup {
        stake_account_pubkey,
        lockup,
        new_custodian_signer: None,
        custodian: 0,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_lockup = match stake_state {
        StakeState::Initialized(meta) => meta.lockup,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(
        current_lockup.unix_timestamp,
        lockup.unix_timestamp.unwrap()
    );
    assert_eq!(current_lockup.epoch, lockup.epoch.unwrap());
    assert_eq!(current_lockup.custodian, config.signers[0].pubkey());

    // Set custodian to another pubkey
    let online_custodian = Keypair::new();
    let online_custodian_pubkey = online_custodian.pubkey();

    let lockup = LockupArgs {
        unix_timestamp: Some(1_581_534_571),
        epoch: Some(201),
        custodian: Some(online_custodian_pubkey),
    };
    config.command = CliCommand::StakeSetLockup {
        stake_account_pubkey,
        lockup,
        new_custodian_signer: None,
        custodian: 0,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();

    let lockup = LockupArgs {
        unix_timestamp: Some(1_581_534_572),
        epoch: Some(202),
        custodian: None,
    };
    config.signers = vec![&default_signer, &online_custodian];
    config.command = CliCommand::StakeSetLockup {
        stake_account_pubkey,
        lockup,
        new_custodian_signer: None,
        custodian: 1,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_lockup = match stake_state {
        StakeState::Initialized(meta) => meta.lockup,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(
        current_lockup.unix_timestamp,
        lockup.unix_timestamp.unwrap()
    );
    assert_eq!(current_lockup.epoch, lockup.epoch.unwrap());
    assert_eq!(current_lockup.custodian, online_custodian_pubkey);

    // Set custodian to offline pubkey
    let lockup = LockupArgs {
        unix_timestamp: Some(1_581_534_573),
        epoch: Some(203),
        custodian: Some(offline_pubkey),
    };
    config.command = CliCommand::StakeSetLockup {
        stake_account_pubkey,
        lockup,
        new_custodian_signer: None,
        custodian: 1,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();

    // Create nonce account
    let minimum_nonce_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(NonceState::size())
        .unwrap();
    let nonce_account = keypair_from_seed(&[1u8; 32]).unwrap();
    let nonce_account_pubkey = nonce_account.pubkey();
    config.signers = vec![&default_signer, &nonce_account];
    config.command = CliCommand::CreateNonceAccount {
        nonce_account: 1,
        seed: None,
        nonce_authority: Some(offline_pubkey),
        memo: None,
        amount: SpendAmount::Some(minimum_nonce_balance),
    };
    process_command(&config).unwrap();
    check_recent_balance(minimum_nonce_balance, &rpc_client, &nonce_account_pubkey);

    // Fetch nonce hash
    let nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;

    // Nonced offline set lockup
    let lockup = LockupArgs {
        unix_timestamp: Some(1_581_534_576),
        epoch: Some(222),
        custodian: None,
    };
    config_offline.command = CliCommand::StakeSetLockup {
        stake_account_pubkey,
        lockup,
        new_custodian_signer: None,
        custodian: 0,
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(nonce_hash),
        nonce_account: Some(nonce_account_pubkey),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    config_offline.output_format = OutputFormat::JsonCompact;
    let sig_response = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sig_response);
    assert!(sign_only.has_all_signers());
    let offline_presigner = sign_only.presigner_of(&offline_pubkey).unwrap();
    config.signers = vec![&offline_presigner];
    config.command = CliCommand::StakeSetLockup {
        stake_account_pubkey,
        lockup,
        new_custodian_signer: None,
        custodian: 0,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(
            blockhash_query::Source::NonceAccount(nonce_account_pubkey),
            sign_only.blockhash,
        ),
        nonce_account: Some(nonce_account_pubkey),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_lockup = match stake_state {
        StakeState::Initialized(meta) => meta.lockup,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(
        current_lockup.unix_timestamp,
        lockup.unix_timestamp.unwrap()
    );
    assert_eq!(current_lockup.epoch, lockup.epoch.unwrap());
    assert_eq!(current_lockup.custodian, offline_pubkey);
}

#[test]
fn test_offline_nonced_create_stake_account_and_withdraw() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_no_fees(mint_pubkey, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());
    let mut config = CliConfig::recent_for_tests();
    let default_signer = keypair_from_seed(&[1u8; 32]).unwrap();
    config.signers = vec![&default_signer];
    config.json_rpc_url = test_validator.rpc_url();

    let mut config_offline = CliConfig::recent_for_tests();
    let offline_signer = keypair_from_seed(&[2u8; 32]).unwrap();
    config_offline.signers = vec![&offline_signer];
    let offline_pubkey = config_offline.signers[0].pubkey();
    config_offline.json_rpc_url = String::default();
    config_offline.command = CliCommand::ClusterVersion;
    // Verify that we cannot reach the cluster
    process_command(&config_offline).unwrap_err();

    request_and_confirm_airdrop(&rpc_client, &config, &config.signers[0].pubkey(), 200_000)
        .unwrap();
    check_recent_balance(200_000, &rpc_client, &config.signers[0].pubkey());

    request_and_confirm_airdrop(&rpc_client, &config_offline, &offline_pubkey, 100_000).unwrap();
    check_recent_balance(100_000, &rpc_client, &offline_pubkey);

    // Create nonce account
    let minimum_nonce_balance = rpc_client
        .get_minimum_balance_for_rent_exemption(NonceState::size())
        .unwrap();
    let nonce_account = keypair_from_seed(&[3u8; 32]).unwrap();
    let nonce_pubkey = nonce_account.pubkey();
    config.signers.push(&nonce_account);
    config.command = CliCommand::CreateNonceAccount {
        nonce_account: 1,
        seed: None,
        nonce_authority: Some(offline_pubkey),
        memo: None,
        amount: SpendAmount::Some(minimum_nonce_balance),
    };
    process_command(&config).unwrap();

    // Fetch nonce hash
    let nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;

    // Create stake account offline
    let stake_keypair = keypair_from_seed(&[4u8; 32]).unwrap();
    let stake_pubkey = stake_keypair.pubkey();
    config_offline.signers.push(&stake_keypair);
    config_offline.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: None,
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(nonce_hash),
        nonce_account: Some(nonce_pubkey),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    config_offline.output_format = OutputFormat::JsonCompact;
    let sig_response = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sig_response);
    assert!(sign_only.has_all_signers());
    let offline_presigner = sign_only.presigner_of(&offline_pubkey).unwrap();
    let stake_presigner = sign_only.presigner_of(&stake_pubkey).unwrap();
    config.signers = vec![&offline_presigner, &stake_presigner];
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: Some(offline_pubkey),
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(
            blockhash_query::Source::NonceAccount(nonce_pubkey),
            sign_only.blockhash,
        ),
        nonce_account: Some(nonce_pubkey),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();
    check_recent_balance(50_000, &rpc_client, &stake_pubkey);

    // Fetch nonce hash
    let nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;

    // Offline, nonced stake-withdraw
    let recipient = keypair_from_seed(&[5u8; 32]).unwrap();
    let recipient_pubkey = recipient.pubkey();
    config_offline.signers.pop();
    config_offline.command = CliCommand::WithdrawStake {
        stake_account_pubkey: stake_pubkey,
        destination_account_pubkey: recipient_pubkey,
        amount: SpendAmount::Some(42),
        withdraw_authority: 0,
        custodian: None,
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(nonce_hash),
        nonce_account: Some(nonce_pubkey),
        nonce_authority: 0,
        memo: None,
        seed: None,
        fee_payer: 0,
    };
    let sig_response = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sig_response);
    let offline_presigner = sign_only.presigner_of(&offline_pubkey).unwrap();
    config.signers = vec![&offline_presigner];
    config.command = CliCommand::WithdrawStake {
        stake_account_pubkey: stake_pubkey,
        destination_account_pubkey: recipient_pubkey,
        amount: SpendAmount::Some(42),
        withdraw_authority: 0,
        custodian: None,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(
            blockhash_query::Source::NonceAccount(nonce_pubkey),
            sign_only.blockhash,
        ),
        nonce_account: Some(nonce_pubkey),
        nonce_authority: 0,
        memo: None,
        seed: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();
    check_recent_balance(42, &rpc_client, &recipient_pubkey);

    // Fetch nonce hash
    let nonce_hash = nonce_utils::get_account_with_commitment(
        &rpc_client,
        &nonce_account.pubkey(),
        CommitmentConfig::processed(),
    )
    .and_then(|ref a| nonce_utils::data_from_account(a))
    .unwrap()
    .blockhash;

    // Create another stake account. This time with seed
    let seed = "seedy";
    config_offline.signers = vec![&offline_signer, &stake_keypair];
    config_offline.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: Some(seed.to_string()),
        staker: None,
        withdrawer: None,
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: true,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::None(nonce_hash),
        nonce_account: Some(nonce_pubkey),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    let sig_response = process_command(&config_offline).unwrap();
    let sign_only = parse_sign_only_reply_string(&sig_response);
    let offline_presigner = sign_only.presigner_of(&offline_pubkey).unwrap();
    let stake_presigner = sign_only.presigner_of(&stake_pubkey).unwrap();
    config.signers = vec![&offline_presigner, &stake_presigner];
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: Some(seed.to_string()),
        staker: Some(offline_pubkey),
        withdrawer: Some(offline_pubkey),
        withdrawer_signer: None,
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::FeeCalculator(
            blockhash_query::Source::NonceAccount(nonce_pubkey),
            sign_only.blockhash,
        ),
        nonce_account: Some(nonce_pubkey),
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();
    let seed_address =
        Pubkey::create_with_seed(&stake_pubkey, seed, &stake::program::id()).unwrap();
    check_recent_balance(50_000, &rpc_client, &seed_address);
}

#[test]
fn test_stake_checked_instructions() {
    solana_logger::setup();

    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet(mint_keypair, None);
    let test_validator = TestValidator::with_no_fees(mint_pubkey, Some(faucet_addr));

    let rpc_client =
        RpcClient::new_with_commitment(test_validator.rpc_url(), CommitmentConfig::processed());
    let default_signer = Keypair::new();

    let mut config = CliConfig::recent_for_tests();
    config.json_rpc_url = test_validator.rpc_url();
    config.signers = vec![&default_signer];

    request_and_confirm_airdrop(&rpc_client, &config, &config.signers[0].pubkey(), 100_000)
        .unwrap();

    // Create stake account with withdrawer
    let stake_keypair = Keypair::new();
    let stake_account_pubkey = stake_keypair.pubkey();
    let withdrawer_keypair = Keypair::new();
    let withdrawer_pubkey = withdrawer_keypair.pubkey();
    config.signers.push(&stake_keypair);
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: None,
        withdrawer: Some(withdrawer_pubkey),
        withdrawer_signer: Some(1),
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap_err(); // unsigned authority should fail

    config.signers = vec![&default_signer, &stake_keypair, &withdrawer_keypair];
    config.command = CliCommand::CreateStakeAccount {
        stake_account: 1,
        seed: None,
        staker: None,
        withdrawer: Some(withdrawer_pubkey),
        withdrawer_signer: Some(1),
        lockup: Lockup::default(),
        amount: SpendAmount::Some(50_000),
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        from: 0,
    };
    process_command(&config).unwrap();

    // Re-authorize account, checking new authority
    let staker_keypair = Keypair::new();
    let staker_pubkey = staker_keypair.pubkey();
    config.signers = vec![&default_signer];
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: staker_pubkey,
            authority: 0,
            new_authority_signer: Some(0),
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap_err(); // unsigned authority should fail

    config.signers = vec![&default_signer, &staker_keypair];
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Staker,
            new_authority_pubkey: staker_pubkey,
            authority: 0,
            new_authority_signer: Some(1),
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_authority = match stake_state {
        StakeState::Initialized(meta) => meta.authorized.staker,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(current_authority, staker_pubkey);

    let new_withdrawer_keypair = Keypair::new();
    let new_withdrawer_pubkey = new_withdrawer_keypair.pubkey();
    config.signers = vec![&default_signer, &withdrawer_keypair];
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Withdrawer,
            new_authority_pubkey: new_withdrawer_pubkey,
            authority: 1,
            new_authority_signer: Some(1),
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap_err(); // unsigned authority should fail

    config.signers = vec![
        &default_signer,
        &withdrawer_keypair,
        &new_withdrawer_keypair,
    ];
    config.command = CliCommand::StakeAuthorize {
        stake_account_pubkey,
        new_authorizations: vec![StakeAuthorizationIndexed {
            authorization_type: StakeAuthorize::Withdrawer,
            new_authority_pubkey: new_withdrawer_pubkey,
            authority: 1,
            new_authority_signer: Some(2),
        }],
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
        custodian: None,
        no_wait: false,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_authority = match stake_state {
        StakeState::Initialized(meta) => meta.authorized.withdrawer,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(current_authority, new_withdrawer_pubkey);

    // Set lockup, checking new custodian
    let custodian = Keypair::new();
    let custodian_pubkey = custodian.pubkey();
    let lockup = LockupArgs {
        unix_timestamp: Some(1_581_534_570),
        epoch: Some(200),
        custodian: Some(custodian_pubkey),
    };
    config.signers = vec![&default_signer, &new_withdrawer_keypair];
    config.command = CliCommand::StakeSetLockup {
        stake_account_pubkey,
        lockup,
        new_custodian_signer: Some(1),
        custodian: 1,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap_err(); // unsigned new custodian should fail

    config.signers = vec![&default_signer, &new_withdrawer_keypair, &custodian];
    config.command = CliCommand::StakeSetLockup {
        stake_account_pubkey,
        lockup,
        new_custodian_signer: Some(2),
        custodian: 1,
        sign_only: false,
        dump_transaction_message: false,
        blockhash_query: BlockhashQuery::default(),
        nonce_account: None,
        nonce_authority: 0,
        memo: None,
        fee_payer: 0,
    };
    process_command(&config).unwrap();
    let stake_account = rpc_client.get_account(&stake_account_pubkey).unwrap();
    let stake_state: StakeState = stake_account.state().unwrap();
    let current_lockup = match stake_state {
        StakeState::Initialized(meta) => meta.lockup,
        _ => panic!("Unexpected stake state!"),
    };
    assert_eq!(
        current_lockup.unix_timestamp,
        lockup.unix_timestamp.unwrap()
    );
    assert_eq!(current_lockup.epoch, lockup.epoch.unwrap());
    assert_eq!(current_lockup.custodian, custodian_pubkey);
}
