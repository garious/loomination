#![allow(incomplete_features)]
#![cfg_attr(RUSTC_WITH_SPECIALIZATION, feature(specialization))]
#![cfg_attr(RUSTC_NEEDS_PROC_MACRO_HYGIENE, feature(proc_macro_hygiene))]

// Allows macro expansion of `use ::solana_sdk::*` to work within this crate
extern crate self as solana_sdk;

#[cfg(feature = "full")]
pub use signer::signers;
pub use solana_program::*;

pub mod account;
pub mod account_utils;
pub mod builtins;
pub mod client;
pub mod commitment_config;
pub mod derivation_path;
pub mod deserialize_utils;
pub mod entrypoint;
pub mod entrypoint_deprecated;
pub mod entrypoint_native;
pub mod epoch_info;
pub mod exit;
pub mod feature;
pub mod feature_set;
pub mod genesis_config;
pub mod hard_forks;
pub mod hash;
pub mod hashed_transaction;
pub mod inflation;
pub mod keyed_account;
pub mod log;
pub mod native_loader;
pub mod nonce_account;
pub mod nonce_keyed_account;
pub mod packet;
pub mod poh_config;
pub mod process_instruction;
pub mod program_utils;
pub mod pubkey;
pub mod recent_blockhashes_account;
pub mod rpc_port;
pub mod secp256k1_instruction;
pub mod shred_version;
pub mod signature;
pub mod signer;
pub mod system_transaction;
pub mod timing;
pub mod transaction;
pub mod transport;

/// Convenience macro to declare a static public key and functions to interact with it
///
/// Input: a single literal base58 string representation of a program's id
///
/// # Example
///
/// ```
/// # // wrapper is used so that the macro invocation occurs in the item position
/// # // rather than in the statement position which isn't allowed.
/// use std::str::FromStr;
/// use solana_sdk::{declare_id, pubkey::Pubkey};
///
/// # mod item_wrapper {
/// #   use solana_sdk::declare_id;
/// declare_id!("My11111111111111111111111111111111111111111");
/// # }
/// # use item_wrapper::id;
///
/// let my_id = Pubkey::from_str("My11111111111111111111111111111111111111111").unwrap();
/// assert_eq!(id(), my_id);
/// ```
pub use solana_sdk_macro::declare_id;
pub use solana_sdk_macro::pubkeys;
#[rustversion::since(1.46.0)]
pub use solana_sdk_macro::respan;

// Unused `solana_sdk::program_stubs!()` macro retained for source backwards compatibility with older programs
#[macro_export]
#[deprecated(
    since = "1.4.3",
    note = "program_stubs macro is obsolete and can be safely removed"
)]
macro_rules! program_stubs {
    () => {};
}

#[macro_use]
extern crate serde_derive;
pub extern crate bs58;
extern crate log as logger;

#[macro_use]
extern crate solana_frozen_abi_macro;
