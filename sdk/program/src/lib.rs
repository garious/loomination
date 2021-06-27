#![allow(incomplete_features)]
#![cfg_attr(RUSTC_WITH_SPECIALIZATION, feature(specialization))]
#![cfg_attr(RUSTC_NEEDS_PROC_MACRO_HYGIENE, feature(proc_macro_hygiene))]

// Allows macro expansion of `use ::solana_program::*` to work within this crate
extern crate self as solana_program;

pub mod account_info;
pub mod blake3;
pub mod borsh;
pub mod bpf_loader;
pub mod bpf_loader_deprecated;
pub mod bpf_loader_upgradeable;
pub mod clock;
pub mod decode_error;
pub mod ed25519_sig_check;
pub mod entrypoint;
pub mod entrypoint_deprecated;
pub mod epoch_schedule;
pub mod feature;
pub mod fee_calculator;
pub mod hash;
pub mod incinerator;
pub mod instruction;
pub mod keccak;
pub mod lamports;
pub mod loader_instruction;
pub mod loader_upgradeable_instruction;
pub mod log;
pub mod message;
pub mod native_token;
pub mod nonce;
pub mod program;
pub mod program_error;
pub mod program_memory;
pub mod program_option;
pub mod program_pack;
pub mod program_stubs;
pub mod pubkey;
pub mod rent;
pub mod sanitize;
pub mod secp256k1_program;
pub mod serialize_utils;
pub mod short_vec;
pub mod slot_hashes;
pub mod slot_history;
pub mod stake;
pub mod stake_history;
pub mod system_instruction;
pub mod system_program;
pub mod sysvar;

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
/// use solana_program::{declare_id, pubkey::Pubkey};
///
/// # mod item_wrapper {
/// #   use solana_program::declare_id;
/// declare_id!("My11111111111111111111111111111111111111111");
/// # }
/// # use item_wrapper::id;
///
/// let my_id = Pubkey::from_str("My11111111111111111111111111111111111111111").unwrap();
/// assert_eq!(id(), my_id);
/// ```
pub use solana_sdk_macro::program_declare_id as declare_id;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate solana_frozen_abi_macro;

/// Convenience macro for doing integer division where the opersation's safety
/// can be checked at compile-time
///
/// Since `unchecked_div_by_const!()` is supposed to fail at compile-time, abuse
/// doctests to cover failure modes
/// Literal denominator div-by-zero fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # let _ = unchecked_div_by_const!(10, 0);
/// # }
/// ```
/// #
/// # Const denominator div-by-zero fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # const D: u64 = 0;
/// # let _ = unchecked_div_by_const!(10, D);
/// # }
/// ```
/// #
/// # Non-const denominator fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # let d = 0;
/// # let _ = unchecked_div_by_const!(10, d);
/// # }
/// ```
/// #
/// Literal denominator div-by-zero fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # const N: u64 = 10;
/// # let _ = unchecked_div_by_const!(N, 0);
/// # }
/// ```
/// #
/// # Const denominator div-by-zero fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # const N: u64 = 10;
/// # const D: u64 = 0;
/// # let _ = unchecked_div_by_const!(N, D);
/// # }
/// ```
/// #
/// # Non-const denominator fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # const N: u64 = 10;
/// # let d = 0;
/// # let _ = unchecked_div_by_const!(N, d);
/// # }
/// ```
/// #
/// Literal denominator div-by-zero fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # let n = 10;
/// # let _ = unchecked_div_by_const!(n, 0);
/// # }
/// ```
/// #
/// # Const denominator div-by-zero fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # let n = 10;
/// # const D: u64 = 0;
/// # let _ = unchecked_div_by_const!(n, D);
/// # }
/// ```
/// #
/// # Non-const denominator fails
/// ```compile_fail
/// # use solana_program::unchecked_div_by_const;
/// # fn main() {
/// # let n = 10;
/// # let d = 0;
/// # let _ = unchecked_div_by_const!(n, d);
/// # }
/// ```
/// #
#[macro_export]
macro_rules! unchecked_div_by_const {
    ($num:expr, $den:expr) => {{
        // Ensure the denominator is compile-time constant
        let _ = [(); ($den - $den) as usize];
        // Compile-time constant integer div-by-zero passes for some reason
        // when invoked from a compilation unit other than that where this
        // macro is defined. Do an explicit zero-check for now. Sorry about the
        // ugly error messages!
        // https://users.rust-lang.org/t/unexpected-behavior-of-compile-time-integer-div-by-zero-check-in-declarative-macro/56718
        let _ = [(); ($den as usize) - 1];
        #[allow(clippy::integer_arithmetic)]
        let quotient = $num / $den;
        quotient
    }};
}

#[cfg(test)]
mod tests {
    use super::unchecked_div_by_const;

    #[test]
    fn test_unchecked_div_by_const() {
        const D: u64 = 2;
        const N: u64 = 10;
        let n = 10;
        assert_eq!(unchecked_div_by_const!(10, 2), 5);
        assert_eq!(unchecked_div_by_const!(N, 2), 5);
        assert_eq!(unchecked_div_by_const!(n, 2), 5);
        assert_eq!(unchecked_div_by_const!(10, D), 5);
        assert_eq!(unchecked_div_by_const!(N, D), 5);
        assert_eq!(unchecked_div_by_const!(n, D), 5);
    }
}
