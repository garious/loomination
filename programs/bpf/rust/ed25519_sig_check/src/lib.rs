//! @brief ed25519_sig_check Syscall test

extern crate solana_program;
use solana_program::{custom_panic_default, msg};

fn test_ed25519_sig_check() {
    use solana_program::ed25519_sig_check::ed25519_sig_check;

    let public_key: &[u8] = b"ec172b93ad5e563bf4932c70e1245034c35467ef2efd4d64ebf819683467e2bf";
    let message: &[u8] = b"616263";
    let signature: &[u8] = b"98a70222f0b8121aa9d30f813d683f809e462b469c7ff87639499bb94e6dae4131f85042463c2a355a2003d062adf5aaa10b8c61e636062aaad11c2a26083406";

    ed25519_sig_check(message, signature, public_key).expect("sig check should succeed");
}

#[no_mangle]
pub extern "C" fn entrypoint(_input: *mut u8) -> u64 {
    msg!("ed25519_sig_check");

    test_ed25519_sig_check();

    0
}

custom_panic_default!();
