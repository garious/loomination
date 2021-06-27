#![feature(test)]
extern crate test;

use ed25519_dalek::{Keypair, PublicKey, Signature, Signer, Verifier};
use test::Bencher;

#[bench]
fn ed25519_sig_check(bencher: &mut Bencher) {
    let mut csprng = rand::thread_rng();
    let keypair: Keypair = Keypair::generate(&mut csprng);

    let message: &[u8] =
        b"This is a test of the ed25519 sig check for the sol_ed25519_sig_check syscall";

    let signature: Signature = keypair.sign(message);

    let public_key: PublicKey = keypair.public;

    bencher.iter(|| {
        assert!(public_key.verify(message, &signature).is_ok());
    });
}
