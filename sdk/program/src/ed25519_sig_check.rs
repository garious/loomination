use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Ed25519SigCheckError {
    #[error("The public key provided to ed25519_sig_check is invalid")]
    InvalidPublicKey,
    #[error("The signature provided to ed25519_sig_check is invalid")]
    InvalidSignature,
    #[error("The signature could be verified")]
    VerifyFailed,
}

impl From<u64> for Ed25519SigCheckError {
    fn from(v: u64) -> Ed25519SigCheckError {
        match v {
            1 => Ed25519SigCheckError::InvalidPublicKey,
            2 => Ed25519SigCheckError::InvalidSignature,
            3 => Ed25519SigCheckError::VerifyFailed,
            _ => panic!("Unsupported Ed25519SigCheckError"),
        }
    }
}

impl From<Ed25519SigCheckError> for u64 {
    fn from(v: Ed25519SigCheckError) -> u64 {
        match v {
            Ed25519SigCheckError::InvalidPublicKey => 1,
            Ed25519SigCheckError::InvalidSignature => 2,
            Ed25519SigCheckError::VerifyFailed => 3,
        }
    }
}

pub fn ed25519_sig_check(
    message: &[u8],
    signature: &[u8],
    publickey: &[u8],
) -> Result<(), Ed25519SigCheckError> {
    #[cfg(target_arch = "bpf")]
    {
        extern "C" {
            fn sol_ed25519_sig_check(
                message: *const u8,
                message_len: u64,
                signature: *const u8,
                publickey: *mut u8,
            ) -> u64;
        }

        let result = unsafe {
            sol_secp256k1_recover(
                message.as_ptr(),
                message.len() as u64,
                signature.as_ptr(),
                publickey.as_mut_ptr(),
            )
        };

        match result {
            0 => Ok(()),
            error => Err(Ed25519SigCheckError::from(error)),
        }
    }

    #[cfg(not(target_arch = "bpf"))]
    {
        use ed25519_dalek::{ed25519::signature::Signature, Verifier};

        let signature = ed25519_dalek::Signature::from_bytes(signature)
            .map_err(|_| Ed25519SigCheckError::InvalidSignature)?;

        let publickey = ed25519_dalek::PublicKey::from_bytes(publickey)
            .map_err(|_| Ed25519SigCheckError::InvalidPublicKey)?;

        publickey
            .verify(message, &signature)
            .map_err(|_| Ed25519SigCheckError::VerifyFailed)
    }
}
