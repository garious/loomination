use {
    bincode::{deserialize, serialize_into, serialized_size},
    serde_derive::{Deserialize, Serialize},
    solana_sdk::{
        account::WritableAccount,
        account_utils::State,
        clock::Slot,
        instruction::{AccountMeta, Instruction, InstructionError},
        keyed_account::keyed_account_at_index,
        process_instruction::{get_sysvar, InvokeContext},
        program_utils::limited_deserialize,
        pubkey::{self, Pubkey},
        rent::Rent,
        system_instruction,
        sysvar::{self, clock::Clock},
    },
};

// NodeInstance program identifier
solana_sdk::declare_id!("Node1nstance1111111111111111111111111111111");

pub type InstanceId = u64;

#[frozen_abi(digest = "AiiUDTocFRGuH18iv5PNPf2CnkMchpzqQsdAaun2WUW7")]
#[derive(Serialize, Debug, Deserialize, Default, PartialEq, AbiExample)]
pub struct NodeInstanceState {
    pub identity: Pubkey,
    pub instance_id: InstanceId,
    pub slot: Slot, // Slot that `instance_id` acquired the lock
}

impl NodeInstanceState {
    fn get_rent_exempt_balance(rent: &Rent) -> u64 {
        rent.minimum_balance(Self::size_of() as usize)
    }

    pub fn size_of() -> u64 {
        serialized_size(&Self::default()).unwrap()
    }

    pub fn deserialize(input: &[u8]) -> Result<Self, InstructionError> {
        deserialize::<NodeInstanceState>(input).map_err(|_| InstructionError::InvalidAccountData)
    }

    pub fn serialize(&self, output: &mut [u8]) -> Result<(), InstructionError> {
        serialize_into(output, self).map_err(|_| InstructionError::GenericError)
    }
}

#[derive(Serialize, Deserialize)]
pub enum NodeInstanceInstruction {
    /// Acquire the lock
    ///
    /// This instruction will succeed if:
    /// 1. The NodeInstance account is uninitialized, or
    /// 2. The NodeInstance account is initialized, and the provided identity matches the account
    ///    identity, and the provided instance id does not match the instance id currently in the
    ///    account
    ///
    /// On success, the `NodeInstanceState::instance_id` and `NodeInstanceState::slot` account
    /// fields are updated.
    ///
    /// # Account references
    ///   0. [WRITE] NodeInstance to acquire
    ///   2. [SIGNER] Identity
    Acquire(InstanceId),

    /// Deallocate the account and refund the lamports it holds to the identity
    ///
    /// # Account references
    ///   0. [WRITE] NodeInstance to close
    ///   1. [SIGNER] Identity
    Close,
}

pub fn process_instruction(
    _program_id: &Pubkey,
    data: &[u8],
    invoke_context: &mut dyn InvokeContext,
) -> Result<(), InstructionError> {
    let keyed_accounts = invoke_context.get_keyed_accounts()?;

    let node_instance_account = keyed_account_at_index(keyed_accounts, 0)?;
    if node_instance_account.owner()? != id() {
        return Err(InstructionError::InvalidAccountOwner);
    }

    // Identity must be a signer
    let identity_account = keyed_account_at_index(keyed_accounts, 1)?;
    let identity = *identity_account
        .signer_key()
        .ok_or(InstructionError::MissingRequiredSignature)?;

    let node_instance_state = State::<NodeInstanceState>::state(node_instance_account)?;
    if node_instance_state != NodeInstanceState::default() {
        // Ensure the provided identity matches if the account is initialized
        if node_instance_state.identity != identity {
            return Err(InstructionError::InvalidArgument);
        }
    }

    match limited_deserialize(data)? {
        NodeInstanceInstruction::Acquire(instance_id) => {
            if instance_id == node_instance_state.instance_id {
                // Already acquired
                return Err(InstructionError::Custom(1));
            }
            if node_instance_state.slot == Slot::MAX {
                // Account closed
                return Err(InstructionError::Custom(2));
            }
            node_instance_account.set_state(&NodeInstanceState {
                identity,
                instance_id,
                slot: get_sysvar::<Clock>(invoke_context, &sysvar::clock::id())?.slot,
            })
        }
        NodeInstanceInstruction::Close => {
            // Return account lamports to the identity
            identity_account
                .try_account_ref_mut()?
                .checked_add_lamports(node_instance_account.lamports()?)?;
            node_instance_account.try_account_ref_mut()?.set_lamports(0);

            // Mark the account as unusable by all
            node_instance_account.set_state(&NodeInstanceState {
                slot: Slot::MAX,
                ..NodeInstanceState::default()
            })
        }
    }
}

fn node_instance_address_seed(identity: &Pubkey) -> String {
    let seed = format!("{:.32}", identity.to_string());
    assert_eq!(seed.len(), pubkey::MAX_SEED_LEN);
    seed
}

/// Client-side convention to derive the NodeInstance address from an identity
pub fn get_node_instance_address(identity: &Pubkey) -> Pubkey {
    Pubkey::create_with_seed(identity, &node_instance_address_seed(identity), &id()).unwrap()
}

pub fn create_and_acquire(
    identity: &Pubkey,
    instance_id: InstanceId,
    rent: &Rent,
) -> Vec<Instruction> {
    let node_instance = get_node_instance_address(identity);
    let account_metas = vec![
        AccountMeta::new(node_instance, false),
        AccountMeta::new_readonly(*identity, true),
    ];

    vec![
        system_instruction::create_account_with_seed(
            identity,
            &node_instance,
            identity,
            &node_instance_address_seed(identity),
            NodeInstanceState::get_rent_exempt_balance(rent),
            NodeInstanceState::size_of(),
            &id(),
        ),
        Instruction::new_with_bincode(
            id(),
            &NodeInstanceInstruction::Acquire(instance_id),
            account_metas,
        ),
    ]
}

pub fn acquire(identity: &Pubkey, instance_id: InstanceId) -> Instruction {
    let node_instance = get_node_instance_address(identity);
    let account_metas = vec![
        AccountMeta::new(node_instance, false),
        AccountMeta::new_readonly(*identity, true),
    ];
    Instruction::new_with_bincode(
        id(),
        &NodeInstanceInstruction::Acquire(instance_id),
        account_metas,
    )
}

pub fn close(identity: &Pubkey) -> Instruction {
    let node_instance = get_node_instance_address(identity);
    let account_metas = vec![
        AccountMeta::new(node_instance, false),
        AccountMeta::new_readonly(*identity, true),
    ];
    Instruction::new_with_bincode(id(), &NodeInstanceInstruction::Close, account_metas)
}
