use crate::{fee_calculator::FeeCalculator, hash::Hash};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Fees {
    pub blockhash: Hash,
    pub fee_calculator: FeeCalculator,
    pub last_valid_block_height: u64,
}
