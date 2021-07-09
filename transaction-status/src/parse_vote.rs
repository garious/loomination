use crate::parse_instruction::{
    check_num_accounts, ParsableProgram, ParseInstructionError, ParsedInstructionEnum,
};
use bincode::deserialize;
use serde_json::json;
use solana_sdk::{instruction::CompiledInstruction, pubkey::Pubkey};
use solana_vote_program::vote_instruction::VoteInstruction;

pub fn parse_vote(
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<ParsedInstructionEnum, ParseInstructionError> {
    let vote_instruction: VoteInstruction = deserialize(&instruction.data)
        .map_err(|_| ParseInstructionError::InstructionNotParsable(ParsableProgram::Vote))?;
    match instruction.accounts.iter().max() {
        Some(index) if (*index as usize) < account_keys.len() => {}
        _ => {
            // Runtime should prevent this from ever happening
            return Err(ParseInstructionError::InstructionKeyMismatch(
                ParsableProgram::Vote,
            ));
        }
    }
    match vote_instruction {
        VoteInstruction::InitializeAccount(vote_init) => {
            check_num_vote_accounts(&instruction.accounts, 4)?;
            Ok(ParsedInstructionEnum {
                instruction_type: "initialize".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "rentSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                    "node": account_keys[instruction.accounts[3] as usize].to_string(),
                    "authorizedVoter": vote_init.authorized_voter.to_string(),
                    "authorizedWithdrawer": vote_init.authorized_withdrawer.to_string(),
                    "commission": vote_init.commission,
                }),
            })
        }
        VoteInstruction::Authorize(new_authorized, authority_type) => {
            check_num_vote_accounts(&instruction.accounts, 3)?;
            Ok(ParsedInstructionEnum {
                instruction_type: "authorize".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                    "authority": account_keys[instruction.accounts[2] as usize].to_string(),
                    "newAuthority": new_authorized.to_string(),
                    "authorityType": authority_type,
                }),
            })
        }
        VoteInstruction::Vote(vote) => {
            check_num_vote_accounts(&instruction.accounts, 4)?;
            let vote = json!({
                "slots": vote.slots,
                "hash": vote.hash.to_string(),
                "timestamp": vote.timestamp,
            });
            Ok(ParsedInstructionEnum {
                instruction_type: "vote".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "slotHashesSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                    "voteAuthority": account_keys[instruction.accounts[3] as usize].to_string(),
                    "vote": vote,
                }),
            })
        }
        VoteInstruction::Withdraw(lamports) => {
            check_num_vote_accounts(&instruction.accounts, 3)?;
            Ok(ParsedInstructionEnum {
                instruction_type: "withdraw".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "destination": account_keys[instruction.accounts[1] as usize].to_string(),
                    "withdrawAuthority": account_keys[instruction.accounts[2] as usize].to_string(),
                    "lamports": lamports,
                }),
            })
        }
        VoteInstruction::UpdateValidatorIdentity => {
            check_num_vote_accounts(&instruction.accounts, 3)?;
            Ok(ParsedInstructionEnum {
                instruction_type: "updateValidatorIdentity".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "newValidatorIdentity": account_keys[instruction.accounts[1] as usize].to_string(),
                    "withdrawAuthority": account_keys[instruction.accounts[2] as usize].to_string(),
                }),
            })
        }
        VoteInstruction::UpdateCommission(commission) => {
            check_num_vote_accounts(&instruction.accounts, 2)?;
            Ok(ParsedInstructionEnum {
                instruction_type: "updateCommission".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "withdrawAuthority": account_keys[instruction.accounts[1] as usize].to_string(),
                    "commission": commission,
                }),
            })
        }
        VoteInstruction::VoteSwitch(vote, hash) => {
            check_num_vote_accounts(&instruction.accounts, 4)?;
            let vote = json!({
                "slots": vote.slots,
                "hash": vote.hash.to_string(),
                "timestamp": vote.timestamp,
            });
            Ok(ParsedInstructionEnum {
                instruction_type: "voteSwitch".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "slotHashesSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                    "voteAuthority": account_keys[instruction.accounts[3] as usize].to_string(),
                    "vote": vote,
                    "hash": hash.to_string(),
                }),
            })
        }
        VoteInstruction::AuthorizeChecked(authority_type) => {
            check_num_vote_accounts(&instruction.accounts, 4)?;
            Ok(ParsedInstructionEnum {
                instruction_type: "authorizeChecked".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                    "authority": account_keys[instruction.accounts[2] as usize].to_string(),
                    "newAuthority": account_keys[instruction.accounts[3] as usize].to_string(),
                    "authorityType": authority_type,
                }),
            })
        }
        VoteInstruction::VoteWithInstance(vote, instance_id) => {
            check_num_vote_accounts(&instruction.accounts, 3)?;
            let vote = json!({
                "slots": vote.slots,
                "hash": vote.hash.to_string(),
                "timestamp": vote.timestamp,
            });
            Ok(ParsedInstructionEnum {
                instruction_type: "voteWithInstance".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "voteAuthority": account_keys[instruction.accounts[1] as usize].to_string(),
                    "nodeInstance": account_keys[instruction.accounts[2] as usize].to_string(),
                    "vote": vote,
                    "instanceId": instance_id,
                }),
            })
        }
        VoteInstruction::VoteSwitchWithInstance(vote, instance_id, hash) => {
            check_num_vote_accounts(&instruction.accounts, 3)?;
            let vote = json!({
                "slots": vote.slots,
                "hash": vote.hash.to_string(),
                "timestamp": vote.timestamp,
            });
            Ok(ParsedInstructionEnum {
                instruction_type: "voteSwitchWithInstance".to_string(),
                info: json!({
                    "voteAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "voteAuthority": account_keys[instruction.accounts[1] as usize].to_string(),
                    "nodeInstance": account_keys[instruction.accounts[2] as usize].to_string(),
                    "vote": vote,
                    "instanceId": instance_id,
                    "hash": hash.to_string(),
                }),
            })
        }
    }
}

fn check_num_vote_accounts(accounts: &[u8], num: usize) -> Result<(), ParseInstructionError> {
    check_num_accounts(accounts, num, ParsableProgram::Vote)
}

#[cfg(test)]
mod test {
    use super::*;
    use solana_sdk::{hash::Hash, message::Message, pubkey::Pubkey};
    use solana_vote_program::{
        vote_instruction,
        vote_state::{Vote, VoteAuthorize, VoteInit},
    };

    #[test]
    #[allow(clippy::same_item_push)]
    fn test_parse_vote_instruction() {
        let mut keys: Vec<Pubkey> = vec![];
        for _ in 0..5 {
            keys.push(solana_sdk::pubkey::new_rand());
        }

        let lamports = 55;
        let hash = Hash([1; 32]);
        let vote = Vote {
            slots: vec![1, 2, 4],
            hash,
            timestamp: Some(1_234_567_890),
        };

        let commission = 10;
        let authorized_voter = solana_sdk::pubkey::new_rand();
        let authorized_withdrawer = solana_sdk::pubkey::new_rand();
        let vote_init = VoteInit {
            node_pubkey: keys[2],
            authorized_voter,
            authorized_withdrawer,
            commission,
        };

        let instructions = vote_instruction::create_account(
            &solana_sdk::pubkey::new_rand(),
            &keys[1],
            &vote_init,
            lamports,
        );
        let message = Message::new(&instructions, None);
        assert_eq!(
            parse_vote(&message.instructions[1], &keys[0..5]).unwrap(),
            ParsedInstructionEnum {
                instruction_type: "initialize".to_string(),
                info: json!({
                    "voteAccount": keys[1].to_string(),
                    "rentSysvar": keys[3].to_string(),
                    "clockSysvar": keys[4].to_string(),
                    "node": keys[2].to_string(),
                    "authorizedVoter": authorized_voter.to_string(),
                    "authorizedWithdrawer": authorized_withdrawer.to_string(),
                    "commission": commission,
                }),
            }
        );
        assert!(parse_vote(&message.instructions[1], &keys[0..3]).is_err());

        let authority_type = VoteAuthorize::Voter;
        let instruction = vote_instruction::authorize(&keys[1], &keys[0], &keys[3], authority_type);
        let message = Message::new(&[instruction], None);
        assert_eq!(
            parse_vote(&message.instructions[0], &keys[0..3]).unwrap(),
            ParsedInstructionEnum {
                instruction_type: "authorize".to_string(),
                info: json!({
                    "voteAccount": keys[1].to_string(),
                    "clockSysvar": keys[2].to_string(),
                    "authority": keys[0].to_string(),
                    "newAuthority": keys[3].to_string(),
                    "authorityType": authority_type,
                }),
            }
        );
        assert!(parse_vote(&message.instructions[0], &keys[0..2]).is_err());

        let instruction = vote_instruction::vote(&keys[1], &keys[0], vote.clone());
        let message = Message::new(&[instruction], None);
        assert_eq!(
            parse_vote(&message.instructions[0], &keys[0..4]).unwrap(),
            ParsedInstructionEnum {
                instruction_type: "vote".to_string(),
                info: json!({
                    "voteAccount": keys[1].to_string(),
                    "slotHashesSysvar": keys[2].to_string(),
                    "clockSysvar": keys[3].to_string(),
                    "voteAuthority": keys[0].to_string(),
                    "vote": {
                        "slots": [1, 2, 4],
                        "hash": hash.to_string(),
                        "timestamp": 1_234_567_890,
                    },
                }),
            }
        );
        assert!(parse_vote(&message.instructions[0], &keys[0..3]).is_err());

        let instruction = vote_instruction::withdraw(&keys[1], &keys[0], lamports, &keys[2]);
        let message = Message::new(&[instruction], None);
        assert_eq!(
            parse_vote(&message.instructions[0], &keys[0..3]).unwrap(),
            ParsedInstructionEnum {
                instruction_type: "withdraw".to_string(),
                info: json!({
                    "voteAccount": keys[1].to_string(),
                    "destination": keys[2].to_string(),
                    "withdrawAuthority": keys[0].to_string(),
                    "lamports": lamports,
                }),
            }
        );
        assert!(parse_vote(&message.instructions[0], &keys[0..2]).is_err());

        let instruction = vote_instruction::update_validator_identity(&keys[2], &keys[1], &keys[0]);
        let message = Message::new(&[instruction], None);
        assert_eq!(
            parse_vote(&message.instructions[0], &keys[0..3]).unwrap(),
            ParsedInstructionEnum {
                instruction_type: "updateValidatorIdentity".to_string(),
                info: json!({
                    "voteAccount": keys[2].to_string(),
                    "newValidatorIdentity": keys[0].to_string(),
                    "withdrawAuthority": keys[1].to_string(),
                }),
            }
        );
        assert!(parse_vote(&message.instructions[0], &keys[0..2]).is_err());

        let instruction = vote_instruction::update_commission(&keys[1], &keys[0], commission);
        let message = Message::new(&[instruction], None);
        assert_eq!(
            parse_vote(&message.instructions[0], &keys[0..2]).unwrap(),
            ParsedInstructionEnum {
                instruction_type: "updateCommission".to_string(),
                info: json!({
                    "voteAccount": keys[1].to_string(),
                    "withdrawAuthority": keys[0].to_string(),
                    "commission": commission,
                }),
            }
        );
        assert!(parse_vote(&message.instructions[0], &keys[0..1]).is_err());

        let proof_hash = Hash([2; 32]);
        let instruction = vote_instruction::vote_switch(&keys[1], &keys[0], vote, proof_hash);
        let message = Message::new(&[instruction], None);
        assert_eq!(
            parse_vote(&message.instructions[0], &keys[0..4]).unwrap(),
            ParsedInstructionEnum {
                instruction_type: "voteSwitch".to_string(),
                info: json!({
                    "voteAccount": keys[1].to_string(),
                    "slotHashesSysvar": keys[2].to_string(),
                    "clockSysvar": keys[3].to_string(),
                    "voteAuthority": keys[0].to_string(),
                    "vote": {
                        "slots": [1, 2, 4],
                        "hash": hash.to_string(),
                        "timestamp": 1_234_567_890,
                    },
                    "hash": proof_hash.to_string(),
                }),
            }
        );
        assert!(parse_vote(&message.instructions[0], &keys[0..3]).is_err());

        let authority_type = VoteAuthorize::Voter;
        let instruction =
            vote_instruction::authorize_checked(&keys[1], &keys[0], &keys[3], authority_type);
        let message = Message::new(&[instruction], None);
        assert_eq!(
            parse_vote(&message.instructions[0], &keys[0..4]).unwrap(),
            ParsedInstructionEnum {
                instruction_type: "authorizeChecked".to_string(),
                info: json!({
                    "voteAccount": keys[2].to_string(),
                    "clockSysvar": keys[3].to_string(),
                    "authority": keys[0].to_string(),
                    "newAuthority": keys[1].to_string(),
                    "authorityType": authority_type,
                }),
            }
        );
        assert!(parse_vote(&message.instructions[0], &keys[0..3]).is_err());
    }
}
