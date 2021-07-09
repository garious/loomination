use crate::replay_stage::{VoteInfo, VoteOp};
use solana_gossip::cluster_info::ClusterInfo;
use solana_poh::poh_recorder::PohRecorder;
use std::{
    sync::{mpsc::Receiver, Arc, Mutex},
    thread::{self, Builder, JoinHandle},
};

pub struct VotingService {
    thread_hdl: JoinHandle<()>,
}

impl VotingService {
    pub fn new(
        vote_receiver: Receiver<VoteInfo>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<Mutex<PohRecorder>>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("sol-vote-service".to_string())
            .spawn(move || {
                for vote_info in vote_receiver.iter() {
                    Self::push_vote(&cluster_info, &poh_recorder, vote_info);
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    pub fn push_vote(
        cluster_info: &ClusterInfo,
        poh_recorder: &Mutex<PohRecorder>,
        vote_info: VoteInfo,
    ) {
        let VoteInfo { vote_tx, op } = vote_info;

        let _ = cluster_info.send_transaction(
            &vote_tx,
            crate::banking_stage::next_leader_tpu(cluster_info, poh_recorder),
        );
        match op {
            VoteOp::PushVote(tower_slots) => {
                cluster_info.push_vote(&tower_slots, vote_tx);
            }
            VoteOp::RefreshVote(last_voted_slot) => {
                cluster_info.refresh_vote(vote_tx, last_voted_slot);
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
