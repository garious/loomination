use {
    crate::{broadcast_stage::BroadcastStage, retransmit_stage::RetransmitStage},
    itertools::Itertools,
    solana_gossip::{
        cluster_info::{compute_retransmit_peers, ClusterInfo},
        contact_info::ContactInfo,
        crds_gossip_pull::CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
        weighted_shuffle::{weighted_best, weighted_shuffle},
    },
    solana_sdk::pubkey::Pubkey,
    std::{any::TypeId, cmp::Reverse, collections::HashMap, marker::PhantomData},
};

enum NodeId {
    // TVU node obtained through gossip (staked or not).
    ContactInfo(ContactInfo),
    // Staked node with no contact-info in gossip table.
    Pubkey(Pubkey),
}

struct Node {
    node: NodeId,
    stake: u64,
}

pub struct ClusterNodes<T> {
    pubkey: Pubkey, // The local node itself.
    // All staked nodes + other known tvu-peers + the node itself;
    // sorted by (stake, pubkey) in descending order.
    nodes: Vec<Node>,
    // Weights and indices for sampling peers. weighted_{shuffle,best} expect
    // weights >= 1. For backward compatibility we use max(1, stake) for
    // weights and exclude nodes with no contact-info.
    index: Vec<(/*weight:*/ u64, /*index:*/ usize)>,
    _phantom: PhantomData<T>,
}

impl Node {
    #[inline]
    fn pubkey(&self) -> Pubkey {
        match &self.node {
            NodeId::Pubkey(pubkey) => *pubkey,
            NodeId::ContactInfo(node) => node.id,
        }
    }

    #[inline]
    fn contact_info(&self) -> Option<&ContactInfo> {
        match &self.node {
            NodeId::Pubkey(_) => None,
            NodeId::ContactInfo(node) => Some(node),
        }
    }
}

impl<T> ClusterNodes<T> {
    pub fn num_peers(&self) -> usize {
        self.index.len()
    }

    // A peer is considered live if they generated their contact info recently.
    pub fn num_peers_live(&self, now: u64) -> usize {
        self.index
            .iter()
            .filter_map(|(_, index)| self.nodes[*index].contact_info())
            .filter(|node| {
                let elapsed = if node.wallclock < now {
                    now - node.wallclock
                } else {
                    node.wallclock - now
                };
                elapsed < CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS
            })
            .count()
    }
}

impl ClusterNodes<BroadcastStage> {
    pub fn new(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Self {
        new_cluster_nodes(cluster_info, stakes)
    }

    /// Returns the root of turbine broadcast tree, which the leader sends the
    /// shred to.
    pub fn get_broadcast_peer(&self, shred_seed: [u8; 32]) -> Option<&ContactInfo> {
        if self.index.is_empty() {
            None
        } else {
            let index = weighted_best(&self.index, shred_seed);
            match &self.nodes[index].node {
                NodeId::ContactInfo(node) => Some(node),
                NodeId::Pubkey(_) => panic!("this should not happen!"),
            }
        }
    }
}

impl ClusterNodes<RetransmitStage> {
    pub fn new(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Self {
        new_cluster_nodes(cluster_info, stakes)
    }

    pub fn get_retransmit_peers(
        &self,
        shred_seed: [u8; 32],
        fanout: usize,
        slot_leader: Option<Pubkey>,
    ) -> (
        Vec<&ContactInfo>, // neighbors
        Vec<&ContactInfo>, // children
    ) {
        // Exclude leader from list of nodes.
        let index = self.index.iter().copied();
        let (weights, index): (Vec<u64>, Vec<usize>) = match slot_leader {
            None => {
                error!("unknown leader for shred slot");
                index.unzip()
            }
            Some(slot_leader) if slot_leader == self.pubkey => {
                error!("retransmit from slot leader: {}", slot_leader);
                index.unzip()
            }
            Some(slot_leader) => index
                .filter(|(_, i)| self.nodes[*i].pubkey() != slot_leader)
                .unzip(),
        };
        let index: Vec<_> = {
            let shuffle = weighted_shuffle(&weights, shred_seed);
            shuffle.into_iter().map(|i| index[i]).collect()
        };
        let self_index = index
            .iter()
            .position(|i| self.nodes[*i].pubkey() == self.pubkey)
            .unwrap();
        let (neighbors, children) = compute_retransmit_peers(fanout, self_index, &index);
        // Assert that the node itself is included in the set of neighbors, at
        // the right offset.
        debug_assert_eq!(
            self.nodes[neighbors[self_index % fanout]].pubkey(),
            self.pubkey
        );
        let get_contact_infos = |index: Vec<usize>| -> Vec<&ContactInfo> {
            index
                .into_iter()
                .map(|i| self.nodes[i].contact_info().unwrap())
                .collect()
        };
        (get_contact_infos(neighbors), get_contact_infos(children))
    }
}

fn new_cluster_nodes<T: 'static>(
    cluster_info: &ClusterInfo,
    stakes: &HashMap<Pubkey, u64>,
) -> ClusterNodes<T> {
    let self_pubkey = cluster_info.id();
    let nodes = get_nodes(cluster_info, stakes);
    let broadcast = TypeId::of::<T>() == TypeId::of::<BroadcastStage>();
    // For backward compatibility:
    //   * nodes which do not have contact-info are excluded.
    //   * stakes are floored at 1.
    // The sorting key here should be equivalent to
    // solana_gossip::deprecated::sorted_stakes_with_index.
    // Leader itself is excluded when sampling broadcast peers.
    let index = nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.contact_info().is_some())
        .filter(|(_, node)| !broadcast || node.pubkey() != self_pubkey)
        .sorted_by_key(|(_, node)| Reverse((node.stake.max(1), node.pubkey())))
        .map(|(index, node)| (node.stake.max(1), index))
        .collect();
    ClusterNodes {
        pubkey: self_pubkey,
        nodes,
        index,
        _phantom: PhantomData::default(),
    }
}

// All staked nodes + other known tvu-peers + the node itself;
// sorted by (stake, pubkey) in descending order.
fn get_nodes(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Vec<Node> {
    let self_pubkey = cluster_info.id();
    // The local node itself.
    std::iter::once({
        let stake = stakes.get(&self_pubkey).copied().unwrap_or_default();
        let node = NodeId::from(cluster_info.my_contact_info());
        Node { node, stake }
    })
    // All known tvu-peers from gossip.
    .chain(cluster_info.tvu_peers().into_iter().map(|node| {
        let stake = stakes.get(&node.id).copied().unwrap_or_default();
        let node = NodeId::from(node);
        Node { node, stake }
    }))
    // All staked nodes.
    .chain(
        stakes
            .iter()
            .filter(|(_, stake)| **stake > 0)
            .map(|(&pubkey, &stake)| Node {
                node: NodeId::from(pubkey),
                stake,
            }),
    )
    .sorted_by_key(|node| Reverse((node.stake, node.pubkey())))
    // Since sorted_by_key is stable, in case of duplicates, this
    // will keep nodes with contact-info.
    .dedup_by(|a, b| a.pubkey() == b.pubkey())
    .collect()
}

impl From<ContactInfo> for NodeId {
    fn from(node: ContactInfo) -> Self {
        NodeId::ContactInfo(node)
    }
}

impl From<Pubkey> for NodeId {
    fn from(pubkey: Pubkey) -> Self {
        NodeId::Pubkey(pubkey)
    }
}

impl<T> Default for ClusterNodes<T> {
    fn default() -> Self {
        Self {
            pubkey: Pubkey::default(),
            nodes: Vec::default(),
            index: Vec::default(),
            _phantom: PhantomData::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::{seq::SliceRandom, Rng},
        solana_gossip::{
            crds_value::{CrdsData, CrdsValue},
            deprecated::{
                shuffle_peers_and_index, sorted_retransmit_peers_and_stakes,
                sorted_stakes_with_index,
            },
        },
        solana_sdk::timing::timestamp,
        std::iter::repeat_with,
    };

    // Legacy methods copied for testing backward compatibility.

    fn get_broadcast_peers(
        cluster_info: &ClusterInfo,
        stakes: Option<&HashMap<Pubkey, u64>>,
    ) -> (Vec<ContactInfo>, Vec<(u64, usize)>) {
        let mut peers = cluster_info.tvu_peers();
        let peers_and_stakes = stake_weight_peers(&mut peers, stakes);
        (peers, peers_and_stakes)
    }

    fn stake_weight_peers(
        peers: &mut Vec<ContactInfo>,
        stakes: Option<&HashMap<Pubkey, u64>>,
    ) -> Vec<(u64, usize)> {
        peers.dedup();
        sorted_stakes_with_index(peers, stakes)
    }

    fn make_cluster<R: Rng>(
        rng: &mut R,
    ) -> (
        Vec<ContactInfo>,
        HashMap<Pubkey, u64>, // stakes
        ClusterInfo,
    ) {
        let mut nodes: Vec<_> = repeat_with(|| ContactInfo::new_rand(rng, None))
            .take(1000)
            .collect();
        nodes.shuffle(rng);
        let this_node = nodes[0].clone();
        let mut stakes: HashMap<Pubkey, u64> = nodes
            .iter()
            .filter_map(|node| {
                if rng.gen_ratio(1, 7) {
                    None // No stake for some of the nodes.
                } else {
                    Some((node.id, rng.gen_range(0, 20)))
                }
            })
            .collect();
        // Add some staked nodes with no contact-info.
        stakes.extend(repeat_with(|| (Pubkey::new_unique(), rng.gen_range(0, 20))).take(100));
        let cluster_info = ClusterInfo::new_with_invalid_keypair(this_node);
        {
            let now = timestamp();
            let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
            // First node is pushed to crds table by ClusterInfo constructor.
            for node in nodes.iter().skip(1) {
                let node = CrdsData::ContactInfo(node.clone());
                let node = CrdsValue::new_unsigned(node);
                assert_eq!(gossip_crds.insert(node, now), Ok(()));
            }
        }
        (nodes, stakes, cluster_info)
    }

    #[test]
    fn test_cluster_nodes_retransmit() {
        let mut rng = rand::thread_rng();
        let (nodes, stakes, cluster_info) = make_cluster(&mut rng);
        let this_node = cluster_info.my_contact_info();
        // ClusterInfo::tvu_peers excludes the node itself.
        assert_eq!(cluster_info.tvu_peers().len(), nodes.len() - 1);
        let cluster_nodes = ClusterNodes::<RetransmitStage>::new(&cluster_info, &stakes);
        // All nodes with contact-info should be in the index.
        assert_eq!(cluster_nodes.index.len(), nodes.len());
        // Staked nodes with no contact-info should be included.
        assert!(cluster_nodes.nodes.len() > nodes.len());
        // Assert that all nodes keep their contact-info.
        // and, all staked nodes are also included.
        {
            let cluster_nodes: HashMap<_, _> = cluster_nodes
                .nodes
                .iter()
                .map(|node| (node.pubkey(), node))
                .collect();
            for node in &nodes {
                assert_eq!(cluster_nodes[&node.id].contact_info().unwrap().id, node.id);
            }
            for (pubkey, stake) in &stakes {
                if *stake > 0 {
                    assert_eq!(cluster_nodes[pubkey].stake, *stake);
                }
            }
        }
        let (peers, stakes_and_index) =
            sorted_retransmit_peers_and_stakes(&cluster_info, Some(&stakes));
        assert_eq!(stakes_and_index.len(), peers.len());
        assert_eq!(cluster_nodes.index.len(), peers.len());
        for (i, node) in cluster_nodes
            .index
            .iter()
            .map(|(_, i)| &cluster_nodes.nodes[*i])
            .enumerate()
        {
            let (stake, index) = stakes_and_index[i];
            // Wallclock may be update by ClusterInfo::push_self.
            if node.pubkey() == this_node.id {
                assert_eq!(this_node.id, peers[index].id)
            } else {
                assert_eq!(node.contact_info().unwrap(), &peers[index]);
            }
            assert_eq!(node.stake.max(1), stake);
        }
        let slot_leader = nodes[1..].choose(&mut rng).unwrap().id;
        // Remove slot leader from peers indices.
        let stakes_and_index: Vec<_> = stakes_and_index
            .into_iter()
            .filter(|(_stake, index)| peers[*index].id != slot_leader)
            .collect();
        assert_eq!(peers.len(), stakes_and_index.len() + 1);
        let mut shred_seed = [0u8; 32];
        rng.fill(&mut shred_seed[..]);
        let (self_index, shuffled_peers_and_stakes) =
            shuffle_peers_and_index(&this_node.id, &peers, &stakes_and_index, shred_seed);
        let shuffled_index: Vec<_> = shuffled_peers_and_stakes
            .into_iter()
            .map(|(_, index)| index)
            .collect();
        assert_eq!(this_node.id, peers[shuffled_index[self_index]].id);
        for fanout in 1..200 {
            let (neighbors_indices, children_indices) =
                compute_retransmit_peers(fanout, self_index, &shuffled_index);
            let (neighbors, children) =
                cluster_nodes.get_retransmit_peers(shred_seed, fanout, Some(slot_leader));
            assert_eq!(children.len(), children_indices.len());
            for (node, index) in children.into_iter().zip(children_indices) {
                assert_eq!(*node, peers[index]);
            }
            assert_eq!(neighbors.len(), neighbors_indices.len());
            assert_eq!(neighbors[0].id, peers[neighbors_indices[0]].id);
            for (node, index) in neighbors.into_iter().zip(neighbors_indices).skip(1) {
                assert_eq!(*node, peers[index]);
            }
        }
    }

    #[test]
    fn test_cluster_nodes_broadcast() {
        let mut rng = rand::thread_rng();
        let (nodes, stakes, cluster_info) = make_cluster(&mut rng);
        // ClusterInfo::tvu_peers excludes the node itself.
        assert_eq!(cluster_info.tvu_peers().len(), nodes.len() - 1);
        let cluster_nodes = ClusterNodes::<BroadcastStage>::new(&cluster_info, &stakes);
        // All nodes with contact-info should be in the index.
        // Excluding this node itself.
        assert_eq!(cluster_nodes.index.len() + 1, nodes.len());
        // Staked nodes with no contact-info should be included.
        assert!(cluster_nodes.nodes.len() > nodes.len());
        // Assert that all nodes keep their contact-info.
        // and, all staked nodes are also included.
        {
            let cluster_nodes: HashMap<_, _> = cluster_nodes
                .nodes
                .iter()
                .map(|node| (node.pubkey(), node))
                .collect();
            for node in &nodes {
                assert_eq!(cluster_nodes[&node.id].contact_info().unwrap().id, node.id);
            }
            for (pubkey, stake) in &stakes {
                if *stake > 0 {
                    assert_eq!(cluster_nodes[pubkey].stake, *stake);
                }
            }
        }
        let (peers, peers_and_stakes) = get_broadcast_peers(&cluster_info, Some(&stakes));
        assert_eq!(peers_and_stakes.len(), peers.len());
        assert_eq!(cluster_nodes.index.len(), peers.len());
        for (i, node) in cluster_nodes
            .index
            .iter()
            .map(|(_, i)| &cluster_nodes.nodes[*i])
            .enumerate()
        {
            let (stake, index) = peers_and_stakes[i];
            assert_eq!(node.contact_info().unwrap(), &peers[index]);
            assert_eq!(node.stake.max(1), stake);
        }
        for _ in 0..100 {
            let mut shred_seed = [0u8; 32];
            rng.fill(&mut shred_seed[..]);
            let index = weighted_best(&peers_and_stakes, shred_seed);
            let peer = cluster_nodes.get_broadcast_peer(shred_seed).unwrap();
            assert_eq!(*peer, peers[index]);
        }
    }
}
