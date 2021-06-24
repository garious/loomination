use super::*;
use solana_entry::entry::Entry;
use solana_ledger::shred::Shredder;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Keypair;

#[derive(Clone)]
pub(super) struct BroadcastFakeShredsRun {
    last_blockhash: Hash,
    partition: usize,
    shred_version: u16,
}

impl BroadcastFakeShredsRun {
    pub(super) fn new(partition: usize, shred_version: u16) -> Self {
        Self {
            last_blockhash: Hash::default(),
            partition,
            shred_version,
        }
    }
}

impl BroadcastRun for BroadcastFakeShredsRun {
    fn run(
        &mut self,
        keypair: &Keypair,
        blockstore: &Arc<Blockstore>,
        receiver: &Receiver<WorkingBankEntry>,
        socket_sender: &Sender<(TransmitShreds, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
    ) -> Result<()> {
        // 1) Pull entries from banking stage
        let receive_results = broadcast_utils::recv_slot_entries(receiver)?;
        let bank = receive_results.bank.clone();
        let last_tick_height = receive_results.last_tick_height;

        let next_shred_index = blockstore
            .meta(bank.slot())
            .expect("Database error")
            .map(|meta| meta.consumed)
            .unwrap_or(0) as u32;

        let num_entries = receive_results.entries.len();

        let shredder = Shredder::new(
            bank.slot(),
            bank.parent().unwrap().slot(),
            (bank.tick_height() % bank.ticks_per_slot()) as u8,
            self.shred_version,
        )
        .expect("Expected to create a new shredder");

        let (data_shreds, coding_shreds, _) = shredder.entries_to_shreds(
            keypair,
            &receive_results.entries,
            last_tick_height == bank.max_tick_height(),
            next_shred_index,
        );

        // If the last blockhash is default, a new block is being created
        // So grab the last blockhash from the parent bank
        if self.last_blockhash == Hash::default() {
            self.last_blockhash = bank.parent().unwrap().last_blockhash();
        }

        let fake_entries: Vec<_> = (0..num_entries)
            .map(|_| Entry::new(&self.last_blockhash, 0, vec![]))
            .collect();

        let (fake_data_shreds, fake_coding_shreds, _) = shredder.entries_to_shreds(
            keypair,
            &fake_entries,
            last_tick_height == bank.max_tick_height(),
            next_shred_index,
        );

        // If it's the last tick, reset the last block hash to default
        // this will cause next run to grab last bank's blockhash
        if last_tick_height == bank.max_tick_height() {
            self.last_blockhash = Hash::default();
        }

        let data_shreds = Arc::new(data_shreds);
        blockstore_sender.send((data_shreds.clone(), None))?;

        // 3) Start broadcast step
        //some indicates fake shreds
        socket_sender.send((
            (Some(Arc::new(HashMap::new())), Arc::new(fake_data_shreds)),
            None,
        ))?;
        socket_sender.send((
            (Some(Arc::new(HashMap::new())), Arc::new(fake_coding_shreds)),
            None,
        ))?;
        //none indicates real shreds
        socket_sender.send(((None, data_shreds), None))?;
        socket_sender.send(((None, Arc::new(coding_shreds)), None))?;

        Ok(())
    }
    fn transmit(
        &mut self,
        receiver: &Arc<Mutex<TransmitReceiver>>,
        cluster_info: &ClusterInfo,
        sock: &UdpSocket,
        _bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<()> {
        for ((stakes, data_shreds), _) in receiver.lock().unwrap().iter() {
            let peers = cluster_info.tvu_peers();
            peers.iter().enumerate().for_each(|(i, peer)| {
                if i <= self.partition && stakes.is_some() {
                    // Send fake shreds to the first N peers
                    data_shreds.iter().for_each(|b| {
                        sock.send_to(&b.payload, &peer.tvu_forwards).unwrap();
                    });
                } else if i > self.partition && stakes.is_none() {
                    data_shreds.iter().for_each(|b| {
                        sock.send_to(&b.payload, &peer.tvu_forwards).unwrap();
                    });
                }
            });
        }
        Ok(())
    }
    fn record(
        &mut self,
        receiver: &Arc<Mutex<RecordReceiver>>,
        blockstore: &Arc<Blockstore>,
    ) -> Result<()> {
        for (data_shreds, _) in receiver.lock().unwrap().iter() {
            blockstore.insert_shreds(data_shreds.to_vec(), None, true)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_gossip::contact_info::ContactInfo;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_tvu_peers_ordering() {
        let cluster = ClusterInfo::new_with_invalid_keypair(ContactInfo::new_localhost(
            &solana_sdk::pubkey::new_rand(),
            0,
        ));
        cluster.insert_info(ContactInfo::new_with_socketaddr(&SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            8080,
        )));
        cluster.insert_info(ContactInfo::new_with_socketaddr(&SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)),
            8080,
        )));
        cluster.insert_info(ContactInfo::new_with_socketaddr(&SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 3)),
            8080,
        )));
        cluster.insert_info(ContactInfo::new_with_socketaddr(&SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 4)),
            8080,
        )));

        let tvu_peers1 = cluster.tvu_peers();
        (0..5).for_each(|_| {
            cluster
                .tvu_peers()
                .iter()
                .zip(tvu_peers1.iter())
                .for_each(|(v1, v2)| {
                    assert_eq!(v1, v2);
                });
        });
    }
}
