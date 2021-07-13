//! The `sendmmsg` module provides sendmmsg() API implementation

use std::io;
use std::net::{SocketAddr, UdpSocket};

#[cfg(not(target_os = "linux"))]
pub fn batch_send(sock: &UdpSocket, packets: &[(&Vec<u8>, &SocketAddr)]) -> io::Result<()> {
    let mut total_sent = 0;
    let mut err = None;
    for (p, a) in packets {
        if let Err(e) = sock.send_to(p, *a) {
            err = Some(e);
        } else {
            total_sent += 1;
        }
    }

    if let Some(err) = err {
        error!(
            "batch_send failed to send some packets {}/{} error {:?}",
            total_sent,
            packets.len(),
            err,
        );
        Err(err)
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn multi_target_send(sock: &UdpSocket, packet: &[u8], dests: &[&SocketAddr]) -> io::Result<()> {
    let mut total_sent = 0;
    let mut err = None;
    for a in dests {
        if let Err(e) = sock.send_to(packet, a) {
            err = Some(e);
        } else {
            total_sent += 1;
        }
    }

    if let Some(err) = err {
        error!(
            "multi_target_send failed to send some packets {}/{} error: {:?}",
            total_sent,
            dests.len(),
            err,
        );
        Err(err)
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
use libc::{iovec, mmsghdr, sockaddr_in, sockaddr_in6, sockaddr_storage};

#[cfg(target_os = "linux")]
fn mmsghdr_for_packet(
    packet: &[u8],
    dest: &SocketAddr,
    index: usize,
    iovs: &mut Vec<iovec>,
    addrs: &mut Vec<sockaddr_storage>,
    hdrs: &mut Vec<mmsghdr>,
) {
    use libc::{c_void, socklen_t};
    use nix::sys::socket::InetAddr;
    use std::mem;

    let addr_in_len = mem::size_of::<sockaddr_in>() as socklen_t;
    let addr_in6_len = mem::size_of::<sockaddr_in6>() as socklen_t;

    iovs.push(iovec {
        iov_base: packet.as_ptr() as *mut c_void,
        iov_len: packet.len(),
    });

    let hdr: mmsghdr = unsafe { mem::zeroed() };
    hdrs.push(hdr);

    let addr_storage: sockaddr_storage = unsafe { mem::zeroed() };
    addrs.push(addr_storage);

    hdrs[index].msg_hdr.msg_iov = &mut iovs[index];
    hdrs[index].msg_hdr.msg_iovlen = 1;

    match InetAddr::from_std(dest) {
        InetAddr::V4(addr) => {
            unsafe {
                core::ptr::write(&mut addrs[index] as *mut _ as *mut _, addr);
            }
            hdrs[index].msg_hdr.msg_name = &mut addrs[index] as *mut _ as *mut _;
            hdrs[index].msg_hdr.msg_namelen = addr_in_len;
        }
        InetAddr::V6(addr) => {
            unsafe {
                core::ptr::write(&mut addrs[index] as *mut _ as *mut _, addr);
            }
            hdrs[index].msg_hdr.msg_name = &mut addrs[index] as *mut _ as *mut _;
            hdrs[index].msg_hdr.msg_namelen = addr_in6_len;
        }
    };
}

#[cfg(target_os = "linux")]
fn sendmmsg_retry(sock: &UdpSocket, hdrs: &mut Vec<mmsghdr>) -> io::Result<()> {
    use libc::sendmmsg;
    use std::os::unix::io::AsRawFd;

    let sock_fd = sock.as_raw_fd();
    let mut pktidx = 0;
    let mut total_sent = 0;
    let mut err = None;

    while pktidx < hdrs.len() {
        let npkts =
            match unsafe { sendmmsg(sock_fd, &mut hdrs[0], (hdrs.len() - pktidx) as u32, 0) } {
                -1 => {
                    err = Some(io::Error::last_os_error());
                    0
                }
                n => n as usize,
            };
        if npkts < hdrs.len() - pktidx {
            debug!(
                "sendmmsg_retry failed to send some packets {}/{} error: {:?}",
                npkts,
                hdrs.len() - pktidx,
                err,
            );
            // skip the packet we failed to send
            pktidx += 1;
        }
        pktidx += npkts;
        total_sent += npkts;
    }

    if let Some(err) = err {
        error!(
            "leaving sendmmsg_retry errors {}/{} sent: {:?}",
            total_sent,
            hdrs.len(),
            err,
        );
        Err(err)
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub fn batch_send(sock: &UdpSocket, packets: &[(&Vec<u8>, &SocketAddr)]) -> io::Result<()> {
    // The vectors are allocated with capacity, as later code inserts elements
    // at specific indices, and uses the address of the vector index in hdrs
    let mut iovs: Vec<iovec> = Vec::with_capacity(packets.len());
    let mut addrs: Vec<sockaddr_storage> = Vec::with_capacity(packets.len());
    let mut hdrs: Vec<mmsghdr> = Vec::with_capacity(packets.len());

    for (i, (pkt, dest)) in packets.iter().enumerate() {
        mmsghdr_for_packet(pkt, dest, i, &mut iovs, &mut addrs, &mut hdrs);
    }

    sendmmsg_retry(sock, &mut hdrs)
}

#[cfg(target_os = "linux")]
pub fn multi_target_send(sock: &UdpSocket, packet: &[u8], dests: &[&SocketAddr]) -> io::Result<()> {
    // The vectors are allocated with capacity, as later code inserts elements
    // at specific indices, and uses the address of the vector index in hdrs
    let mut iovs: Vec<iovec> = Vec::with_capacity(dests.len());
    let mut addrs: Vec<sockaddr_storage> = Vec::with_capacity(dests.len());
    let mut hdrs: Vec<mmsghdr> = Vec::with_capacity(dests.len());

    for (i, dest) in dests.iter().enumerate() {
        mmsghdr_for_packet(packet, dest, i, &mut iovs, &mut addrs, &mut hdrs);
    }

    sendmmsg_retry(sock, &mut hdrs)
}

#[cfg(test)]
mod tests {
    use crate::packet::Packet;
    use crate::recvmmsg::recv_mmsg;
    use crate::sendmmsg::{batch_send, multi_target_send};
    use solana_sdk::packet::PACKET_DATA_SIZE;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};

    #[test]
    pub fn test_send_mmsg_one_dest() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");

        let packets: Vec<_> = (0..32).map(|_| vec![0u8; PACKET_DATA_SIZE]).collect();
        let packet_refs: Vec<_> = packets.iter().map(|p| (p, &addr)).collect();

        let sent = batch_send(&sender, &packet_refs).ok();
        assert_eq!(sent, Some(()));

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..]).unwrap().1;
        assert_eq!(32, recv);
    }

    #[test]
    pub fn test_send_mmsg_multi_dest() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();

        let reader2 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr2 = reader2.local_addr().unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");

        let packets: Vec<_> = (0..32).map(|_| vec![0u8; PACKET_DATA_SIZE]).collect();
        let packet_refs: Vec<_> = packets
            .iter()
            .enumerate()
            .map(|(i, p)| if i < 16 { (p, &addr) } else { (p, &addr2) })
            .collect();

        let sent = batch_send(&sender, &packet_refs).ok();
        assert_eq!(sent, Some(()));

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..]).unwrap().1;
        assert_eq!(16, recv);

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader2, &mut packets[..]).unwrap().1;
        assert_eq!(16, recv);
    }

    #[test]
    pub fn test_multicast_msg() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();

        let reader2 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr2 = reader2.local_addr().unwrap();

        let reader3 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr3 = reader3.local_addr().unwrap();

        let reader4 = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr4 = reader4.local_addr().unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");

        let packet = Packet::default();

        let sent = multi_target_send(
            &sender,
            &packet.data[..packet.meta.size],
            &[&addr, &addr2, &addr3, &addr4],
        )
        .ok();
        assert_eq!(sent, Some(()));

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader, &mut packets[..]).unwrap().1;
        assert_eq!(1, recv);

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader2, &mut packets[..]).unwrap().1;
        assert_eq!(1, recv);

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader3, &mut packets[..]).unwrap().1;
        assert_eq!(1, recv);

        let mut packets = vec![Packet::default(); 32];
        let recv = recv_mmsg(&reader4, &mut packets[..]).unwrap().1;
        assert_eq!(1, recv);
    }

    #[test]
    fn test_message_header_from_packet() {
        let packets: Vec<_> = (0..2).map(|_| vec![0u8; PACKET_DATA_SIZE]).collect();
        let ip4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
        let ip6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8080);
        let packet_refs: Vec<_> = vec![(&packets[0], &ip4), (&packets[1], &ip6)];
        let sender = UdpSocket::bind(":::0").expect("bind");
        batch_send(&sender, &packet_refs).unwrap();
    }
}
