use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use rs_bp::bundle::bundle_manager::BundleManager;
use rs_bp::bundle::routing::forwarding_copy;
use rs_bp::bundle::{Bundle, BundlePayload};
use rs_bp::cla::{ClaError, UdpConvergenceLayer};
use rs_bp::transport::UdpTransport;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{Instant, MissedTickBehavior};

use super::cli::{print_node_help, print_prompt};
use super::persistence::{
    load_pending, pending_directory, previous_peer, remove_pending, save_forwarded_pending,
    save_pending,
};
use super::AppResult;

const RETRY_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) async fn run_node(bind_addr: SocketAddr, next_addr: SocketAddr) -> AppResult<()> {
    let cla = create_cla(bind_addr).await?;
    let local_addr = cla.local_addr()?;
    let node_id = node_id_for_address(local_addr);
    let next_node_id = node_id_for_address(next_addr);
    let manager = BundleManager::new();
    let pending_dir = pending_directory(&node_id);
    let mut pending = load_pending(&pending_dir).await?;
    let mut received_ids = HashSet::new();

    println!("rs-bp node {node_id}");
    println!("listening on {local_addr}");
    println!("next node is {next_node_id} at {next_addr}");
    println!("{} pending bundle(s) restored", pending.len());
    print_node_help();

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut retry_timer = tokio::time::interval_at(Instant::now() + RETRY_INTERVAL, RETRY_INTERVAL);
    retry_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    print_prompt()?;

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else {
                    println!();
                    return Ok(());
                };

                if !handle_command(
                    line.trim(),
                    &cla,
                    &manager,
                    &node_id,
                    &next_node_id,
                    next_addr,
                    &pending_dir,
                    &mut pending,
                ).await? {
                    return Ok(());
                }
                print_prompt()?;
            }
            incoming = cla.receive_bundle_from() => {
                match incoming {
                    Ok((bundle, peer_addr)) => {
                        println!();
                        handle_incoming(
                            &cla,
                            &manager,
                            &node_id,
                            next_addr,
                            peer_addr,
                            bundle,
                            &pending_dir,
                            &mut pending,
                            &mut received_ids,
                        ).await?;
                        print_prompt()?;
                    }
                    Err(ClaError::Deserialize) => {
                        println!();
                        eprintln!("ignored malformed UDP bundle");
                        print_prompt()?;
                    }
                    Err(ClaError::Io(error)) if error.kind() == io::ErrorKind::ConnectionReset => {
                        // Windows reports an ICMP "port unreachable" response this way.
                        // The peer is offline, so keep pending bundles and continue retrying.
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            _ = retry_timer.tick() => {
                retry_pending(&cla, next_addr, &pending_dir, &mut pending).await?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_command(
    command: &str,
    cla: &UdpConvergenceLayer,
    manager: &BundleManager,
    node_id: &str,
    next_node_id: &str,
    next_addr: SocketAddr,
    pending_dir: &Path,
    pending: &mut HashMap<String, Bundle>,
) -> AppResult<bool> {
    let submission = if let Some(arguments) = command.strip_prefix("send-to ") {
        let Some((destination, text)) = arguments.trim().split_once(char::is_whitespace) else {
            println!("usage: send-to <destination> <text>");
            return Ok(true);
        };
        Some((destination, text))
    } else {
        command
            .strip_prefix("send ")
            .map(|text| (next_node_id, text))
    };
    if let Some((destination, text)) = submission {
        let text = text.trim();
        if text.is_empty() {
            println!("message must not be empty");
            return Ok(true);
        }

        let bundle = manager.create_bundle(
            node_id,
            destination,
            BundlePayload::Message(text.to_string()),
        );
        if destination == node_id {
            println!("message from {node_id}: {text}");
            println!("bundle {} delivered locally", bundle.id);
            return Ok(true);
        }
        let outgoing = forwarding_copy(&bundle).expect("new bundles have an unused hop limit");
        if let Err(error) = cla.validate_bundle_size(&outgoing) {
            eprintln!("message was not queued: {error}");
            return Ok(true);
        }
        if let Err(error) = save_pending(pending_dir, &bundle).await {
            eprintln!("could not queue bundle {}: {error}", bundle.id);
            return Ok(true);
        }
        pending.insert(bundle.id.clone(), bundle.clone());
        if let Err(error) = cla.send_bundle(&outgoing, next_addr).await {
            eprintln!(
                "could not send bundle {}; kept for retry: {error}",
                bundle.id
            );
        }
        println!(
            "queued bundle {} for {} via {next_addr}",
            bundle.id, bundle.destination
        );
        return Ok(true);
    }

    match command {
        "send" => println!("usage: send <text>"),
        "send-to" => println!("usage: send-to <destination> <text>"),
        "pending" => print_pending(pending),
        "status" => {
            println!("node: {node_id}");
            println!("next node: {next_node_id} at {next_addr}");
            println!("pending bundles: {}", pending.len());
        }
        "help" => print_node_help(),
        "quit" | "exit" => return Ok(false),
        "" => {}
        _ => println!("unknown command; type 'help'"),
    }

    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn handle_incoming(
    cla: &UdpConvergenceLayer,
    manager: &BundleManager,
    node_id: &str,
    next_addr: SocketAddr,
    peer_addr: SocketAddr,
    bundle: Bundle,
    pending_dir: &Path,
    pending: &mut HashMap<String, Bundle>,
    received_ids: &mut HashSet<String>,
) -> AppResult<()> {
    if BundleManager::bundle_expired(&bundle) {
        return Ok(());
    }
    match &bundle.payload {
        BundlePayload::Message(text) => {
            if !BundleManager::bundle_at_destination(&bundle, node_id) {
                if let Some(existing) = pending.get(&bundle.id) {
                    let mut comparable = bundle.clone();
                    comparable.hop_count = existing.hop_count;
                    if existing != &comparable {
                        eprintln!("ignored conflicting bundle {}", bundle.id);
                    }
                    // Keep the original reverse path; scheduled retries own forwarding.
                    return Ok(());
                }
                let Some(outgoing) = forwarding_copy(&bundle) else {
                    eprintln!("discarded bundle {}: hop limit reached", bundle.id);
                    return Ok(());
                };
                if let Err(error) = cla.validate_bundle_size(&outgoing) {
                    eprintln!("cannot forward bundle {}: {error}", bundle.id);
                    return Ok(());
                }
                if let Err(error) = save_forwarded_pending(pending_dir, &bundle, peer_addr).await {
                    eprintln!("could not store relayed bundle {}: {error}", bundle.id);
                    return Ok(());
                }
                pending.insert(bundle.id.clone(), bundle.clone());
                if let Err(error) = cla.send_bundle(&outgoing, next_addr).await {
                    eprintln!(
                        "could not forward bundle {}; kept for retry: {error}",
                        bundle.id
                    );
                }
                println!(
                    "forward pending {} for {} via {next_addr}",
                    bundle.id, bundle.destination
                );
                return Ok(());
            }
            if received_ids.insert(bundle.id.clone()) {
                println!("message from {}: {text}", bundle.source);
            }

            // ACK every copy, including duplicates, in case an earlier ACK was lost.
            let ack = manager.create_bundle(
                node_id.to_string(),
                bundle.source,
                BundlePayload::Ack {
                    original_bundle_id: bundle.id.clone(),
                },
            );
            if let Err(error) = send_forwarded(cla, &ack, peer_addr).await {
                eprintln!("could not acknowledge bundle {}: {error}", bundle.id);
            }
        }
        BundlePayload::Ack { original_bundle_id } => {
            let expected_ack = pending.get(original_bundle_id).is_some_and(|original| {
                peer_addr == next_addr
                    && bundle.source == original.destination
                    && bundle.destination == original.source
            });
            if !expected_ack {
                return Ok(());
            }
            let upstream = match previous_peer(pending_dir, original_bundle_id).await {
                Ok(upstream) => upstream,
                Err(error) => {
                    eprintln!("could not read reverse path for {original_bundle_id}: {error}");
                    return Ok(());
                }
            };
            if let Some(upstream) = upstream {
                if let Err(error) = send_forwarded(cla, &bundle, upstream).await {
                    eprintln!(
                        "could not relay ACK for {original_bundle_id}; kept for retry: {error}"
                    );
                    return Ok(());
                }
            } else if bundle.destination != node_id {
                return Ok(());
            }
            if delete_pending(pending_dir, pending, original_bundle_id).await {
                if upstream.is_some() {
                    println!("relayed delivery ACK for {original_bundle_id}");
                } else {
                    println!("bundle {original_bundle_id} delivered and acknowledged");
                }
            }
        }
        BundlePayload::RequestSummaryVector | BundlePayload::SummaryVector(_) => {
            println!("summary-vector exchange is not implemented yet");
        }
    }

    Ok(())
}

async fn retry_pending(
    cla: &UdpConvergenceLayer,
    next_addr: SocketAddr,
    pending_dir: &Path,
    pending: &mut HashMap<String, Bundle>,
) -> AppResult<()> {
    let mut expired = Vec::new();

    for (bundle_id, bundle) in pending.iter() {
        if BundleManager::bundle_expired(bundle) || forwarding_copy(bundle).is_none() {
            expired.push(bundle_id.clone());
        } else if let Err(error) = send_forwarded(cla, bundle, next_addr).await {
            eprintln!("could not retry bundle {bundle_id}: {error}");
        }
    }

    for bundle_id in expired {
        if delete_pending(pending_dir, pending, &bundle_id).await {
            println!("removed expired or hop-exhausted pending bundle {bundle_id}");
        }
    }

    Ok(())
}

async fn send_forwarded(
    cla: &UdpConvergenceLayer,
    bundle: &Bundle,
    peer: SocketAddr,
) -> AppResult<()> {
    let outgoing = forwarding_copy(bundle)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "hop limit reached"))?;
    cla.send_bundle(&outgoing, peer).await?;
    Ok(())
}

async fn delete_pending(directory: &Path, pending: &mut HashMap<String, Bundle>, id: &str) -> bool {
    match remove_pending(directory, id).await {
        Ok(()) => {
            pending.remove(id);
            true
        }
        Err(error) => {
            eprintln!("could not remove pending bundle {id}; kept for retry: {error}");
            false
        }
    }
}

pub(crate) async fn run_demo() -> AppResult<()> {
    let receiver = create_cla("127.0.0.1:0".parse()?).await?;
    let sender = create_cla("127.0.0.1:0".parse()?).await?;
    let receiver_addr = receiver.local_addr()?;
    let manager = BundleManager::new();
    let bundle = manager.create_bundle(
        node_id_for_address(sender.local_addr()?),
        node_id_for_address(receiver_addr),
        BundlePayload::Message("Hello from the rs-bp MVP".to_string()),
    );

    sender.send_bundle(&bundle, receiver_addr).await?;
    let received = tokio::time::timeout(Duration::from_secs(2), receiver.receive_bundle())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "bundle receive timed out"))??;

    println!("sent bundle {}", bundle.id);
    println!("received payload: {:?}", received.payload);
    if received != bundle {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bundle changed in transit").into());
    }
    println!("MVP exchange completed successfully");
    Ok(())
}

async fn create_cla(bind_addr: SocketAddr) -> AppResult<UdpConvergenceLayer> {
    let transport = UdpTransport::bind(bind_addr).await?;
    Ok(UdpConvergenceLayer::new(transport))
}

fn node_id_for_address(address: SocketAddr) -> String {
    format!("ipn:1:{}", address.port())
}

fn print_pending(pending: &HashMap<String, Bundle>) {
    if pending.is_empty() {
        println!("no pending bundles");
        return;
    }

    for bundle in pending.values() {
        println!(
            "{} -> {}: {:?}",
            bundle.id, bundle.destination, bundle.payload
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::persistence::tests::TestDirectory;
    use super::*;

    async fn socket() -> UdpConvergenceLayer {
        create_cla("127.0.0.1:0".parse().unwrap()).await.unwrap()
    }

    fn message(manager: &BundleManager) -> Bundle {
        manager.create_bundle(
            "ipn:1:7001",
            "ipn:1:7002",
            BundlePayload::Message("hello".into()),
        )
    }

    #[tokio::test]
    async fn send_to_preserves_final_destination_in_the_queue() {
        let cla = socket().await;
        let peer = socket().await;
        let dir = TestDirectory::new();
        let mut pending = HashMap::new();
        handle_command(
            "send-to ipn:1:7003 hello C",
            &cla,
            &BundleManager::new(),
            "ipn:1:7001",
            "ipn:1:7002",
            peer.local_addr().unwrap(),
            &dir.0,
            &mut pending,
        )
        .await
        .unwrap();
        assert_eq!(pending.len(), 1);
        let bundle = pending.values().next().unwrap();
        assert_eq!(bundle.destination, "ipn:1:7003");
        assert_eq!(bundle.source, "ipn:1:7001");
    }

    #[tokio::test]
    async fn nonlocal_message_is_stored_without_local_delivery() {
        let cla = socket().await;
        let peer = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let bundle = message(&manager);
        let mut pending = HashMap::new();
        let mut seen = HashSet::new();
        handle_incoming(
            &cla,
            &manager,
            "relay",
            peer.local_addr().unwrap(),
            cla.local_addr().unwrap(),
            bundle.clone(),
            &dir.0,
            &mut pending,
            &mut seen,
        )
        .await
        .unwrap();
        assert!(seen.is_empty());
        assert_eq!(
            load_pending(&dir.0).await.unwrap().get(&bundle.id),
            Some(&bundle)
        );
    }

    #[tokio::test]
    async fn wrong_destination_is_not_delivered() {
        let cla = socket().await;
        let peer = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let mut seen = HashSet::new();
        handle_incoming(
            &cla,
            &manager,
            "another-node",
            peer.local_addr().unwrap(),
            peer.local_addr().unwrap(),
            message(&manager),
            &dir.0,
            &mut HashMap::new(),
            &mut seen,
        )
        .await
        .unwrap();
        assert!(seen.is_empty());
    }

    #[tokio::test]
    async fn ack_from_wrong_source_keeps_pending_data() {
        let cla = socket().await;
        let peer = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let bundle = message(&manager);
        save_pending(&dir.0, &bundle).await.unwrap();
        let mut pending = HashMap::from([(bundle.id.clone(), bundle.clone())]);
        let ack = manager.create_bundle(
            "wrong-source",
            &bundle.source,
            BundlePayload::Ack {
                original_bundle_id: bundle.id.clone(),
            },
        );
        handle_incoming(
            &cla,
            &manager,
            &bundle.source,
            peer.local_addr().unwrap(),
            peer.local_addr().unwrap(),
            ack,
            &dir.0,
            &mut pending,
            &mut HashSet::new(),
        )
        .await
        .unwrap();
        assert!(pending.contains_key(&bundle.id));
        assert!(load_pending(&dir.0).await.unwrap().contains_key(&bundle.id));
    }

    #[tokio::test]
    async fn initial_send_error_keeps_node_running_and_bundle_queued() {
        let cla = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let mut pending = HashMap::new();
        let result = handle_command(
            "send hello",
            &cla,
            &manager,
            "ipn:1:7001",
            "ipn:1:7002",
            "[::1]:7002".parse().unwrap(),
            &dir.0,
            &mut pending,
        )
        .await;
        assert!(result.unwrap());
        assert_eq!(pending.len(), 1);
        assert_eq!(load_pending(&dir.0).await.unwrap(), pending);
    }

    #[tokio::test]
    async fn expiration_deletion_failure_preserves_memory() {
        let cla = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let mut bundle = message(&manager);
        bundle.expires_at = chrono::Utc::now() - chrono::Duration::seconds(1);
        let path = dir
            .0
            .join(format!("{}.bundle", bundle.id.replace(':', "_")));
        std::fs::create_dir(&path).unwrap();
        let mut pending = HashMap::from([(bundle.id.clone(), bundle.clone())]);
        let _ = retry_pending(&cla, cla.local_addr().unwrap(), &dir.0, &mut pending).await;
        assert!(pending.contains_key(&bundle.id));
    }

    #[tokio::test]
    async fn validates_ack_peer_destination_and_lifetime_before_removing() {
        let cla = socket().await;
        let peer = socket().await;
        let stranger = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let original = message(&manager);
        save_pending(&dir.0, &original).await.unwrap();
        let mut pending = HashMap::from([(original.id.clone(), original.clone())]);
        let ack = manager.create_bundle(
            &original.destination,
            &original.source,
            BundlePayload::Ack {
                original_bundle_id: original.id.clone(),
            },
        );
        let expected = peer.local_addr().unwrap();
        let mut wrong_destination = ack.clone();
        wrong_destination.destination = "another-node".into();
        let mut expired = ack.clone();
        expired.expires_at = chrono::Utc::now() - chrono::Duration::seconds(1);
        let mut unknown = ack.clone();
        unknown.payload = BundlePayload::Ack {
            original_bundle_id: "unknown".into(),
        };
        for (incoming, address) in [
            (ack.clone(), stranger.local_addr().unwrap()),
            (wrong_destination, expected),
            (expired, expected),
            (unknown, expected),
        ] {
            handle_incoming(
                &cla,
                &manager,
                &original.source,
                expected,
                address,
                incoming,
                &dir.0,
                &mut pending,
                &mut HashSet::new(),
            )
            .await
            .unwrap();
            assert!(pending.contains_key(&original.id));
            assert!(load_pending(&dir.0)
                .await
                .unwrap()
                .contains_key(&original.id));
        }
        for _ in 0..2 {
            handle_incoming(
                &cla,
                &manager,
                &original.source,
                expected,
                expected,
                ack.clone(),
                &dir.0,
                &mut pending,
                &mut HashSet::new(),
            )
            .await
            .unwrap();
            assert!(pending.is_empty());
            assert!(load_pending(&dir.0).await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn ack_deletion_failure_preserves_memory() {
        let cla = socket().await;
        let peer = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let original = message(&manager);
        std::fs::create_dir(dir.0.join(format!("{}.bundle", original.id))).unwrap();
        let mut pending = HashMap::from([(original.id.clone(), original.clone())]);
        let ack = manager.create_bundle(
            &original.destination,
            &original.source,
            BundlePayload::Ack {
                original_bundle_id: original.id.clone(),
            },
        );
        handle_incoming(
            &cla,
            &manager,
            &original.source,
            peer.local_addr().unwrap(),
            peer.local_addr().unwrap(),
            ack,
            &dir.0,
            &mut pending,
            &mut HashSet::new(),
        )
        .await
        .unwrap();
        assert!(pending.contains_key(&original.id));
    }

    async fn receive(cla: &UdpConvergenceLayer) -> (Bundle, SocketAddr) {
        tokio::time::timeout(Duration::from_secs(2), cla.receive_bundle_from())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn restores_retries_and_recovers_a_lost_ack_without_duplicate_delivery() {
        let sender = socket().await;
        let receiver = socket().await;
        let sender_addr = sender.local_addr().unwrap();
        let receiver_addr = receiver.local_addr().unwrap();
        let sender_id = node_id_for_address(sender_addr);
        let receiver_id = node_id_for_address(receiver_addr);
        let dir = TestDirectory::new();
        let receiver_dir = TestDirectory::new();
        let original = BundleManager::new().create_bundle(
            &sender_id,
            &receiver_id,
            BundlePayload::Message("offline message".into()),
        );
        // Persist while offline, then reconstruct all sender state as on restart.
        save_pending(&dir.0, &original).await.unwrap();
        let mut pending = load_pending(&dir.0).await.unwrap();
        let manager = BundleManager::new();
        let new_bundle = manager.create_bundle(
            &sender_id,
            &receiver_id,
            BundlePayload::Message("after restart".into()),
        );
        assert_ne!(original.id, new_bundle.id);
        let mut seen = HashSet::new();
        let mut receiver_pending = HashMap::new();
        for attempt in 0..2 {
            retry_pending(&sender, receiver_addr, &dir.0, &mut pending)
                .await
                .unwrap();
            let (incoming, address) = receive(&receiver).await;
            assert_eq!(incoming, forwarding_copy(&original).unwrap());
            handle_incoming(
                &receiver,
                &manager,
                &receiver_id,
                sender_addr,
                address,
                incoming,
                &receiver_dir.0,
                &mut receiver_pending,
                &mut seen,
            )
            .await
            .unwrap();
            let (ack, address) = receive(&sender).await;
            assert_eq!(
                ack.payload,
                BundlePayload::Ack {
                    original_bundle_id: original.id.clone()
                }
            );
            assert_eq!(seen.len(), 1);
            if attempt == 0 {
                // Drop the first ACK to force a duplicate and another ACK.
                assert_eq!(pending.len(), 1);
            } else {
                handle_incoming(
                    &sender,
                    &manager,
                    &sender_id,
                    receiver_addr,
                    address,
                    ack,
                    &dir.0,
                    &mut pending,
                    &mut HashSet::new(),
                )
                .await
                .unwrap();
            }
        }
        assert!(pending.is_empty());
        assert!(load_pending(&dir.0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn oversized_message_is_not_queued() {
        let cla = socket().await;
        let dir = TestDirectory::new();
        let mut pending = HashMap::new();
        assert!(handle_command(
            &format!("send {}", "x".repeat(65_507)),
            &cla,
            &BundleManager::new(),
            "a",
            "b",
            cla.local_addr().unwrap(),
            &dir.0,
            &mut pending
        )
        .await
        .unwrap());
        assert!(pending.is_empty());
        assert!(load_pending(&dir.0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn expired_messages_are_neither_delivered_nor_retained_for_retry() {
        let cla = socket().await;
        let peer = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let mut bundle = message(&manager);
        bundle.created_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        bundle.created_at =
            chrono::DateTime::from_timestamp(bundle.created_at.timestamp(), 0).unwrap();
        bundle.expires_at = bundle.created_at + chrono::Duration::seconds(1);
        save_pending(&dir.0, &bundle).await.unwrap();
        let mut pending = HashMap::from([(bundle.id.clone(), bundle.clone())]);
        let mut seen = HashSet::new();
        handle_incoming(
            &cla,
            &manager,
            &bundle.destination,
            peer.local_addr().unwrap(),
            peer.local_addr().unwrap(),
            bundle.clone(),
            &dir.0,
            &mut pending,
            &mut seen,
        )
        .await
        .unwrap();
        assert!(seen.is_empty());
        retry_pending(&cla, peer.local_addr().unwrap(), &dir.0, &mut pending)
            .await
            .unwrap();
        assert!(pending.is_empty());
        assert!(load_pending(&dir.0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_ack_send_is_recoverable() {
        let cla = socket().await;
        let peer = socket().await;
        let dir = TestDirectory::new();
        let manager = BundleManager::new();
        let bundle = message(&manager);
        let mut seen = HashSet::new();
        let mut pending = HashMap::new();
        handle_incoming(
            &cla,
            &manager,
            &bundle.destination,
            peer.local_addr().unwrap(),
            "[::1]:7001".parse().unwrap(),
            bundle.clone(),
            &dir.0,
            &mut pending,
            &mut seen,
        )
        .await
        .unwrap();
        handle_incoming(
            &cla,
            &manager,
            &bundle.destination,
            peer.local_addr().unwrap(),
            peer.local_addr().unwrap(),
            bundle.clone(),
            &dir.0,
            &mut pending,
            &mut seen,
        )
        .await
        .unwrap();
        let (ack, _) = receive(&peer).await;
        assert_eq!(
            ack.payload,
            BundlePayload::Ack {
                original_bundle_id: bundle.id
            }
        );
        assert_eq!(seen.len(), 1);
    }

    #[tokio::test]
    async fn three_hop_path_recovers_a_lost_reverse_ack_and_rejects_forged_receipts() {
        let a = socket().await;
        let b = socket().await;
        let c = socket().await;
        let stranger = socket().await;
        let a_addr = a.local_addr().unwrap();
        let b_addr = b.local_addr().unwrap();
        let c_addr = c.local_addr().unwrap();
        let a_id = node_id_for_address(a_addr);
        let b_id = node_id_for_address(b_addr);
        let c_id = node_id_for_address(c_addr);
        let a_dir = TestDirectory::new();
        let b_dir = TestDirectory::new();
        let c_dir = TestDirectory::new();
        let manager = BundleManager::new();
        let original = manager.create_bundle(&a_id, &c_id, BundlePayload::Message("via B".into()));
        save_pending(&a_dir.0, &original).await.unwrap();
        let mut a_pending = load_pending(&a_dir.0).await.unwrap();
        let mut b_pending = HashMap::new();
        let mut b_seen = HashSet::new();
        let mut c_seen = HashSet::new();
        for attempt in 0..2 {
            retry_pending(&a, b_addr, &a_dir.0, &mut a_pending)
                .await
                .unwrap();
            let (message_at_b, sender) = receive(&b).await;
            handle_incoming(
                &b,
                &manager,
                &b_id,
                c_addr,
                sender,
                message_at_b,
                &b_dir.0,
                &mut b_pending,
                &mut b_seen,
            )
            .await
            .unwrap();
            assert!(b_seen.is_empty());
            assert!(a_pending.contains_key(&original.id));
            // Rebuild the relay's memory; the return path must come from disk.
            b_pending = load_pending(&b_dir.0).await.unwrap();
            assert_eq!(
                previous_peer(&b_dir.0, &original.id).await.unwrap(),
                Some(a_addr)
            );
            let (message_at_c, sender) = receive(&c).await;
            assert_eq!(
                message_at_c,
                forwarding_copy(&forwarding_copy(&original).unwrap()).unwrap()
            );
            handle_incoming(
                &c,
                &manager,
                &c_id,
                a_addr,
                sender,
                message_at_c,
                &c_dir.0,
                &mut HashMap::new(),
                &mut c_seen,
            )
            .await
            .unwrap();
            let (ack, sender) = receive(&b).await;
            assert_eq!(ack.source, c_id);
            assert_eq!(ack.destination, a_id);
            let mut wrong_source = ack.clone();
            wrong_source.source = b_id.clone();
            let mut wrong_destination = ack.clone();
            wrong_destination.destination = b_id.clone();
            for (forged, address) in [
                (wrong_source, c_addr),
                (wrong_destination, c_addr),
                (ack.clone(), stranger.local_addr().unwrap()),
            ] {
                handle_incoming(
                    &b,
                    &manager,
                    &b_id,
                    c_addr,
                    address,
                    forged,
                    &b_dir.0,
                    &mut b_pending,
                    &mut b_seen,
                )
                .await
                .unwrap();
                assert!(b_pending.contains_key(&original.id));
            }
            handle_incoming(
                &b,
                &manager,
                &b_id,
                c_addr,
                sender,
                ack,
                &b_dir.0,
                &mut b_pending,
                &mut b_seen,
            )
            .await
            .unwrap();
            let (ack, sender) = receive(&a).await;
            assert_eq!(sender, b_addr);
            assert!(b_pending.is_empty());
            assert_eq!(c_seen.len(), 1);
            if attempt == 1 {
                handle_incoming(
                    &a,
                    &manager,
                    &a_id,
                    b_addr,
                    sender,
                    ack,
                    &a_dir.0,
                    &mut a_pending,
                    &mut HashSet::new(),
                )
                .await
                .unwrap();
            }
            // On attempt zero the final B->A ACK is deliberately lost.
        }
        assert!(a_pending.is_empty());
        assert!(load_pending(&a_dir.0).await.unwrap().is_empty());
        assert!(load_pending(&b_dir.0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn relay_does_not_accept_unpersistable_or_hop_exhausted_bundles() {
        let relay = socket().await;
        let next = socket().await;
        let dir = TestDirectory::new();
        let blocked = dir.0.join("not-a-directory");
        std::fs::write(&blocked, "blocked").unwrap();
        let manager = BundleManager::new();
        let mut bundle = message(&manager);
        let mut pending = HashMap::new();
        let mut seen = HashSet::new();
        handle_incoming(
            &relay,
            &manager,
            "relay",
            next.local_addr().unwrap(),
            relay.local_addr().unwrap(),
            bundle.clone(),
            &blocked,
            &mut pending,
            &mut seen,
        )
        .await
        .unwrap();
        assert!(pending.is_empty());
        bundle.hop_count = Some(rs_bp::bundle::model::HopCount { limit: 1, count: 1 });
        handle_incoming(
            &relay,
            &manager,
            "relay",
            next.local_addr().unwrap(),
            relay.local_addr().unwrap(),
            bundle,
            &dir.0,
            &mut pending,
            &mut seen,
        )
        .await
        .unwrap();
        assert!(pending.is_empty());
        assert!(seen.is_empty());
        assert!(load_pending(&dir.0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn local_destinations_and_incomplete_send_to_do_not_enter_forwarding_queue() {
        let cla = socket().await;
        let dir = TestDirectory::new();
        let mut pending = HashMap::new();
        for command in [
            "send-to ipn:1:7001 local delivery",
            "send-to",
            "send-to ipn:1:7003",
            "send-to ipn:1:7003   ",
        ] {
            handle_command(
                command,
                &cla,
                &BundleManager::new(),
                "ipn:1:7001",
                "ipn:1:7002",
                cla.local_addr().unwrap(),
                &dir.0,
                &mut pending,
            )
            .await
            .unwrap();
            assert!(pending.is_empty());
            assert!(load_pending(&dir.0).await.unwrap().is_empty());
        }
    }
}
