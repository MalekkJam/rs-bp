use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use rs_bp::bundle::bundle_manager::BundleManager;
use rs_bp::bundle::{Bundle, BundlePayload};
use rs_bp::cla::{ClaError, UdpConvergenceLayer};
use rs_bp::transport::UdpTransport;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{Instant, MissedTickBehavior};

use super::cli::{print_node_help, print_prompt};
use super::persistence::{load_pending, pending_directory, remove_pending, save_pending};
use super::AppResult;

const RETRY_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) async fn run_node(bind_addr: SocketAddr, next_addr: SocketAddr) -> AppResult<()> {
    let cla = create_cla(bind_addr).await?;
    let local_addr = cla.local_addr()?;
    let node_id = node_id_for_address(local_addr);
    let next_node_id = node_id_for_address(next_addr);
    let manager = BundleManager::new();
    let pending_dir = pending_directory(&node_id);
    let mut pending = load_pending(&pending_dir)?;
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
    if let Some(text) = command.strip_prefix("send ") {
        let text = text.trim();
        if text.is_empty() {
            println!("message must not be empty");
            return Ok(true);
        }

        let bundle = manager.create_bundle(
            node_id,
            next_node_id,
            BundlePayload::Message(text.to_string()),
        );
        save_pending(pending_dir, &bundle)?;
        pending.insert(bundle.id.clone(), bundle.clone());
        cla.send_bundle(&bundle, next_addr).await?;
        println!("queued bundle {} for delivery", bundle.id);
        return Ok(true);
    }

    match command {
        "send" => println!("usage: send <text>"),
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
    peer_addr: SocketAddr,
    bundle: Bundle,
    pending_dir: &Path,
    pending: &mut HashMap<String, Bundle>,
    received_ids: &mut HashSet<String>,
) -> AppResult<()> {
    match &bundle.payload {
        BundlePayload::Message(text) => {
            if BundleManager::bundle_expired(&bundle) {
                println!("discarded expired bundle {}", bundle.id);
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
            cla.send_bundle(&ack, peer_addr).await?;
        }
        BundlePayload::Ack { original_bundle_id } => {
            if pending.remove(original_bundle_id).is_some() {
                remove_pending(pending_dir, original_bundle_id)?;
                println!("bundle {original_bundle_id} delivered and acknowledged");
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
        if BundleManager::bundle_expired(bundle) {
            expired.push(bundle_id.clone());
        } else if let Err(error) = cla.send_bundle(bundle, next_addr).await {
            eprintln!("could not retry bundle {bundle_id}: {error}");
        }
    }

    for bundle_id in expired {
        pending.remove(&bundle_id);
        remove_pending(pending_dir, &bundle_id)?;
        println!("expired pending bundle {bundle_id}");
    }

    Ok(())
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
