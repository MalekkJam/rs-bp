use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rs_bp::bundle::bundle_manager::BundleManager;
use rs_bp::bundle::{Bundle, BundlePayload};
use rs_bp::cla::bundle::ProtobufBundle;
use rs_bp::cla::{protobuf, ClaError, UdpConvergenceLayer};
use rs_bp::transport::UdpTransport;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{Instant, MissedTickBehavior};

type AppResult<T> = Result<T, Box<dyn Error>>;

const RETRY_INTERVAL: Duration = Duration::from_secs(2);

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> AppResult<()> {
    let mut args = env::args().skip(1);

    match args.next().as_deref() {
        None => {
            let bind_addr = prompt_address("Local UDP address")?;
            let next_addr = prompt_address("Next node UDP address")?;
            run_node(bind_addr, next_addr).await
        }
        Some("node") => {
            let bind_addr = parse_address(args.next(), "local UDP address")?;
            let next_addr = parse_address(args.next(), "next node UDP address")?;
            ensure_no_extra_args(args)?;
            run_node(bind_addr, next_addr).await
        }
        Some("demo") => run_demo().await,
        Some("help" | "--help" | "-h") => {
            print_usage();
            Ok(())
        }
        Some(command) => Err(invalid_input(format!(
            "unknown command '{command}'; run with --help for usage"
        ))),
    }
}

async fn run_node(bind_addr: SocketAddr, next_addr: SocketAddr) -> AppResult<()> {
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

async fn run_demo() -> AppResult<()> {
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

fn pending_directory(node_id: &str) -> PathBuf {
    PathBuf::from("storage")
        .join(safe_path_component(node_id))
        .join("pending")
}

fn save_pending(directory: &Path, bundle: &Bundle) -> AppResult<()> {
    fs::create_dir_all(directory)?;
    let protobuf_bundle = ProtobufBundle::from(bundle);
    let bytes = protobuf::serialize(&protobuf_bundle).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "could not serialize pending bundle",
        )
    })?;
    fs::write(pending_path(directory, &bundle.id), bytes)?;
    Ok(())
}

fn load_pending(directory: &Path) -> AppResult<HashMap<String, Bundle>> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error.into()),
    };
    let mut pending = HashMap::new();

    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("bundle") {
            continue;
        }

        let result = fs::read(&path)
            .ok()
            .and_then(|bytes| protobuf::deserialize(&bytes))
            .and_then(|bundle| Bundle::try_from(bundle).ok());
        match result {
            Some(bundle) => {
                pending.insert(bundle.id.clone(), bundle);
            }
            None => eprintln!("ignored invalid pending file {}", path.display()),
        }
    }

    Ok(pending)
}

fn remove_pending(directory: &Path, bundle_id: &str) -> AppResult<()> {
    match fs::remove_file(pending_path(directory, bundle_id)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn pending_path(directory: &Path, bundle_id: &str) -> PathBuf {
    directory.join(format!("{}.bundle", safe_path_component(bundle_id)))
}

fn safe_path_component(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            character if character.is_control() => '_',
            character => character,
        })
        .collect()
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

fn prompt_address(label: &str) -> AppResult<SocketAddr> {
    print!("{label}: ");
    io::stdout().flush()?;
    let mut value = String::new();
    io::stdin().read_line(&mut value)?;
    value
        .trim()
        .parse()
        .map_err(|error| invalid_input(format!("invalid {label}: {error}")))
}

fn parse_address(value: Option<String>, name: &str) -> AppResult<SocketAddr> {
    let value = value.ok_or_else(|| invalid_input(format!("missing {name}")))?;
    value
        .parse()
        .map_err(|error| invalid_input(format!("invalid {name} '{value}': {error}")))
}

fn ensure_no_extra_args(mut args: impl Iterator<Item = String>) -> AppResult<()> {
    if let Some(argument) = args.next() {
        return Err(invalid_input(format!("unexpected argument: {argument}")));
    }
    Ok(())
}

fn print_prompt() -> AppResult<()> {
    print!("rs-bp> ");
    io::stdout().flush()?;
    Ok(())
}

fn print_node_help() {
    println!("commands: send <text>, pending, status, help, quit");
}

fn invalid_input(message: impl Into<String>) -> Box<dyn Error> {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn print_usage() {
    println!("Run interactively:");
    println!("  cargo run");
    println!();
    println!("Run with addresses supplied:");
    println!("  cargo run -- node <local-address> <next-node-address>");
    println!();
    println!("Example two-node setup:");
    println!("  cargo run -- node 127.0.0.1:7001 127.0.0.1:7002");
    println!("  cargo run -- node 127.0.0.1:7002 127.0.0.1:7001");
}
