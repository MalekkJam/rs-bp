use std::env;
use std::io::{self, Write};
use std::net::SocketAddr;

use super::runtime::{run_demo, run_node};
use super::AppResult;

pub async fn run() -> AppResult<()> {
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

pub(crate) fn print_prompt() -> AppResult<()> {
    print!("rs-bp> ");
    io::stdout().flush()?;
    Ok(())
}

pub(crate) fn print_node_help() {
    println!("commands: send <text>, pending, status, help, quit");
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

fn invalid_input(message: impl Into<String>) -> Box<dyn std::error::Error> {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}
