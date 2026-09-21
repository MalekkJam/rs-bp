use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("rs-bp-multihop-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn node_directory(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::create_dir_all(&path).unwrap();
        path
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        // Only this test's uniquely created workspace is removed.
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Node {
    child: Child,
    output: Receiver<String>,
    history: Vec<String>,
}

impl Node {
    fn start(directory: &Path, local: SocketAddr, next: SocketAddr) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rs-bp"))
            .args(["node", &local.to_string(), &next.to_string()])
            .current_dir(directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let (sender, output) = mpsc::channel();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        for stream in [
            Box::new(stdout) as Box<dyn std::io::Read + Send>,
            Box::new(stderr),
        ] {
            let sender = sender.clone();
            thread::spawn(move || {
                for line in BufReader::new(stream).lines() {
                    let Ok(line) = line else {
                        break;
                    };
                    if sender.send(line).is_err() {
                        break;
                    }
                }
            });
        }
        Self {
            child,
            output,
            history: Vec::new(),
        }
    }

    fn send(&mut self, command: &str) {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{command}").unwrap();
        stdin.flush().unwrap();
    }

    fn wait_for(&mut self, text: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = self.output.recv_timeout(remaining).unwrap_or_else(|error| {
                panic!("waiting for {text:?}: {error}; output: {:?}", self.history)
            });
            let matches = line.contains(text);
            self.history.push(line);
            if matches {
                return;
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Kill only the child owned by this test, including on assertion failure.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn pending_count(directory: &Path, port: u16) -> usize {
    let path = directory.join(format!("storage/ipn_1_{port}/pending"));
    fs::read_dir(path)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "bundle")
        })
        .count()
}

#[test]
fn three_processes_deliver_via_relay_after_sender_and_relay_restart() {
    let workspace = Workspace::new();
    // Reserve distinct OS-assigned ports together; release before node binding.
    let reservations: Vec<_> = (0..3)
        .map(|_| UdpSocket::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: Vec<_> = reservations
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect();
    let (a_addr, b_addr, c_addr) = (addresses[0], addresses[1], addresses[2]);
    drop(reservations);
    let a_dir = workspace.node_directory("a");
    let b_dir = workspace.node_directory("b");
    let c_dir = workspace.node_directory("c");
    let mut a = Node::start(&a_dir, a_addr, b_addr);
    let mut b = Node::start(&b_dir, b_addr, c_addr);
    a.wait_for("0 pending bundle(s) restored");
    b.wait_for("0 pending bundle(s) restored");
    a.send(&format!("send-to ipn:1:{} hello through B", c_addr.port()));
    a.wait_for("queued bundle");
    b.wait_for("forward pending");
    assert_eq!(pending_count(&a_dir, a_addr.port()), 1);
    assert_eq!(pending_count(&b_dir, b_addr.port()), 1);
    assert!(!b.history.iter().any(|line| line.contains("message from")));
    // C has never been started. Both A's queue and B's reverse path must survive.
    drop(a);
    drop(b);
    let mut a = Node::start(&a_dir, a_addr, b_addr);
    let mut b = Node::start(&b_dir, b_addr, c_addr);
    a.wait_for("1 pending bundle(s) restored");
    b.wait_for("1 pending bundle(s) restored");
    let mut c = Node::start(&c_dir, c_addr, a_addr);
    c.wait_for(&format!(
        "message from ipn:1:{}: hello through B",
        a_addr.port()
    ));
    b.wait_for("relayed delivery ACK");
    a.wait_for("delivered and acknowledged");
    assert_eq!(pending_count(&a_dir, a_addr.port()), 0);
    assert_eq!(pending_count(&b_dir, b_addr.port()), 0);
    assert!(!b.history.iter().any(|line| line.contains("message from")));
}
