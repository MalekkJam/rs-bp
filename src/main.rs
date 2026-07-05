mod app;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = app::cli::run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
