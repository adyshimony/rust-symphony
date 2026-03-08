use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match symphony_rust::cli::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err:#}");
            ExitCode::FAILURE
        }
    }
}
