use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(name = "bsky-thread")]
#[command(about = "Fetch a Bluesky thread as JSON, showing only the author's replies")]
struct Args {
    /// Bluesky post URL (e.g., https://bsky.app/profile/user.bsky.social/post/xyz)
    url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let output = bsky_thread_lib::fetch_thread(&args.url).await?;

    println!("{}", serde_json::to_string_pretty(&output)?);

    Ok(())
}
