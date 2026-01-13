use anyhow::{anyhow, Result};
use bsky_video_lib::{
    convert_to_mp4, fetch_video_info, parse_master_playlist, parse_media_playlist, resolve_url,
    select_best_variant,
};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::io::AsyncWriteExt;

#[derive(Parser)]
#[command(name = "bsky-video-dl")]
#[command(about = "Download videos from Bluesky posts")]
struct Args {
    /// Bluesky post URL containing a video
    url: String,

    /// Output filename (auto-generated from post ID if not provided)
    #[arg(short, long)]
    output: Option<String>,

    /// Convert to MP4 (uses native Rust remuxing, no ffmpeg needed)
    #[arg(long)]
    mp4: bool,

    /// Show verbose output
    #[arg(short, long)]
    verbose: bool,
}

/// Download video segments to a file with progress display
async fn download_video_to_file(
    client: &reqwest::Client,
    playlist_url: &str,
    output_path: &str,
    verbose: bool,
) -> Result<()> {
    // Parse master playlist
    let variants = parse_master_playlist(client, playlist_url).await?;

    if variants.is_empty() {
        return Err(anyhow!("No video variants found in playlist"));
    }

    // Select best quality
    let best = select_best_variant(&variants)
        .ok_or_else(|| anyhow!("Failed to select video quality"))?;

    eprintln!(
        "Selected quality: {} (bandwidth: {})",
        best.resolution.as_deref().unwrap_or("unknown"),
        best.bandwidth
    );

    // Parse media playlist
    let media_url = resolve_url(playlist_url, &best.uri);
    let segments = parse_media_playlist(client, &media_url).await?;

    eprintln!("Found {} segments to download", segments.len());

    // Create output file
    let mut file = tokio::fs::File::create(output_path).await?;

    // Set up progress bar
    let pb = ProgressBar::new(segments.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} segments ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Download segments
    for segment in &segments {
        let segment_url = resolve_url(&media_url, &segment.uri);

        if verbose {
            eprintln!("Downloading: {}", segment_url);
        }

        let response = client.get(&segment_url).send().await?.error_for_status()?;
        let bytes = response.bytes().await?;
        file.write_all(&bytes).await?;

        pb.inc(1);
    }

    pb.finish_with_message("Download complete");
    file.flush().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = reqwest::Client::new();

    // Fetch video info from post
    eprintln!("Fetching video info...");
    let video_info = fetch_video_info(&client, &args.url).await?;

    if args.verbose {
        eprintln!("Playlist URL: {}", video_info.playlist_url);
    }

    // Determine output filename
    let ts_filename = args
        .output
        .clone()
        .unwrap_or_else(|| format!("{}.ts", video_info.post_id));

    // Download video
    download_video_to_file(&client, &video_info.playlist_url, &ts_filename, args.verbose).await?;

    // Optional MP4 conversion
    if args.mp4 {
        let mp4_filename = if ts_filename.ends_with(".ts") {
            ts_filename.replace(".ts", ".mp4")
        } else {
            format!("{}.mp4", ts_filename)
        };

        eprintln!("Converting to MP4...");

        // Read the TS file
        let ts_data = tokio::fs::read(&ts_filename).await?;

        // Convert to MP4
        let mp4_data = convert_to_mp4(&ts_data)?;

        // Write MP4 file
        tokio::fs::write(&mp4_filename, &mp4_data).await?;

        // Remove the .ts file
        tokio::fs::remove_file(&ts_filename).await?;

        eprintln!("Saved: {}", mp4_filename);
    } else {
        eprintln!("Saved: {}", ts_filename);
    }

    Ok(())
}
