use anyhow::{anyhow, Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;
use std::process::Command;
use tokio::io::AsyncWriteExt;

const PUBLIC_API: &str = "https://public.api.bsky.app/xrpc";

#[derive(Parser)]
#[command(name = "bsky-video-dl")]
#[command(about = "Download videos from Bluesky posts")]
struct Args {
    /// Bluesky post URL containing a video
    url: String,

    /// Output filename (auto-generated from post ID if not provided)
    #[arg(short, long)]
    output: Option<String>,

    /// Convert to MP4 using ffmpeg (requires ffmpeg in PATH)
    #[arg(long)]
    mp4: bool,

    /// Show verbose output
    #[arg(short, long)]
    verbose: bool,
}

// API Response Types

#[derive(Debug, Deserialize)]
struct ResolveHandleResponse {
    did: String,
}

#[derive(Debug, Deserialize)]
struct ThreadResponse {
    thread: ThreadViewPost,
}

#[derive(Debug, Deserialize)]
struct ThreadViewPost {
    post: Post,
}

#[derive(Debug, Deserialize)]
struct Post {
    embed: Option<serde_json::Value>,
}

// Video embed structure from app.bsky.embed.video#view
#[derive(Debug, Deserialize)]
struct VideoView {
    playlist: String,
}

// HLS Types

struct HlsVariant {
    bandwidth: u64,
    resolution: Option<String>,
    uri: String,
}

struct HlsSegment {
    uri: String,
}

// Video info extracted from post
struct VideoInfo {
    post_id: String,
    playlist_url: String,
}

/// Parse a Bluesky URL to extract handle and post ID
fn parse_bsky_url(url: &str) -> Result<(String, String)> {
    let url = url.trim_end_matches('/');
    let parts: Vec<&str> = url.split('/').collect();

    let profile_idx = parts.iter().position(|&p| p == "profile");
    let post_idx = parts.iter().position(|&p| p == "post");

    match (profile_idx, post_idx) {
        (Some(p_idx), Some(post_idx)) if p_idx + 1 < parts.len() && post_idx + 1 < parts.len() => {
            let handle = parts[p_idx + 1].to_string();
            let post_id = parts[post_idx + 1]
                .split('?')
                .next()
                .unwrap_or(parts[post_idx + 1])
                .to_string();
            Ok((handle, post_id))
        }
        _ => Err(anyhow!(
            "Invalid Bluesky URL format. Expected: https://bsky.app/profile/USER/post/POSTID"
        )),
    }
}

/// Resolve a Bluesky handle to a DID
async fn resolve_handle(client: &reqwest::Client, handle: &str) -> Result<String> {
    // If already a DID, return as-is
    if handle.starts_with("did:") {
        return Ok(handle.to_string());
    }

    let url = format!(
        "{}/com.atproto.identity.resolveHandle?handle={}",
        PUBLIC_API, handle
    );

    let response: ResolveHandleResponse = client
        .get(&url)
        .send()
        .await?
        .error_for_status()
        .context("Failed to resolve handle")?
        .json()
        .await?;

    Ok(response.did)
}

/// Fetch video info from a Bluesky post
async fn fetch_video_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
    let (handle, post_id) = parse_bsky_url(url)?;
    let did = resolve_handle(client, &handle).await?;

    let at_uri = format!("at://{}/app.bsky.feed.post/{}", did, post_id);
    let api_url = format!(
        "{}/app.bsky.feed.getPostThread?uri={}&depth=0&parentHeight=0",
        PUBLIC_API,
        urlencoding::encode(&at_uri)
    );

    let response: ThreadResponse = client
        .get(&api_url)
        .send()
        .await?
        .error_for_status()
        .context("Failed to fetch post")?
        .json()
        .await?;

    // Extract video embed
    let embed = response
        .thread
        .post
        .embed
        .ok_or_else(|| anyhow!("Post does not contain any embed"))?;

    // Check if it's a video embed
    let embed_type = embed
        .get("$type")
        .and_then(|t| t.as_str())
        .unwrap_or("");

    if embed_type != "app.bsky.embed.video#view" {
        return Err(anyhow!(
            "Post does not contain a video (found embed type: {})",
            embed_type
        ));
    }

    let video_view: VideoView =
        serde_json::from_value(embed).context("Failed to parse video embed")?;

    Ok(VideoInfo {
        post_id,
        playlist_url: video_view.playlist,
    })
}

/// Parse HLS master playlist to extract quality variants
async fn parse_master_playlist(client: &reqwest::Client, url: &str) -> Result<Vec<HlsVariant>> {
    let content = client
        .get(url)
        .send()
        .await?
        .error_for_status()
        .context("Failed to fetch master playlist")?
        .text()
        .await?;

    let mut variants = Vec::new();
    let lines: Vec<&str> = content.lines().collect();

    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];

        if line.starts_with("#EXT-X-STREAM-INF:") {
            let attrs = &line["#EXT-X-STREAM-INF:".len()..];
            let bandwidth = extract_attribute(attrs, "BANDWIDTH")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let resolution = extract_attribute(attrs, "RESOLUTION");

            // Next non-empty, non-comment line is the URI
            if i + 1 < lines.len() {
                let uri = lines[i + 1].to_string();
                if !uri.starts_with('#') && !uri.is_empty() {
                    variants.push(HlsVariant {
                        bandwidth,
                        resolution,
                        uri,
                    });
                }
            }
        }
        i += 1;
    }

    Ok(variants)
}

/// Extract an attribute value from HLS tag attributes
fn extract_attribute(attrs: &str, name: &str) -> Option<String> {
    for attr in attrs.split(',') {
        if let Some((key, value)) = attr.split_once('=') {
            if key == name {
                return Some(value.trim_matches('"').to_string());
            }
        }
    }
    None
}

/// Select the highest quality variant
fn select_best_variant(variants: &[HlsVariant]) -> Option<&HlsVariant> {
    variants.iter().max_by_key(|v| v.bandwidth)
}

/// Parse HLS media playlist to get segment list
async fn parse_media_playlist(client: &reqwest::Client, url: &str) -> Result<Vec<HlsSegment>> {
    let content = client
        .get(url)
        .send()
        .await?
        .error_for_status()
        .context("Failed to fetch media playlist")?
        .text()
        .await?;

    let mut segments = Vec::new();

    for line in content.lines() {
        // Skip comments and empty lines, collect segment URIs
        if !line.starts_with('#') && !line.is_empty() {
            segments.push(HlsSegment {
                uri: line.to_string(),
            });
        }
    }

    Ok(segments)
}

/// Resolve a potentially relative URL against a base URL
fn resolve_url(base: &str, relative: &str) -> String {
    if relative.starts_with("http://") || relative.starts_with("https://") {
        relative.to_string()
    } else {
        // Get base directory and append relative path
        let base_dir = base.rsplit_once('/').map(|(b, _)| b).unwrap_or(base);
        format!("{}/{}", base_dir, relative)
    }
}

/// Download video segments and concatenate into output file
async fn download_video(
    client: &reqwest::Client,
    base_url: &str,
    segments: &[HlsSegment],
    output_path: &str,
    verbose: bool,
) -> Result<()> {
    let mut file = tokio::fs::File::create(output_path)
        .await
        .context("Failed to create output file")?;

    let pb = ProgressBar::new(segments.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} segments ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    for segment in segments {
        let segment_url = resolve_url(base_url, &segment.uri);

        if verbose {
            eprintln!("Downloading: {}", segment_url);
        }

        let response = client
            .get(&segment_url)
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("Failed to download segment: {}", segment.uri))?;

        let bytes = response.bytes().await?;
        file.write_all(&bytes).await?;

        pb.inc(1);
    }

    pb.finish_with_message("Download complete");
    file.flush().await?;

    Ok(())
}

/// Convert .ts file to .mp4 using ffmpeg
fn convert_to_mp4(input_path: &str, output_path: &str) -> Result<()> {
    let status = Command::new("ffmpeg")
        .args([
            "-i", input_path, "-c", "copy", // Copy streams without re-encoding
            "-y", // Overwrite output file
            output_path,
        ])
        .status()
        .context("Failed to run ffmpeg. Is ffmpeg installed and in PATH?")?;

    if !status.success() {
        return Err(anyhow!("ffmpeg conversion failed with exit code: {:?}", status.code()));
    }

    // Remove the .ts file after successful conversion
    std::fs::remove_file(input_path).context("Failed to remove temporary .ts file")?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let client = reqwest::Client::new();

    // Step 1: Fetch video info from post
    eprintln!("Fetching video info...");
    let video_info = fetch_video_info(&client, &args.url).await?;

    if args.verbose {
        eprintln!("Playlist URL: {}", video_info.playlist_url);
    }

    // Step 2: Parse master playlist
    let variants = parse_master_playlist(&client, &video_info.playlist_url).await?;

    if variants.is_empty() {
        return Err(anyhow!("No video variants found in playlist"));
    }

    // Step 3: Select highest quality
    let best = select_best_variant(&variants)
        .ok_or_else(|| anyhow!("Failed to select video quality"))?;

    eprintln!(
        "Selected quality: {} (bandwidth: {})",
        best.resolution.as_deref().unwrap_or("unknown"),
        best.bandwidth
    );

    // Step 4: Parse media playlist for segments
    let media_url = resolve_url(&video_info.playlist_url, &best.uri);
    let segments = parse_media_playlist(&client, &media_url).await?;

    eprintln!("Found {} segments to download", segments.len());

    // Step 5: Determine output filename
    let ts_filename = args
        .output
        .clone()
        .unwrap_or_else(|| format!("{}.ts", video_info.post_id));

    // Step 6: Download and concatenate segments
    download_video(&client, &media_url, &segments, &ts_filename, args.verbose).await?;

    // Step 7: Optional MP4 conversion
    if args.mp4 {
        let mp4_filename = if ts_filename.ends_with(".ts") {
            ts_filename.replace(".ts", ".mp4")
        } else {
            format!("{}.mp4", ts_filename)
        };
        eprintln!("Converting to MP4...");
        convert_to_mp4(&ts_filename, &mp4_filename)?;
        eprintln!("Saved: {}", mp4_filename);
    } else {
        eprintln!("Saved: {}", ts_filename);
    }

    Ok(())
}
