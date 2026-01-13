//! Library for downloading videos from Bluesky posts
//!
//! This library handles:
//! - Fetching video metadata from Bluesky posts
//! - Parsing HLS playlists
//! - Downloading video segments
//! - Converting MPEG-TS to MP4

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::io::Cursor;

const PUBLIC_API: &str = "https://public.api.bsky.app/xrpc";

// ============================================================================
// Public Types
// ============================================================================

/// Information about a video from a Bluesky post
#[derive(Debug, Clone)]
pub struct VideoInfo {
    /// The post ID from the URL
    pub post_id: String,
    /// The HLS master playlist URL
    pub playlist_url: String,
}

/// An HLS variant (quality level)
#[derive(Debug, Clone)]
pub struct HlsVariant {
    pub bandwidth: u64,
    pub resolution: Option<String>,
    pub uri: String,
}

/// An HLS segment
#[derive(Debug, Clone)]
pub struct HlsSegment {
    pub uri: String,
}

// ============================================================================
// Internal API Response Types
// ============================================================================

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

#[derive(Debug, Deserialize)]
struct VideoView {
    playlist: String,
}

// ============================================================================
// URL Parsing
// ============================================================================

/// Parse a Bluesky URL to extract handle and post ID
pub fn parse_bsky_url(url: &str) -> Result<(String, String)> {
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

/// Resolve a potentially relative URL against a base URL
pub fn resolve_url(base: &str, relative: &str) -> String {
    if relative.starts_with("http://") || relative.starts_with("https://") {
        relative.to_string()
    } else {
        let base_dir = base.rsplit_once('/').map(|(b, _)| b).unwrap_or(base);
        format!("{}/{}", base_dir, relative)
    }
}

// ============================================================================
// Bluesky API Functions
// ============================================================================

/// Resolve a Bluesky handle to a DID
pub async fn resolve_handle(client: &reqwest::Client, handle: &str) -> Result<String> {
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

/// Fetch video info from a Bluesky post URL
pub async fn fetch_video_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
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

    let embed = response
        .thread
        .post
        .embed
        .ok_or_else(|| anyhow!("Post does not contain any embed"))?;

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

// ============================================================================
// HLS Parsing
// ============================================================================

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

/// Parse HLS master playlist to extract quality variants
pub async fn parse_master_playlist(client: &reqwest::Client, url: &str) -> Result<Vec<HlsVariant>> {
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

/// Parse HLS media playlist to get segment list
pub async fn parse_media_playlist(client: &reqwest::Client, url: &str) -> Result<Vec<HlsSegment>> {
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
        if !line.starts_with('#') && !line.is_empty() {
            segments.push(HlsSegment {
                uri: line.to_string(),
            });
        }
    }

    Ok(segments)
}

/// Select the highest quality variant from a list
pub fn select_best_variant(variants: &[HlsVariant]) -> Option<&HlsVariant> {
    variants.iter().max_by_key(|v| v.bandwidth)
}

// ============================================================================
// Video Download
// ============================================================================

/// Download video segments to memory and return as a single byte vector
///
/// This function downloads all HLS segments and concatenates them into a
/// single MPEG-TS byte stream in memory.
pub async fn download_video_to_memory(
    client: &reqwest::Client,
    playlist_url: &str,
) -> Result<Vec<u8>> {
    // Parse master playlist
    let variants = parse_master_playlist(client, playlist_url).await?;

    if variants.is_empty() {
        return Err(anyhow!("No video variants found in playlist"));
    }

    // Select best quality
    let best = select_best_variant(&variants)
        .ok_or_else(|| anyhow!("Failed to select video quality"))?;

    // Parse media playlist
    let media_url = resolve_url(playlist_url, &best.uri);
    let segments = parse_media_playlist(client, &media_url).await?;

    // Download all segments
    let mut video_data = Vec::new();

    for segment in &segments {
        let segment_url = resolve_url(&media_url, &segment.uri);
        let response = client
            .get(&segment_url)
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("Failed to download segment: {}", segment.uri))?;

        let bytes = response.bytes().await?;
        video_data.extend_from_slice(&bytes);
    }

    Ok(video_data)
}

/// Download video segments with progress callback
///
/// The callback receives (segments_done, total_segments) after each segment.
pub async fn download_video_to_memory_with_progress<F>(
    client: &reqwest::Client,
    playlist_url: &str,
    mut progress: F,
) -> Result<Vec<u8>>
where
    F: FnMut(usize, usize),
{
    // Parse master playlist
    let variants = parse_master_playlist(client, playlist_url).await?;

    if variants.is_empty() {
        return Err(anyhow!("No video variants found in playlist"));
    }

    // Select best quality
    let best = select_best_variant(&variants)
        .ok_or_else(|| anyhow!("Failed to select video quality"))?;

    // Parse media playlist
    let media_url = resolve_url(playlist_url, &best.uri);
    let segments = parse_media_playlist(client, &media_url).await?;

    let total = segments.len();
    let mut video_data = Vec::new();

    for (i, segment) in segments.iter().enumerate() {
        let segment_url = resolve_url(&media_url, &segment.uri);
        let response = client
            .get(&segment_url)
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("Failed to download segment: {}", segment.uri))?;

        let bytes = response.bytes().await?;
        video_data.extend_from_slice(&bytes);

        progress(i + 1, total);
    }

    Ok(video_data)
}

// ============================================================================
// Video Conversion
// ============================================================================

/// Convert MPEG-TS data to MP4 in memory
///
/// Takes raw MPEG-TS bytes and returns MP4 bytes.
pub fn convert_to_mp4(ts_data: &[u8]) -> Result<Vec<u8>> {
    let input = Cursor::new(ts_data);
    let mut output = Cursor::new(Vec::new());

    ts_to_mp4::remux(input, &mut output).context("Failed to remux TS to MP4")?;

    Ok(output.into_inner())
}

// ============================================================================
// High-Level API
// ============================================================================

/// Download a video from a Bluesky post URL and return as MP4 bytes
///
/// This is a convenience function that:
/// 1. Fetches video info from the post
/// 2. Downloads all HLS segments
/// 3. Converts to MP4 format
pub async fn download_video_as_mp4(client: &reqwest::Client, bsky_url: &str) -> Result<Vec<u8>> {
    let video_info = fetch_video_info(client, bsky_url).await?;
    let ts_data = download_video_to_memory(client, &video_info.playlist_url).await?;
    let mp4_data = convert_to_mp4(&ts_data)?;
    Ok(mp4_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bsky_url() {
        let (handle, post_id) =
            parse_bsky_url("https://bsky.app/profile/user.bsky.social/post/abc123").unwrap();
        assert_eq!(handle, "user.bsky.social");
        assert_eq!(post_id, "abc123");
    }

    #[test]
    fn test_parse_bsky_url_with_query() {
        let (handle, post_id) =
            parse_bsky_url("https://bsky.app/profile/user.bsky.social/post/abc123?ref=share")
                .unwrap();
        assert_eq!(handle, "user.bsky.social");
        assert_eq!(post_id, "abc123");
    }

    #[test]
    fn test_resolve_url_absolute() {
        let url = resolve_url("https://example.com/video/", "https://cdn.example.com/file.ts");
        assert_eq!(url, "https://cdn.example.com/file.ts");
    }

    #[test]
    fn test_resolve_url_relative() {
        let url = resolve_url("https://example.com/video/playlist.m3u8", "720p/video.m3u8");
        assert_eq!(url, "https://example.com/video/720p/video.m3u8");
    }
}
