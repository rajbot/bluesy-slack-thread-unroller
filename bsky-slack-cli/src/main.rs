use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;
use std::process::Command;

const DEFAULT_DOMAIN: &str = "salesforce.enterprise";

#[derive(Parser)]
#[command(name = "bsky-slack-thread")]
#[command(about = "Unroll a Bluesky thread into a Slack thread using scli")]
struct Args {
    /// Bluesky post URL (e.g., https://bsky.app/profile/user.bsky.social/post/xyz)
    url: String,

    /// Slack channel ID (e.g., C12345678) or channel URL (https://workspace.slack.com/archives/C12345678)
    channel: String,

    /// Path to the scli binary (defaults to ~/dev/scli/scli)
    #[arg(long)]
    scli: Option<PathBuf>,

    /// Slack workspace domain used to build thread permalinks when CHANNEL is a bare ID
    #[arg(long, default_value = DEFAULT_DOMAIN)]
    domain: String,

    /// Delay between posts in milliseconds
    #[arg(long, default_value_t = 10_000)]
    delay_ms: u64,

    /// Print the messages that would be sent without posting them
    #[arg(long)]
    dry_run: bool,
}

#[derive(Deserialize)]
struct SendResponse {
    channel: String,
    ts: String,
}

/// Parse a channel argument into (channel_id, domain).
/// Accepts a bare channel ID or a channel URL like https://workspace.slack.com/archives/C123.
fn parse_channel(channel: &str, default_domain: &str) -> Result<(String, String)> {
    if let Some(rest) = channel.strip_prefix("https://") {
        let (host, path) = rest
            .split_once('/')
            .ok_or_else(|| anyhow!("Invalid channel URL: {}", channel))?;
        let domain = host
            .strip_suffix(".slack.com")
            .ok_or_else(|| anyhow!("Channel URL must be on slack.com: {}", channel))?;
        let id = path
            .strip_prefix("archives/")
            .and_then(|p| p.split('/').next())
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow!("Channel URL must look like /archives/C123: {}", channel))?;
        return Ok((id.to_string(), domain.to_string()));
    }

    let valid = channel.len() > 1
        && channel.starts_with(['C', 'D', 'G'])
        && channel.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
    if !valid {
        bail!("Invalid channel: {} (expected a channel ID like C12345678 or a channel URL)", channel);
    }
    Ok((channel.to_string(), default_domain.to_string()))
}

/// Build a Slack thread permalink that scli accepts as a reply target.
fn thread_permalink(domain: &str, channel_id: &str, ts: &str) -> String {
    format!("https://{}.slack.com/archives/{}/p{}", domain, channel_id, ts.replace('.', ""))
}

fn default_scli_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join("dev/scli/scli"))
}

/// Send a message with `scli send <target> <text>` and return the parsed response.
fn scli_send(scli: &PathBuf, target: &str, text: &str) -> Result<SendResponse> {
    let output = Command::new(scli)
        .args(["send", target, text])
        .output()
        .with_context(|| format!("Failed to run {}", scli.display()))?;

    // scli prints errors to stdout, so include both streams on failure
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("scli send failed: {}{}", stdout.trim(), stderr.trim());
    }

    serde_json::from_str(&stdout).with_context(|| format!("Failed to parse scli output: {}", stdout.trim()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let (channel_id, domain) = parse_channel(&args.channel, &args.domain)?;
    let scli = match args.scli {
        Some(path) => path,
        None => default_scli_path()?,
    };

    let (thread, total_count) = bsky_thread_lib::fetch_thread_with_total(&args.url).await?;
    if total_count == 0 {
        bail!("No posts found in thread");
    }
    eprintln!("Fetched {} posts by @{}", total_count, thread.author.handle);

    let messages: Vec<String> = thread
        .posts
        .iter()
        .enumerate()
        .map(|(i, post)| format!("[{}/{}] {}", i + 1, total_count, post.url))
        .collect();

    if args.dry_run {
        eprintln!("Dry run: would post to {} via {}", channel_id, scli.display());
        for (i, message) in messages.iter().enumerate() {
            let prefix = if i == 0 { "top-level" } else { "reply" };
            println!("{}: {}", prefix, message);
        }
        return Ok(());
    }

    // Post the root post as the top-level channel message
    // Use a channel URL so scli posts to the right workspace domain
    let channel_url = format!("https://{}.slack.com/archives/{}", domain, channel_id);
    let root = scli_send(&scli, &channel_url, &messages[0])?;
    let permalink = thread_permalink(&domain, &root.channel, &root.ts);
    eprintln!("Posted [1/{}] top-level message: {}", total_count, permalink);

    // Thread the remaining posts under it
    for (i, message) in messages.iter().enumerate().skip(1) {
        tokio::time::sleep(tokio::time::Duration::from_millis(args.delay_ms)).await;
        scli_send(&scli, &permalink, message)
            .with_context(|| format!("Failed to post message {} of {}", i + 1, total_count))?;
        eprintln!("Posted [{}/{}]", i + 1, total_count);
    }

    println!("{}", permalink);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_channel_bare_id() {
        let (id, domain) = parse_channel("C12345678", DEFAULT_DOMAIN).unwrap();
        assert_eq!(id, "C12345678");
        assert_eq!(domain, DEFAULT_DOMAIN);
    }

    #[test]
    fn test_parse_channel_url() {
        let (id, domain) = parse_channel("https://myteam.slack.com/archives/C12345678", DEFAULT_DOMAIN).unwrap();
        assert_eq!(id, "C12345678");
        assert_eq!(domain, "myteam");
    }

    #[test]
    fn test_parse_channel_url_with_trailing_path() {
        let (id, domain) = parse_channel("https://myteam.slack.com/archives/C12345678/", DEFAULT_DOMAIN).unwrap();
        assert_eq!(id, "C12345678");
        assert_eq!(domain, "myteam");
    }

    #[test]
    fn test_parse_channel_invalid() {
        assert!(parse_channel("general", DEFAULT_DOMAIN).is_err());
        assert!(parse_channel("C", DEFAULT_DOMAIN).is_err());
        assert!(parse_channel("https://example.com/archives/C123", DEFAULT_DOMAIN).is_err());
        assert!(parse_channel("https://myteam.slack.com/messages/C123", DEFAULT_DOMAIN).is_err());
    }

    #[test]
    fn test_thread_permalink() {
        assert_eq!(
            thread_permalink("myteam", "C12345678", "1749691322.335509"),
            "https://myteam.slack.com/archives/C12345678/p1749691322335509"
        );
    }
}
