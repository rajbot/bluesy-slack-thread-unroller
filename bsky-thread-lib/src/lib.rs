use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

const PUBLIC_API: &str = "https://public.api.bsky.app/xrpc";

// API Response types
#[derive(Debug, Deserialize)]
struct ResolveHandleResponse {
    did: String,
}

#[derive(Debug, Deserialize)]
struct ThreadResponse {
    thread: ThreadViewPost,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ThreadViewPost {
    post: Post,
    #[serde(default)]
    replies: Vec<ThreadViewPostOrBlocked>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
enum ThreadViewPostOrBlocked {
    ThreadView(ThreadViewPost),
    Blocked(BlockedOrNotFound),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct BlockedOrNotFound {
    #[serde(rename = "$type")]
    type_field: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Post {
    uri: String,
    cid: String,
    author: Author,
    record: Record,
    #[serde(rename = "indexedAt")]
    indexed_at: String,
    #[serde(rename = "likeCount")]
    like_count: Option<u64>,
    #[serde(rename = "repostCount")]
    repost_count: Option<u64>,
    #[serde(rename = "replyCount")]
    reply_count: Option<u64>,
    embed: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Author {
    did: String,
    handle: String,
    #[serde(rename = "displayName")]
    display_name: Option<String>,
    avatar: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Record {
    text: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(default)]
    facets: Vec<serde_json::Value>,
    embed: Option<serde_json::Value>,
}

// Output types (simplified for clean JSON output)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadOutput {
    pub author: AuthorOutput,
    pub posts: Vec<PostOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorOutput {
    pub did: String,
    pub handle: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostOutput {
    pub uri: String,
    pub url: String,
    pub text: String,
    pub created_at: String,
    pub likes: Option<u64>,
    pub reposts: Option<u64>,
    pub replies: Option<u64>,
    pub embed: Option<serde_json::Value>,
}

fn at_uri_to_web_url(at_uri: &str, handle: &str) -> String {
    // Convert at://did:plc:xxx/app.bsky.feed.post/yyy to https://bsky.app/profile/handle/post/yyy
    let post_id = at_uri.rsplit('/').next().unwrap_or("");
    format!("https://bsky.app/profile/{}/post/{}", handle, post_id)
}

/// Parse a Bluesky URL and extract the handle and post ID.
///
/// # Example
/// ```
/// use bsky_thread_lib::parse_bsky_url;
/// let (handle, post_id) = parse_bsky_url("https://bsky.app/profile/simonwillison.net/post/3m7gzjew3ss2e").unwrap();
/// assert_eq!(handle, "simonwillison.net");
/// assert_eq!(post_id, "3m7gzjew3ss2e");
/// ```
pub fn parse_bsky_url(url: &str) -> Result<(String, String)> {
    // Parse URLs like https://bsky.app/profile/simonwillison.net/post/3m7gzjew3ss2e
    let url = url.trim_end_matches('/');

    let parts: Vec<&str> = url.split('/').collect();

    // Find "profile" and "post" indices
    let profile_idx = parts.iter().position(|&p| p == "profile")
        .ok_or_else(|| anyhow!("Invalid Bluesky URL: missing 'profile' segment"))?;
    let post_idx = parts.iter().position(|&p| p == "post")
        .ok_or_else(|| anyhow!("Invalid Bluesky URL: missing 'post' segment"))?;

    if profile_idx + 1 >= parts.len() || post_idx + 1 >= parts.len() {
        return Err(anyhow!("Invalid Bluesky URL: incomplete path"));
    }

    let handle = parts[profile_idx + 1].to_string();
    let post_id = parts[post_idx + 1].to_string();

    Ok((handle, post_id))
}

async fn resolve_handle(client: &reqwest::Client, handle: &str) -> Result<String> {
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

async fn get_post_thread(client: &reqwest::Client, at_uri: &str) -> Result<ThreadResponse> {
    let url = format!(
        "{}/app.bsky.feed.getPostThread?uri={}&depth=100&parentHeight=0",
        PUBLIC_API,
        urlencoding::encode(at_uri)
    );

    let response: ThreadResponse = client
        .get(&url)
        .send()
        .await?
        .error_for_status()
        .context("Failed to fetch thread")?
        .json()
        .await?;

    Ok(response)
}

fn collect_author_posts(thread: &ThreadViewPost, author_did: &str, author_handle: &str) -> Vec<PostOutput> {
    let mut posts = Vec::new();

    // Add the root post
    posts.push(PostOutput {
        uri: thread.post.uri.clone(),
        url: at_uri_to_web_url(&thread.post.uri, author_handle),
        text: thread.post.record.text.clone(),
        created_at: thread.post.record.created_at.clone(),
        likes: thread.post.like_count,
        reposts: thread.post.repost_count,
        replies: thread.post.reply_count,
        embed: thread.post.embed.clone(),
    });

    // Recursively collect author's replies
    collect_author_replies(&thread.replies, author_did, author_handle, &mut posts);

    posts
}

fn collect_author_replies(
    replies: &[ThreadViewPostOrBlocked],
    author_did: &str,
    author_handle: &str,
    posts: &mut Vec<PostOutput>,
) {
    for reply in replies {
        if let ThreadViewPostOrBlocked::ThreadView(thread_view) = reply {
            // Only include posts by the original author
            if thread_view.post.author.did == author_did {
                posts.push(PostOutput {
                    uri: thread_view.post.uri.clone(),
                    url: at_uri_to_web_url(&thread_view.post.uri, author_handle),
                    text: thread_view.post.record.text.clone(),
                    created_at: thread_view.post.record.created_at.clone(),
                    likes: thread_view.post.like_count,
                    reposts: thread_view.post.repost_count,
                    replies: thread_view.post.reply_count,
                    embed: thread_view.post.embed.clone(),
                });

                // Continue searching in this author's replies
                collect_author_replies(&thread_view.replies, author_did, author_handle, posts);
            }
        }
    }
}

/// Fetch a Bluesky thread and return only the posts by the original author.
///
/// # Arguments
/// * `url` - A Bluesky post URL (e.g., "https://bsky.app/profile/user.bsky.social/post/xyz")
///
/// # Returns
/// A `ThreadOutput` containing the author info and all their posts in the thread.
pub async fn fetch_thread(url: &str) -> Result<ThreadOutput> {
    let (handle, post_id) = parse_bsky_url(url)?;

    let client = reqwest::Client::new();

    // Resolve handle to DID (unless it's already a DID)
    let did = if handle.starts_with("did:") {
        handle.clone()
    } else {
        resolve_handle(&client, &handle).await?
    };

    // Build AT URI
    let at_uri = format!("at://{}/app.bsky.feed.post/{}", did, post_id);

    // Fetch thread
    let thread_response = get_post_thread(&client, &at_uri).await?;

    // Extract author info
    let author = &thread_response.thread.post.author;
    let author_did = &author.did;

    // Collect all posts by the author
    let posts = collect_author_posts(&thread_response.thread, author_did, &author.handle);

    // Build output
    Ok(ThreadOutput {
        author: AuthorOutput {
            did: author.did.clone(),
            handle: author.handle.clone(),
            display_name: author.display_name.clone(),
        },
        posts,
    })
}
