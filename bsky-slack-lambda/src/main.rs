use anyhow::{anyhow, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use lambda_runtime::{service_fn, Error as LambdaError, LambdaEvent};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use tracing::{error, info, warn};
use urlencoding::decode;

const SHORTCUT_CALLBACK_ID: &str = "unroll_bluesky_thread";
const BATCH_SIZE: usize = 10;
const LOAD_MORE_ACTION_ID: &str = "load_more_thread";

type HmacSha256 = Hmac<Sha256>;

#[tokio::main]
async fn main() -> std::result::Result<(), LambdaError> {
    // Initialize tracing with JSON formatter for CloudWatch
    tracing_subscriber::fmt()
        .with_target(false)
        .without_time()
        .json()
        .init();

    info!("Bluesky Thread Unroller Lambda starting");

    lambda_runtime::run(service_fn(handle_request)).await
}

async fn handle_request(
    event: LambdaEvent<ApiGatewayProxyRequest>,
) -> std::result::Result<ApiGatewayProxyResponse, LambdaError> {
    let (request, _context) = event.into_parts();

    match process_request(request).await {
        Ok(response) => Ok(response),
        Err(e) => {
            error!("Error processing request: {}", e);
            Ok(ApiGatewayProxyResponse {
                status_code: 200, // Return 200 to avoid Slack retries
                headers: HashMap::new(),
                body: Some(format!(r#"{{"text":"Error: {}"}}"#, e)),
                is_base64_encoded: false,
            })
        }
    }
}

async fn process_request(
    request: ApiGatewayProxyRequest,
) -> Result<ApiGatewayProxyResponse> {
    let raw_body = request.body.unwrap_or_default();
    let headers = request.headers.unwrap_or_default();

    // Decode body if base64-encoded
    let body = if request.is_base64_encoded.unwrap_or(false) {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&raw_body)
            .map_err(|e| anyhow!("Failed to decode base64 body: {}", e))?;
        String::from_utf8(decoded)
            .map_err(|e| anyhow!("Body is not valid UTF-8: {}", e))?
    } else {
        raw_body
    };

    // Verify Slack request signature
    if let Err(e) = verify_slack_signature(&headers, &body) {
        warn!("Invalid request signature: {}", e);
        return Ok(ApiGatewayProxyResponse {
            status_code: 401,
            headers: HashMap::new(),
            body: Some("Invalid signature".to_string()),
            is_base64_encoded: false,
        });
    }

    let content_type = headers
        .get("content-type")
        .or_else(|| headers.get("Content-Type"))
        .map(|s| s.to_lowercase())
        .unwrap_or_default();

    // Parse form-encoded payload (interactive requests)
    if content_type.contains("application/x-www-form-urlencoded") {
        let form_data = parse_form_data(&body)?;

        if let Some(payload_str) = form_data.get("payload") {
            // Parse as generic Value first to inspect type
            let payload_value: Value = serde_json::from_str(payload_str)?;

            // Check if it's a block_actions payload (button click)
            if payload_value.get("type").and_then(|t| t.as_str()) == Some("block_actions") {
                let block_actions: BlockActionsPayload = serde_json::from_value(payload_value)?;
                info!("Received block_actions");
                return handle_block_actions(block_actions).await;
            }

            // Otherwise, try as shortcut
            let payload: ShortcutPayload = serde_json::from_value(payload_value)?;
            if payload.callback_id.as_deref() == Some(SHORTCUT_CALLBACK_ID) {
                info!("Received unroll_bluesky_thread shortcut");
                return handle_bluesky_shortcut(payload).await;
            }
        }
    }

    // Return empty 200 for unhandled requests
    Ok(ApiGatewayProxyResponse {
        status_code: 200,
        headers: HashMap::new(),
        body: None,
        is_base64_encoded: false,
    })
}

fn verify_slack_signature(headers: &HashMap<String, String>, body: &str) -> Result<()> {
    let signing_secret = std::env::var("SLACK_SIGNING_SECRET")
        .map_err(|_| anyhow!("SLACK_SIGNING_SECRET not set"))?;

    let timestamp = headers
        .get("x-slack-request-timestamp")
        .or_else(|| headers.get("X-Slack-Request-Timestamp"))
        .ok_or_else(|| anyhow!("Missing timestamp header"))?;

    let signature = headers
        .get("x-slack-signature")
        .or_else(|| headers.get("X-Slack-Signature"))
        .ok_or_else(|| anyhow!("Missing signature header"))?;

    // Create the signature base string
    let basestring = format!("v0:{}:{}", timestamp, body);

    // Compute HMAC-SHA256
    let mut mac = HmacSha256::new_from_slice(signing_secret.as_bytes())
        .map_err(|_| anyhow!("Invalid signing secret"))?;
    mac.update(basestring.as_bytes());
    let computed = format!("v0={}", hex::encode(mac.finalize().into_bytes()));

    // Compare signatures
    if computed != *signature {
        return Err(anyhow!("Signature mismatch"));
    }

    Ok(())
}

async fn handle_bluesky_shortcut(payload: ShortcutPayload) -> Result<ApiGatewayProxyResponse> {
    let bot_token = std::env::var("SLACK_BOT_TOKEN")
        .map_err(|_| anyhow!("SLACK_BOT_TOKEN not set"))?;

    // Extract data using helper functions
    let channel_id = extract_channel_id(&payload)?;
    let message_ts = extract_message_ts(&payload)?;
    let message_text = extract_message_text(&payload)?;

    info!(
        "Processing message in channel {} with ts {}",
        channel_id, message_ts
    );

    // Find Bluesky URL in the message
    let bsky_url = extract_bluesky_url(message_text)
        .ok_or_else(|| anyhow!("No Bluesky URL found in message"))?;

    info!("Found Bluesky URL: {}", bsky_url);

    // Recursively fetch all pages to get true total count
    let (thread, total_count) = bsky_thread_lib::fetch_thread_with_total(&bsky_url).await?;

    info!(
        "Fetched complete thread with {} posts by {} (recursive fetch)",
        total_count,
        thread.author.handle
    );

    // Post first batch (starting from index 1, skipping root at 0)
    let client = reqwest::Client::new();
    let batch_end = std::cmp::min(BATCH_SIZE - 1, total_count - 1); // First batch ends at index 9 (displays as [10/TOTAL])

    if batch_end >= 1 {
        post_batch(
            &client,
            &bot_token,
            channel_id,
            message_ts,
            &thread.posts,
            1,         // start_idx (skip root at 0)
            batch_end, // end_idx
            total_count,
        )
        .await?;
    }

    // Post "load more" button if there are remaining posts
    if total_count > BATCH_SIZE {
        // More than BATCH_SIZE total posts means more than first batch (9 posts)
        // Get continuation URI (URI of last post we just displayed)
        let continuation_uri = thread.posts[batch_end].uri.clone();

        post_load_more_button(
            &client,
            &bot_token,
            channel_id,
            message_ts,
            &bsky_url,
            0, // current_batch (just completed batch 0)
            total_count,
            &continuation_uri,
            &thread.author.did,
        )
        .await?;
    }

    // Return success acknowledgment
    Ok(ApiGatewayProxyResponse {
        status_code: 200,
        headers: HashMap::new(),
        body: None, // Empty body acknowledges the shortcut
        is_base64_encoded: false,
    })
}

async fn post_batch(
    client: &reqwest::Client,
    bot_token: &str,
    channel_id: &str,
    thread_ts: &str,
    posts: &[bsky_thread_lib::PostOutput],
    start_idx: usize,
    end_idx: usize,
    total_count: usize,
) -> Result<()> {
    for i in start_idx..=end_idx {
        let post = &posts[i];

        // Add numbered prefix: [2/50] https://bsky.app/...
        let message_text = format!("[{}/{}] {}", i + 1, total_count, post.url);

        let post_request = serde_json::json!({
            "channel": channel_id,
            "thread_ts": thread_ts,
            "text": message_text,
            "unfurl_links": true,
            "unfurl_media": true
        });

        let response = client
            .post("https://slack.com/api/chat.postMessage")
            .header("Authorization", format!("Bearer {}", bot_token))
            .header("Content-Type", "application/json")
            .json(&post_request)
            .send()
            .await?;

        let response_body: Value = response.json().await?;

        if response_body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            warn!(
                "Failed to post message {}: {:?}",
                i + 1,
                response_body.get("error")
            );
        } else {
            info!("Posted message {} of {}", i + 1, total_count);
        }

        // Small delay to avoid rate limiting (only between posts)
        if i < end_idx {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    }

    Ok(())
}

async fn post_batch_with_offset(
    client: &reqwest::Client,
    bot_token: &str,
    channel_id: &str,
    thread_ts: &str,
    posts: &[bsky_thread_lib::PostOutput],
    start_idx: usize,
    end_idx: usize,
    display_start: usize,
    total_count: usize,
) -> Result<()> {
    for (i, post_idx) in (start_idx..=end_idx).enumerate() {
        let post = &posts[post_idx];
        let display_num = display_start + i;

        // Add numbered prefix: [12/50] https://bsky.app/...
        let message_text = format!("[{}/{}] {}", display_num, total_count, post.url);

        let post_request = serde_json::json!({
            "channel": channel_id,
            "thread_ts": thread_ts,
            "text": message_text,
            "unfurl_links": true,
            "unfurl_media": true
        });

        let response = client
            .post("https://slack.com/api/chat.postMessage")
            .header("Authorization", format!("Bearer {}", bot_token))
            .header("Content-Type", "application/json")
            .json(&post_request)
            .send()
            .await?;

        let response_body: Value = response.json().await?;

        if response_body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            warn!(
                "Failed to post message {}: {:?}",
                display_num,
                response_body.get("error")
            );
        } else {
            info!("Posted message {} of {}", display_num, total_count);
        }

        // Small delay to avoid rate limiting (only between posts)
        if post_idx < end_idx {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    }

    Ok(())
}

async fn post_load_more_button(
    client: &reqwest::Client,
    bot_token: &str,
    channel_id: &str,
    thread_ts: &str,
    bsky_url: &str,
    current_batch: usize,
    total_count: usize,
    continuation_uri: &str,
    author_did: &str,
) -> Result<()> {
    // Calculate remaining messages
    let posted_count = (BATCH_SIZE - 1) + current_batch * BATCH_SIZE; // First batch: 9, then +10 per batch
    let remaining = total_count - posted_count - 1; // -1 for root post
    let next_batch_size = std::cmp::min(remaining, BATCH_SIZE);

    // Create button text
    let button_text = if remaining <= BATCH_SIZE {
        format!("unroll the last {} messages", remaining)
    } else {
        format!("unroll the next {} messages", next_batch_size)
    };

    // Encode state in button value
    let state = LoadMoreState {
        bsky_url: bsky_url.to_string(),
        current_batch,
        thread_ts: thread_ts.to_string(),
        channel_id: channel_id.to_string(),
        total_count,
        continuation_uri: continuation_uri.to_string(),
        author_did: author_did.to_string(),
    };
    let state_json = serde_json::to_string(&state)?;

    // Build BlockKit message
    let blocks = serde_json::json!([
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": button_text,
                    },
                    "action_id": LOAD_MORE_ACTION_ID,
                    "value": state_json,
                }
            ]
        }
    ]);

    let post_request = serde_json::json!({
        "channel": channel_id,
        "thread_ts": thread_ts,
        "blocks": blocks,
        "text": button_text, // Fallback text
    });

    let response = client
        .post("https://slack.com/api/chat.postMessage")
        .header("Authorization", format!("Bearer {}", bot_token))
        .header("Content-Type", "application/json")
        .json(&post_request)
        .send()
        .await?;

    let response_body: Value = response.json().await?;

    if response_body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        warn!(
            "Failed to post load more button: {:?}",
            response_body.get("error")
        );
    }

    Ok(())
}

async fn delete_message(
    client: &reqwest::Client,
    bot_token: &str,
    channel_id: &str,
    message_ts: &str,
) -> Result<()> {
    let delete_request = serde_json::json!({
        "channel": channel_id,
        "ts": message_ts,
    });

    let response = client
        .post("https://slack.com/api/chat.delete")
        .header("Authorization", format!("Bearer {}", bot_token))
        .header("Content-Type", "application/json")
        .json(&delete_request)
        .send()
        .await?;

    let response_body: Value = response.json().await?;

    if response_body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        // Log but don't fail - message might already be deleted
        warn!(
            "Failed to delete message (may be already deleted): {:?}",
            response_body.get("error")
        );
    } else {
        info!("Deleted button message");
    }

    Ok(())
}

async fn handle_block_actions(payload: BlockActionsPayload) -> Result<ApiGatewayProxyResponse> {
    let bot_token = std::env::var("SLACK_BOT_TOKEN")
        .map_err(|_| anyhow!("SLACK_BOT_TOKEN not set"))?;

    // Find our action in the actions array
    let action = payload
        .actions
        .iter()
        .find(|a| a.action_id == LOAD_MORE_ACTION_ID)
        .ok_or_else(|| anyhow!("Load more action not found"))?;

    // Deserialize state from button value
    let state: LoadMoreState = serde_json::from_str(&action.value)?;

    info!(
        "Loading batch {} (total: {}) from continuation URI",
        state.current_batch + 1,
        state.total_count
    );

    // Extract message timestamp to delete the button message
    let button_message_ts = payload
        .message
        .as_ref()
        .and_then(|m| m.get("ts"))
        .and_then(|ts| ts.as_str())
        .ok_or_else(|| anyhow!("Could not find button message timestamp"))?;

    // Delete the button message
    let client = reqwest::Client::new();
    delete_message(&client, &bot_token, &state.channel_id, button_message_ts).await?;

    // Fetch ONLY the next page starting from continuation URI
    let posts = bsky_thread_lib::fetch_thread_from_uri(
        &state.continuation_uri,
        &state.author_did
    ).await?;

    // Calculate batch range
    let next_batch = state.current_batch + 1;

    // We need to map logical indices to posts in the fetched page
    // The fetched page starts at continuation_uri (which we already displayed)
    // So we skip the first post and take the next BATCH_SIZE posts
    let page_start = 1; // Skip the continuation point (duplicate)
    let page_end = std::cmp::min(page_start + BATCH_SIZE - 1, posts.len() - 1);

    // Calculate logical indices for [N/TOTAL] display
    // First batch posts 9 (displayed as [2/31] to [10/31])
    // Each subsequent batch posts 10
    let logical_start = next_batch * BATCH_SIZE + 1;

    // Post the batch with correct numbering
    post_batch_with_offset(
        &client,
        &bot_token,
        &state.channel_id,
        &state.thread_ts,
        &posts,
        page_start,
        page_end,
        logical_start,
        state.total_count,
    )
    .await?;

    // Check if more posts remain
    let logical_end = logical_start + (page_end - page_start);
    if logical_end < state.total_count {
        // Get new continuation URI (last post we just displayed)
        let new_continuation_uri = posts[page_end].uri.clone();

        post_load_more_button(
            &client,
            &bot_token,
            &state.channel_id,
            &state.thread_ts,
            &state.bsky_url,
            next_batch,
            state.total_count,
            &new_continuation_uri,
            &state.author_did,
        )
        .await?;
    }

    // Return success (no response needed for button clicks)
    Ok(ApiGatewayProxyResponse {
        status_code: 200,
        headers: HashMap::new(),
        body: None,
        is_base64_encoded: false,
    })
}

fn extract_channel_id(payload: &ShortcutPayload) -> Result<&str> {
    payload
        .channel
        .as_ref()
        .and_then(|c| {
            c.get("id")
                .and_then(|id| id.as_str())
                .or_else(|| c.as_str())
        })
        .ok_or_else(|| anyhow!("Could not find channel ID"))
}

fn extract_message_ts(payload: &ShortcutPayload) -> Result<&str> {
    payload
        .message
        .as_ref()
        .and_then(|m| m.get("ts"))
        .and_then(|ts| ts.as_str())
        .ok_or_else(|| anyhow!("Could not find message timestamp"))
}

fn extract_message_text(payload: &ShortcutPayload) -> Result<&str> {
    payload
        .message
        .as_ref()
        .and_then(|m| m.get("text"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| anyhow!("Could not find message text"))
}

fn extract_bluesky_url(text: &str) -> Option<String> {
    // Match Bluesky URLs like https://bsky.app/profile/user/post/xyz
    let re = Regex::new(r"https://bsky\.app/profile/[^/\s]+/post/[a-zA-Z0-9]+").ok()?;

    re.find(text).map(|m| m.as_str().to_string())
}

fn parse_form_data(body: &str) -> Result<HashMap<String, String>> {
    let mut form_data = HashMap::new();

    for pair in body.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            let decoded_key =
                decode(key).map_err(|_| anyhow!("Failed to decode form key"))?;
            let decoded_value =
                decode(value).map_err(|_| anyhow!("Failed to decode form value"))?;
            form_data.insert(decoded_key.to_string(), decoded_value.to_string());
        }
    }

    Ok(form_data)
}

#[derive(Debug, Deserialize)]
struct ShortcutPayload {
    #[serde(rename = "type")]
    payload_type: Option<String>,
    callback_id: Option<String>,
    trigger_id: String,
    user: Value,
    channel: Option<Value>,
    message: Option<Value>,
    response_url: Option<String>,
    token: Option<String>,
    team: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct BlockActionsPayload {
    #[serde(rename = "type")]
    payload_type: String,
    actions: Vec<Action>,
    channel: Option<Value>,
    message: Option<Value>,
    user: Value,
}

#[derive(Debug, Deserialize)]
struct Action {
    action_id: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoadMoreState {
    bsky_url: String,
    current_batch: usize,
    thread_ts: String,
    channel_id: String,
    total_count: usize,        // True total from recursive fetch
    continuation_uri: String,  // URI to fetch next page from
    author_did: String,        // Needed for fetch_thread_from_uri
}

#[derive(Debug, Deserialize)]
struct ApiGatewayProxyRequest {
    #[serde(rename = "httpMethod")]
    http_method: Option<String>,
    path: Option<String>,
    #[serde(rename = "queryStringParameters")]
    query_string_parameters: Option<HashMap<String, String>>,
    headers: Option<HashMap<String, String>>,
    body: Option<String>,
    #[serde(rename = "isBase64Encoded")]
    is_base64_encoded: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ApiGatewayProxyResponse {
    #[serde(rename = "statusCode")]
    status_code: i32,
    headers: HashMap<String, String>,
    body: Option<String>,
    #[serde(rename = "isBase64Encoded")]
    is_base64_encoded: bool,
}
