use anyhow::{anyhow, Result};
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvocationType;
use base64::Engine;
use hmac::{Hmac, Mac};
use lambda_runtime::{service_fn, Error as LambdaError, LambdaEvent};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::OnceLock;
use tracing::{error, info, warn};
use urlencoding::decode;

const SHORTCUT_CALLBACK_ID: &str = "unroll_bluesky_thread";
const VIDEO_SHORTCUT_CALLBACK_ID: &str = "import_video";
const BATCH_SIZE: usize = 10;
const LOAD_MORE_ACTION_ID: &str = "load_more_thread";

// Global Lambda client (initialized once)
static LAMBDA_CLIENT: OnceLock<aws_sdk_lambda::Client> = OnceLock::new();

/// Background task types for async Lambda self-invocation
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "background_task_type")]
enum BackgroundTask {
    #[serde(rename = "thread_unroll")]
    ThreadUnroll {
        bot_token: String,
        channel_id: String,
        message_ts: String,
        message_text: String,
        response_url: String,
    },
    #[serde(rename = "video_import")]
    VideoImport {
        bot_token: String,
        channel_id: String,
        message_ts: String,
        message_text: String,
        response_url: String,
    },
    #[serde(rename = "load_more")]
    LoadMore {
        bot_token: String,
        state: LoadMoreState,
        button_message_ts: String,
        response_url: String,
    },
}

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

    // Initialize AWS SDK Lambda client
    let config = aws_config::load_from_env().await;
    let lambda_client = aws_sdk_lambda::Client::new(&config);
    LAMBDA_CLIENT.set(lambda_client).ok();

    lambda_runtime::run(service_fn(handle_request)).await
}

async fn handle_request(
    event: LambdaEvent<Value>,
) -> std::result::Result<ApiGatewayProxyResponse, LambdaError> {
    let (request, _context) = event.into_parts();

    // Check if this is a background task invocation (async self-invocation)
    if let Ok(task) = serde_json::from_value::<BackgroundTask>(request.clone()) {
        info!("Executing background task");
        execute_background_task(task).await;
        return Ok(ApiGatewayProxyResponse {
            status_code: 200,
            headers: HashMap::new(),
            body: None,
            is_base64_encoded: false,
        });
    }

    // Otherwise, parse as API Gateway request from Slack
    let api_request: ApiGatewayProxyRequest = serde_json::from_value(request)
        .map_err(|e| LambdaError::from(format!("Failed to parse request: {}", e)))?;

    match process_request(api_request).await {
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

/// Execute a background task (called from async self-invocation)
async fn execute_background_task(task: BackgroundTask) {
    match task {
        BackgroundTask::ThreadUnroll {
            bot_token,
            channel_id,
            message_ts,
            message_text,
            response_url,
        } => {
            process_thread_unroll(bot_token, channel_id, message_ts, message_text, response_url)
                .await;
        }
        BackgroundTask::VideoImport {
            bot_token,
            channel_id,
            message_ts,
            message_text,
            response_url,
        } => {
            process_video_import(bot_token, channel_id, message_ts, message_text, response_url)
                .await;
        }
        BackgroundTask::LoadMore {
            bot_token,
            state,
            button_message_ts,
            response_url,
        } => {
            process_load_more(bot_token, state, button_message_ts, response_url).await;
        }
    }
}

/// Invoke this Lambda asynchronously to process a background task
async fn invoke_background_task(task: BackgroundTask) -> Result<()> {
    let function_name = std::env::var("AWS_LAMBDA_FUNCTION_NAME")
        .map_err(|_| anyhow!("AWS_LAMBDA_FUNCTION_NAME not set"))?;

    let client = LAMBDA_CLIENT
        .get()
        .ok_or_else(|| anyhow!("Lambda client not initialized"))?;

    let payload = serde_json::to_vec(&task)?;

    info!("Invoking Lambda {} asynchronously for background task", function_name);

    client
        .invoke()
        .function_name(&function_name)
        .invocation_type(InvocationType::Event) // Async invocation
        .payload(Blob::new(payload))
        .send()
        .await
        .map_err(|e| anyhow!("Failed to invoke Lambda: {}", e))?;

    Ok(())
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
            if payload.callback_id.as_deref() == Some(VIDEO_SHORTCUT_CALLBACK_ID) {
                info!("Received import_video shortcut");
                return handle_video_import(payload).await;
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
    // Extract everything needed BEFORE invoking background task
    let bot_token = std::env::var("SLACK_BOT_TOKEN")
        .map_err(|_| anyhow!("SLACK_BOT_TOKEN not set"))?;

    let channel_id = extract_channel_id(&payload)?.to_string();
    let message_ts = extract_message_ts(&payload)?.to_string();
    let message_text = extract_message_text(&payload)?.to_string();
    let response_url = payload
        .response_url
        .clone()
        .ok_or_else(|| anyhow!("Could not extract response_url from payload"))?;

    info!(
        "Received unroll_bluesky_thread shortcut for channel {} with ts {}",
        channel_id, message_ts
    );

    // Invoke Lambda asynchronously to do the actual work
    invoke_background_task(BackgroundTask::ThreadUnroll {
        bot_token,
        channel_id,
        message_ts,
        message_text,
        response_url,
    })
    .await?;

    // Return 200 OK immediately to acknowledge the shortcut
    Ok(ApiGatewayProxyResponse {
        status_code: 200,
        headers: HashMap::new(),
        body: None,
        is_base64_encoded: false,
    })
}

async fn handle_video_import(payload: ShortcutPayload) -> Result<ApiGatewayProxyResponse> {
    // Extract everything needed BEFORE invoking background task
    let bot_token = std::env::var("SLACK_BOT_TOKEN")
        .map_err(|_| anyhow!("SLACK_BOT_TOKEN not set"))?;

    let channel_id = extract_channel_id(&payload)?.to_string();
    let message_ts = extract_message_ts(&payload)?.to_string();
    let message_text = extract_message_text(&payload)?.to_string();
    let response_url = payload
        .response_url
        .clone()
        .ok_or_else(|| anyhow!("Could not extract response_url from payload"))?;

    info!(
        "Received import_video shortcut for channel {} with ts {}",
        channel_id, message_ts
    );

    // Invoke Lambda asynchronously to do the actual work
    invoke_background_task(BackgroundTask::VideoImport {
        bot_token,
        channel_id,
        message_ts,
        message_text,
        response_url,
    })
    .await?;

    // Return 200 OK immediately to acknowledge the shortcut
    Ok(ApiGatewayProxyResponse {
        status_code: 200,
        headers: HashMap::new(),
        body: None,
        is_base64_encoded: false,
    })
}

async fn upload_file_to_slack(
    client: &reqwest::Client,
    bot_token: &str,
    channel_id: &str,
    thread_ts: &str,
    filename: &str,
    content: Vec<u8>,
) -> Result<()> {
    // Use Slack's newer upload flow:
    // 1. Get upload URL from files.getUploadURLExternal
    // 2. Upload file to that URL
    // 3. Complete upload with files.completeUploadExternal

    let content_len = content.len();

    // Step 1: Get upload URL
    let get_url_response = client
        .get("https://slack.com/api/files.getUploadURLExternal")
        .header("Authorization", format!("Bearer {}", bot_token))
        .query(&[
            ("filename", filename),
            ("length", &content_len.to_string()),
        ])
        .send()
        .await?;

    let get_url_body: Value = get_url_response.json().await?;

    if get_url_body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        let error = get_url_body
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");
        let needed = get_url_body
            .get("needed")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !needed.is_empty() {
            return Err(anyhow!("Slack API error: {} (needed scope: {})", error, needed));
        }
        return Err(anyhow!("Slack API error: {}", error));
    }

    let upload_url = get_url_body
        .get("upload_url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("No upload_url in response"))?;

    let file_id = get_url_body
        .get("file_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("No file_id in response"))?;

    info!("Got upload URL for file_id: {}", file_id);

    // Step 2: Upload the file to the external URL
    let upload_response = client
        .post(upload_url)
        .header("Content-Type", "video/mp4")
        .body(content)
        .send()
        .await?;

    if !upload_response.status().is_success() {
        return Err(anyhow!(
            "Failed to upload file: HTTP {}",
            upload_response.status()
        ));
    }

    info!("Uploaded file to external URL");

    // Step 3: Complete the upload and share to channel
    let complete_request = serde_json::json!({
        "files": [{
            "id": file_id,
            "title": filename,
        }],
        "channel_id": channel_id,
        "thread_ts": thread_ts,
    });

    let complete_response = client
        .post("https://slack.com/api/files.completeUploadExternal")
        .header("Authorization", format!("Bearer {}", bot_token))
        .header("Content-Type", "application/json")
        .json(&complete_request)
        .send()
        .await?;

    let complete_body: Value = complete_response.json().await?;

    if complete_body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        let error = complete_body
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");

        if error == "not_in_channel" || error == "channel_not_found" {
            return Err(anyhow!("not_in_channel: Bot is not a member of channel {}", channel_id));
        }

        let needed = complete_body
            .get("needed")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !needed.is_empty() {
            return Err(anyhow!("Slack API error: {} (needed scope: {})", error, needed));
        }
        return Err(anyhow!("Slack API error: {}", error));
    }

    info!("Completed file upload to channel");
    Ok(())
}

async fn send_ephemeral_error(
    client: &reqwest::Client,
    response_url: &str,
    error_message: &str,
) -> Result<()> {
    let message = serde_json::json!({
        "response_type": "ephemeral",
        "text": error_message,
    });

    let response = client
        .post(response_url)
        .header("Content-Type", "application/json")
        .json(&message)
        .send()
        .await?;

    if !response.status().is_success() {
        warn!(
            "Failed to send error message via response_url: status {}",
            response.status()
        );
    }

    Ok(())
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
            let error = response_body.get("error").and_then(|v| v.as_str());

            // Check if the error is not_in_channel or channel_not_found (private channels)
            if error == Some("not_in_channel") || error == Some("channel_not_found") {
                warn!("Bot is not in channel {} (error: {:?}), stopping batch", channel_id, error);
                return Err(anyhow!("not_in_channel: Bot is not a member of channel {}", channel_id));
            }

            // For other errors, log and continue
            warn!(
                "Failed to post message {}: {:?}",
                i + 1,
                error
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
            let error = response_body.get("error").and_then(|v| v.as_str());

            // Check if the error is not_in_channel or channel_not_found (private channels)
            if error == Some("not_in_channel") || error == Some("channel_not_found") {
                warn!("Bot is not in channel {} (error: {:?}), stopping batch", channel_id, error);
                return Err(anyhow!("not_in_channel: Bot is not a member of channel {}", channel_id));
            }

            // For other errors, log and continue
            warn!(
                "Failed to post message {}: {:?}",
                display_num,
                error
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

async fn send_not_in_channel_message(
    client: &reqwest::Client,
    response_url: &str,
) -> Result<()> {
    let message = serde_json::json!({
        "response_type": "ephemeral",
        "text": "I'm not a member of this channel yet! Please invite me first by typing `/invite @Bluesky Thread Unroller` in the channel.",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":warning: *I'm not a member of this channel yet!*\n\nPlease invite me to this channel by typing:\n`/invite @Bluesky Thread Unroller`\n\nThen try the unroll action again."
                }
            }
        ]
    });

    let response = client
        .post(response_url)
        .header("Content-Type", "application/json")
        .json(&message)
        .send()
        .await?;

    if !response.status().is_success() {
        warn!(
            "Failed to send response_url message: status {}",
            response.status()
        );
    } else {
        info!("Sent not_in_channel message via response_url");
    }

    Ok(())
}

async fn send_processing_message(
    client: &reqwest::Client,
    response_url: &str,
    message: &str,
) -> Result<()> {
    let payload = serde_json::json!({
        "response_type": "ephemeral",
        "text": message,
    });

    let response = client
        .post(response_url)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?;

    if !response.status().is_success() {
        warn!(
            "Failed to send processing message via response_url: status {}",
            response.status()
        );
    }

    Ok(())
}

/// Background task to process thread unrolling
async fn process_thread_unroll(
    bot_token: String,
    channel_id: String,
    message_ts: String,
    message_text: String,
    response_url: String,
) {
    let client = reqwest::Client::new();

    // Send initial processing message
    if let Err(e) = send_processing_message(&client, &response_url, "Fetching Bluesky thread...").await {
        warn!("Failed to send processing message: {}", e);
    }

    // Find Bluesky URL in the message
    let bsky_url = match extract_bluesky_url(&message_text) {
        Some(url) => url,
        None => {
            let _ = send_ephemeral_error(&client, &response_url, "No Bluesky URL found in message").await;
            return;
        }
    };

    info!("Found Bluesky URL: {}", bsky_url);

    // Recursively fetch all pages to get true total count
    let (thread, total_count) = match bsky_thread_lib::fetch_thread_with_total(&bsky_url).await {
        Ok(result) => result,
        Err(e) => {
            error!("Failed to fetch thread: {}", e);
            let _ = send_ephemeral_error(&client, &response_url, &format!("Failed to fetch thread: {}", e)).await;
            return;
        }
    };

    info!(
        "Fetched complete thread with {} posts by {} (recursive fetch)",
        total_count,
        thread.author.handle
    );

    // Post first batch (starting from index 1, skipping root at 0)
    let batch_end = std::cmp::min(BATCH_SIZE - 1, total_count - 1);

    if batch_end >= 1 {
        let result = post_batch(
            &client,
            &bot_token,
            &channel_id,
            &message_ts,
            &thread.posts,
            1,         // start_idx (skip root at 0)
            batch_end, // end_idx
            total_count,
        )
        .await;

        // Check for not_in_channel error
        if let Err(e) = result {
            let error_msg = e.to_string();
            if error_msg.contains("not_in_channel") {
                info!("Bot not in channel {}, sending message via response_url", channel_id);
                let _ = send_not_in_channel_message(&client, &response_url).await;
                return;
            }
            error!("Failed to post batch: {}", e);
            let _ = send_ephemeral_error(&client, &response_url, &format!("Failed to post messages: {}", e)).await;
            return;
        }
    }

    // Post "load more" button if there are remaining posts
    if total_count > BATCH_SIZE {
        let continuation_uri = thread.posts[batch_end].uri.clone();

        if let Err(e) = post_load_more_button(
            &client,
            &bot_token,
            &channel_id,
            &message_ts,
            &bsky_url,
            0, // current_batch (just completed batch 0)
            total_count,
            &continuation_uri,
            &thread.author.did,
        )
        .await
        {
            warn!("Failed to post load more button: {}", e);
        }
    }

    info!("Thread unroll completed successfully");
}

/// Background task to process video import
async fn process_video_import(
    bot_token: String,
    channel_id: String,
    message_ts: String,
    message_text: String,
    response_url: String,
) {
    let client = reqwest::Client::new();

    // Send initial processing message
    if let Err(e) = send_processing_message(&client, &response_url, "Downloading video from Bluesky...").await {
        warn!("Failed to send processing message: {}", e);
    }

    // Find Bluesky URL in the message
    let bsky_url = match extract_bluesky_url(&message_text) {
        Some(url) => url,
        None => {
            let _ = send_ephemeral_error(&client, &response_url, "No Bluesky URL found in message").await;
            return;
        }
    };

    info!("Found Bluesky URL: {}", bsky_url);

    // Fetch video info from Bluesky
    let video_info = match bsky_video_lib::fetch_video_info(&client, &bsky_url).await {
        Ok(info) => info,
        Err(e) => {
            warn!("Failed to fetch video info: {}", e);
            let _ = send_ephemeral_error(&client, &response_url, &format!("Could not find video in post: {}", e)).await;
            return;
        }
    };

    info!("Found video, downloading from playlist: {}", video_info.playlist_url);

    // Download video to memory
    let ts_data = match bsky_video_lib::download_video_to_memory(&client, &video_info.playlist_url).await {
        Ok(data) => data,
        Err(e) => {
            warn!("Failed to download video: {}", e);
            let _ = send_ephemeral_error(&client, &response_url, &format!("Failed to download video: {}", e)).await;
            return;
        }
    };

    info!("Downloaded {} bytes of TS data", ts_data.len());

    // Convert to MP4
    let mp4_data = match bsky_video_lib::convert_to_mp4(&ts_data) {
        Ok(data) => data,
        Err(e) => {
            warn!("Failed to convert video to MP4: {}", e);
            let _ = send_ephemeral_error(&client, &response_url, &format!("Failed to convert video: {}", e)).await;
            return;
        }
    };

    info!("Converted to {} bytes of MP4 data", mp4_data.len());

    // Upload to Slack
    let filename = format!("{}.mp4", video_info.post_id);
    let result = upload_file_to_slack(
        &client,
        &bot_token,
        &channel_id,
        &message_ts,
        &filename,
        mp4_data,
    )
    .await;

    // Check for not_in_channel error
    if let Err(e) = result {
        let error_msg = e.to_string();
        if error_msg.contains("not_in_channel") || error_msg.contains("channel_not_found") {
            info!("Bot not in channel {}, sending message via response_url", channel_id);
            let _ = send_not_in_channel_message(&client, &response_url).await;
        } else {
            warn!("Failed to upload video: {}", e);
            let _ = send_ephemeral_error(&client, &response_url, &format!("Failed to upload video: {}", e)).await;
        }
    } else {
        info!("Successfully uploaded video to Slack");
    }
}

/// Background task to process "load more" button clicks
async fn process_load_more(
    bot_token: String,
    state: LoadMoreState,
    button_message_ts: String,
    response_url: String,
) {
    let client = reqwest::Client::new();

    info!(
        "Loading batch {} (total: {}) from continuation URI",
        state.current_batch + 1,
        state.total_count
    );

    // Delete the button message
    if let Err(e) = delete_message(&client, &bot_token, &state.channel_id, &button_message_ts).await {
        warn!("Failed to delete button message: {}", e);
    }

    // Fetch ONLY the next page starting from continuation URI
    let posts = match bsky_thread_lib::fetch_thread_from_uri(
        &state.continuation_uri,
        &state.author_did
    ).await {
        Ok(posts) => posts,
        Err(e) => {
            error!("Failed to fetch thread continuation: {}", e);
            let _ = send_ephemeral_error(&client, &response_url, &format!("Failed to load more posts: {}", e)).await;
            return;
        }
    };

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
    let result = post_batch_with_offset(
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
    .await;

    // Check for not_in_channel error
    if let Err(e) = result {
        let error_msg = e.to_string();
        if error_msg.contains("not_in_channel") {
            info!("Bot not in channel {}, sending message via response_url", state.channel_id);
            let _ = send_not_in_channel_message(&client, &response_url).await;
            return;
        }
        error!("Failed to post batch: {}", e);
        let _ = send_ephemeral_error(&client, &response_url, &format!("Failed to load more posts: {}", e)).await;
        return;
    }

    // Check if more posts remain
    let logical_end = logical_start + (page_end - page_start);
    if logical_end < state.total_count {
        // Get new continuation URI (last post we just displayed)
        let new_continuation_uri = posts[page_end].uri.clone();

        if let Err(e) = post_load_more_button(
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
        .await
        {
            warn!("Failed to post load more button: {}", e);
        }
    }

    info!("Load more batch completed successfully");
}

async fn handle_block_actions(payload: BlockActionsPayload) -> Result<ApiGatewayProxyResponse> {
    // Extract everything needed BEFORE invoking background task
    let bot_token = std::env::var("SLACK_BOT_TOKEN")
        .map_err(|_| anyhow!("SLACK_BOT_TOKEN not set"))?;

    let response_url = payload
        .response_url
        .clone()
        .ok_or_else(|| anyhow!("Could not extract response_url from payload"))?;

    // Find our action in the actions array
    let action = payload
        .actions
        .iter()
        .find(|a| a.action_id == LOAD_MORE_ACTION_ID)
        .ok_or_else(|| anyhow!("Load more action not found"))?;

    // Deserialize state from button value
    let state: LoadMoreState = serde_json::from_str(&action.value)?;

    // Extract message timestamp to delete the button message
    let button_message_ts = payload
        .message
        .as_ref()
        .and_then(|m| m.get("ts"))
        .and_then(|ts| ts.as_str())
        .ok_or_else(|| anyhow!("Could not find button message timestamp"))?
        .to_string();

    info!(
        "Received load_more button click for batch {} (total: {})",
        state.current_batch + 1,
        state.total_count
    );

    // Invoke Lambda asynchronously to do the actual work
    invoke_background_task(BackgroundTask::LoadMore {
        bot_token,
        state,
        button_message_ts,
        response_url,
    })
    .await?;

    // Return 200 OK immediately to acknowledge the button click
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
    response_url: Option<String>,
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

// ========================================
// Batch Calculation Helper Functions
// ========================================
// These functions extract the batch calculation logic
// to make it testable and prevent regression of bugs.

/// Calculate the number of posts posted after completing the given batch.
/// First batch (0) posts 9 messages, subsequent batches post 10 each.
fn calculate_posted_count(current_batch: usize, batch_size: usize) -> usize {
    (batch_size - 1) + current_batch * batch_size
}

/// Calculate the logical start index for [N/TOTAL] display numbering.
/// This is the display number (not array index) for the first message in a batch.
fn calculate_logical_start(next_batch: usize, batch_size: usize) -> usize {
    next_batch * batch_size + 1
}

/// Calculate the batch end index for the first batch.
/// First batch should end at BATCH_SIZE - 1 to post only 9 messages.
fn calculate_batch_end(batch_size: usize, total_count: usize) -> usize {
    std::cmp::min(batch_size - 1, total_count - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================
    // Batch Calculation Tests - 31-Post Thread
    // ========================================
    // These tests verify the batch numbering logic for the 31-post thread
    // that exposed bugs in the original implementation.

    const TEST_BATCH_SIZE: usize = 10;
    const TEST_TOTAL_COUNT: usize = 31;

    #[test]
    fn test_batch_0_calculations() {
        // Batch 0: Display [2/31] to [10/31] (9 messages)
        let current_batch = 0;
        let batch_end = calculate_batch_end(TEST_BATCH_SIZE, TEST_TOTAL_COUNT);
        let posted_count = calculate_posted_count(current_batch, TEST_BATCH_SIZE);
        let remaining = TEST_TOTAL_COUNT - posted_count - 1; // -1 for root

        assert_eq!(batch_end, 9, "First batch should end at index 9");
        assert_eq!(posted_count, 9, "First batch should post 9 messages");
        assert_eq!(remaining, 21, "Should have 21 messages remaining");

        // Button should show "unroll the next 10 messages"
        let next_batch_size = std::cmp::min(remaining, TEST_BATCH_SIZE);
        assert_eq!(next_batch_size, 10);
    }

    #[test]
    fn test_batch_1_calculations() {
        // Batch 1: Display [11/31] to [20/31] (10 messages)
        // state.current_batch = 0 (just finished batch 0), next_batch = 1
        let current_batch = 0; // Just finished batch 0
        let next_batch = current_batch + 1; // About to post batch 1
        let posted_count = calculate_posted_count(current_batch, TEST_BATCH_SIZE);
        let logical_start = calculate_logical_start(next_batch, TEST_BATCH_SIZE);
        let remaining = TEST_TOTAL_COUNT - posted_count - 1;

        assert_eq!(posted_count, 9, "After batch 0, should have posted 9 messages");
        assert_eq!(logical_start, 11, "Batch 1 should start at [11/31]");
        assert_eq!(remaining, 21, "Should have 21 messages remaining");

        // Logical end should be 20
        let logical_end = logical_start + (TEST_BATCH_SIZE - 1);
        assert_eq!(logical_end, 20, "Batch 1 should end at [20/31]");
    }

    #[test]
    fn test_batch_2_calculations() {
        // Batch 2: Display [21/31] to [30/31] (10 messages)
        // state.current_batch = 1 (just finished batch 1), next_batch = 2
        let current_batch = 1; // Just finished batch 1
        let next_batch = current_batch + 1; // About to post batch 2
        let posted_count = calculate_posted_count(current_batch, TEST_BATCH_SIZE);
        let logical_start = calculate_logical_start(next_batch, TEST_BATCH_SIZE);
        let remaining = TEST_TOTAL_COUNT - posted_count - 1;

        assert_eq!(posted_count, 19, "After batch 1, should have posted 19 messages (9+10)");
        assert_eq!(logical_start, 21, "Batch 2 should start at [21/31]");
        assert_eq!(remaining, 11, "Should have 11 messages remaining");

        // Logical end should be 30
        let logical_end = logical_start + (TEST_BATCH_SIZE - 1);
        assert_eq!(logical_end, 30, "Batch 2 should end at [30/31]");
    }

    #[test]
    fn test_batch_3_calculations() {
        // Batch 3: Display [31/31] (1 message, NO button after)
        // state.current_batch = 2 (just finished batch 2), next_batch = 3
        let current_batch = 2; // Just finished batch 2
        let next_batch = current_batch + 1; // About to post batch 3
        let posted_count = calculate_posted_count(current_batch, TEST_BATCH_SIZE);
        let logical_start = calculate_logical_start(next_batch, TEST_BATCH_SIZE);
        let remaining = TEST_TOTAL_COUNT - posted_count - 1;

        assert_eq!(posted_count, 29, "After batch 2, should have posted 29 messages (9+10+10)");
        assert_eq!(logical_start, 31, "Batch 3 should start at [31/31]");
        assert_eq!(remaining, 1, "Should have 1 message remaining");

        // logical_end should equal logical_start (only 1 message)
        // which equals TEST_TOTAL_COUNT, so no button should appear after
        assert_eq!(logical_start, TEST_TOTAL_COUNT, "Last message is [31/31]");
    }

    // ========================================
    // Edge Case Tests
    // ========================================

    #[test]
    fn test_10_post_thread() {
        // 10-post thread: Only batch 0, remaining=0, no button
        let total = 10;
        let batch_end = calculate_batch_end(TEST_BATCH_SIZE, total);
        let posted_count = calculate_posted_count(0, TEST_BATCH_SIZE);
        let remaining = total - posted_count - 1;

        assert_eq!(batch_end, 9, "Should end at index 9");
        assert_eq!(posted_count, 9, "Should post 9 messages");
        assert_eq!(remaining, 0, "No messages remaining, no button");
    }

    #[test]
    fn test_11_post_thread() {
        // 11-post thread: Batch 0 + 1 more post in batch 1
        let total = 11;

        // Batch 0
        let batch_end_0 = calculate_batch_end(TEST_BATCH_SIZE, total);
        let posted_count_0 = calculate_posted_count(0, TEST_BATCH_SIZE);
        let remaining_0 = total - posted_count_0 - 1;

        assert_eq!(batch_end_0, 9);
        assert_eq!(posted_count_0, 9);
        assert_eq!(remaining_0, 1, "Should have 1 message remaining");

        // Batch 1
        let logical_start_1 = calculate_logical_start(1, TEST_BATCH_SIZE);
        assert_eq!(logical_start_1, 11, "Second batch starts at [11/11]");
    }

    #[test]
    fn test_20_post_thread() {
        // 20-post thread: Batch 0 (9) + Batch 1 (10), remaining=0
        let total = 20;

        // After batch 1
        let posted_count_1 = calculate_posted_count(1, TEST_BATCH_SIZE);
        let remaining_1 = total - posted_count_1 - 1;

        assert_eq!(posted_count_1, 19);
        assert_eq!(remaining_1, 0, "No messages remaining after batch 1");
    }

    #[test]
    fn test_21_post_thread() {
        // 21-post thread: Batch 0 (9) + Batch 1 (10) + Batch 2 (1)
        let total = 21;

        // After batch 1
        let posted_count_1 = calculate_posted_count(1, TEST_BATCH_SIZE);
        let remaining_1 = total - posted_count_1 - 1;

        assert_eq!(remaining_1, 1, "Should have 1 message remaining");

        // Batch 2
        let logical_start_2 = calculate_logical_start(2, TEST_BATCH_SIZE);
        assert_eq!(logical_start_2, 21, "Third batch starts at [21/21]");
    }

    // ========================================
    // URL Extraction Tests
    // ========================================

    #[test]
    fn test_extract_bluesky_url_single() {
        let text = "Check out this thread https://bsky.app/profile/user.bsky.social/post/abc123";
        let url = extract_bluesky_url(text);
        assert!(url.is_some());
        assert_eq!(url.unwrap(), "https://bsky.app/profile/user.bsky.social/post/abc123");
    }

    #[test]
    fn test_extract_bluesky_url_multiple() {
        let text = "https://bsky.app/profile/user1.bsky.social/post/abc https://bsky.app/profile/user2.bsky.social/post/xyz";
        let url = extract_bluesky_url(text);
        assert!(url.is_some());
        // Should extract first URL
        assert_eq!(url.unwrap(), "https://bsky.app/profile/user1.bsky.social/post/abc");
    }

    #[test]
    fn test_extract_bluesky_url_with_text() {
        let text = "Hey check this out: https://bsky.app/profile/cool.user/post/12345 - really cool!";
        let url = extract_bluesky_url(text);
        assert!(url.is_some());
        assert_eq!(url.unwrap(), "https://bsky.app/profile/cool.user/post/12345");
    }

    #[test]
    fn test_extract_bluesky_url_none() {
        let text = "No URL here just some text";
        let url = extract_bluesky_url(text);
        assert!(url.is_none());
    }

    #[test]
    fn test_extract_bluesky_url_malformed() {
        let text = "https://bsky.app/not-a-valid-url";
        let url = extract_bluesky_url(text);
        assert!(url.is_none());
    }
}
