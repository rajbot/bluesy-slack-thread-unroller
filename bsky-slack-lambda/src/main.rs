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
            let payload: ShortcutPayload = serde_json::from_str(payload_str)?;

            // Check if this is our shortcut
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

    // Debug log the payload
    info!("Channel payload: {:?}", payload.channel);

    // Extract message text
    let message_text = payload
        .message
        .as_ref()
        .and_then(|m| m.get("text"))
        .and_then(|t| t.as_str())
        .unwrap_or("");

    // Extract channel ID - try both "id" field and direct string
    let channel_id = payload
        .channel
        .as_ref()
        .and_then(|c| {
            // Try as object with "id" field first
            c.get("id")
                .and_then(|id| id.as_str())
                // Fall back to direct string value
                .or_else(|| c.as_str())
        })
        .ok_or_else(|| anyhow!("Could not find channel ID"))?;

    // Extract message timestamp for threading
    let message_ts = payload
        .message
        .as_ref()
        .and_then(|m| m.get("ts"))
        .and_then(|ts| ts.as_str())
        .ok_or_else(|| anyhow!("Could not find message timestamp"))?;

    info!(
        "Processing message in channel {} with ts {}",
        channel_id, message_ts
    );

    // Find Bluesky URL in the message
    let bsky_url = extract_bluesky_url(message_text)
        .ok_or_else(|| anyhow!("No Bluesky URL found in message"))?;

    info!("Found Bluesky URL: {}", bsky_url);

    // Fetch the thread using the library
    let thread = bsky_thread_lib::fetch_thread(&bsky_url).await?;

    info!(
        "Fetched thread with {} posts by {}",
        thread.posts.len(),
        thread.author.handle
    );

    // Post each thread post as a reply in the Slack thread
    // Skip the first post (index 0) since it's already in the original message
    let client = reqwest::Client::new();

    for (i, post) in thread.posts.iter().enumerate().skip(1) {
        let message_text = &post.url;

        let post_request = serde_json::json!({
            "channel": channel_id,
            "thread_ts": message_ts,
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
                i,
                response_body.get("error")
            );
        } else {
            info!("Posted message {} of {}", i + 1, thread.posts.len());
        }

        // Small delay to avoid rate limiting
        if i < thread.posts.len() - 1 {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    }

    // Return success acknowledgment
    Ok(ApiGatewayProxyResponse {
        status_code: 200,
        headers: HashMap::new(),
        body: None, // Empty body acknowledges the shortcut
        is_base64_encoded: false,
    })
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
