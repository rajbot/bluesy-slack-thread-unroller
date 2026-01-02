# Bluesky Thread Unroller

A Rust toolkit for unrolling Bluesky threads. Includes a CLI tool and a Slack app that posts thread replies to a Slack thread.

![Bluesky Thread Unroller Demo](bluesky-thread-unroller.gif)

## Project Structure

```
bsky-thread-unroller/
├── bsky-thread-lib/        # Shared library for fetching Bluesky threads
├── bsky-thread-cli/        # CLI tool
└── bsky-slack-lambda/      # Slack app (AWS Lambda)
```

## CLI Tool

### Build

```bash
cargo build --release
```

### Usage

```bash
./target/release/bsky-thread 'https://bsky.app/profile/simonwillison.net/post/3m7gzjew3ss2e'
```

Outputs JSON with the thread author's posts only (filters out other replies).

---

## Slack App Setup

The Slack app uses a message shortcut called "Unroll Bluesky Thread". When triggered on a message containing a Bluesky URL, it fetches the thread and posts each post as a reply in a Slack thread.

### Prerequisites

- AWS account with permissions to create Lambda functions and API Gateway
- [cargo-lambda](https://www.cargo-lambda.info/) installed:
  ```bash
  brew tap cargo-lambda/cargo-lambda
  brew install cargo-lambda
  ```

### Step 1: Create Slack App

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps)
2. Click **Create New App** → **From scratch**
3. Enter app name (e.g., "Bluesky Thread Unroller") and select your workspace
4. Click **Create App**

### Step 2: Configure Message Shortcut

1. In the left sidebar, click **Interactivity & Shortcuts**
2. Toggle **Interactivity** to **On**
3. Under **Shortcuts**, click **Create New Shortcut**
4. Select **On messages** (not global)
5. Fill in:
   - **Name**: `Unroll Bluesky Thread`
   - **Short Description**: `Unroll a Bluesky thread into Slack replies`
   - **Callback ID**: `unroll_bluesky_thread` (must match exactly)
6. Click **Create**
7. Leave the **Request URL** blank for now (we'll set it after deploying Lambda)

### Step 3: Add Bot Permissions

1. In the left sidebar, click **OAuth & Permissions**
2. Under **Scopes** → **Bot Token Scopes**, add:
   - `chat:write` - Post messages
   - `links:read` - Read URLs in messages
   - `links:write` - Required for Slack to unfurl links in bot messages
3. Scroll up and click **Install to Workspace**
4. Authorize the app
5. Copy the **Bot User OAuth Token** (starts with `xoxb-`)

### Step 4: Get Signing Secret

1. In the left sidebar, click **Basic Information**
2. Under **App Credentials**, find **Signing Secret**
3. Click **Show** and copy it

### Step 5: Deploy Lambda

```bash
# Build for Lambda (ARM64 for better performance/cost)
cargo lambda build --release --arm64

# Deploy to AWS
cargo lambda deploy bsky-slack-lambda \
  --iam-role arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_LAMBDA_ROLE \
  --env-var SLACK_BOT_TOKEN=xoxb-your-token \
  --env-var SLACK_SIGNING_SECRET=your-signing-secret
```

If you don't have a Lambda execution role, create one with basic Lambda permissions:

```bash
# Create the role (one-time setup)
aws iam create-role \
  --role-name bsky-slack-lambda-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach basic execution policy
aws iam attach-role-policy \
  --role-name bsky-slack-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

### Step 6: Create API Gateway

1. Go to [API Gateway Console](https://console.aws.amazon.com/apigateway)
2. Click **Create API** → **HTTP API** → **Build**
3. Click **Add integration**:
   - Integration type: **Lambda**
   - Lambda function: `bsky-slack-lambda`
4. API name: `bsky-slack-api`
5. Click **Next**
6. Configure route:
   - Method: **POST**
   - Resource path: `/slack/events`
7. Click **Next** → **Next** → **Create**
8. Copy the **Invoke URL** (e.g., `https://abc123.execute-api.us-east-1.amazonaws.com`)

### Step 7: Configure Slack Request URL

1. Go back to your Slack app settings
2. Click **Interactivity & Shortcuts**
3. Set **Request URL** to: `https://YOUR_API_GATEWAY_URL/slack/events`
4. Click **Save Changes**

### Step 8: Invite Bot to Channel

The bot must be invited to any channel where you want to use it:

1. Go to the channel where you want to use the shortcut
2. Type `/invite @YourBotName` (use your bot's app name)
3. Or: Click channel name → **Integrations** → **Add apps**

### Step 9: Test

1. In Slack, find a message containing a Bluesky post URL
2. Click the **⋮** (more actions) menu on the message
3. Select **Unroll Bluesky Thread**
4. The bot will post each thread reply as a Slack thread message

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SLACK_BOT_TOKEN` | Bot OAuth token (starts with `xoxb-`) |
| `SLACK_SIGNING_SECRET` | Used to verify requests are from Slack |

---

## How It Works

1. User triggers "Unroll Bluesky Thread" shortcut on a message
2. Lambda receives the request and verifies the Slack signature
3. Extracts the Bluesky URL from the message text
4. Fetches the thread from Bluesky's public API
5. Filters to only include posts by the original author
6. Posts each post's URL as a threaded reply (Slack auto-unfurls them)

---

## Troubleshooting

**"No Bluesky URL found in message"**
- Make sure the message contains a full Bluesky URL like `https://bsky.app/profile/user/post/xyz`

**"Invalid signature" / 401 errors**
- Verify `SLACK_SIGNING_SECRET` is set correctly in Lambda environment variables
- Make sure you copied the signing secret from Basic Information, not the verification token

**Messages not posting**
- Check the bot has been added to the channel, or the channel is public
- Verify `SLACK_BOT_TOKEN` is correct and has `chat:write` scope

**Check CloudWatch Logs**
```bash
aws logs tail /aws/lambda/bsky-slack-lambda --follow
```
