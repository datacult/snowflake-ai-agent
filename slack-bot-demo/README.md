# Slack Bot — Snowflake Cortex Agent Integration (Local)

A local Python bot that connects your Slack workspace to a **Snowflake Cortex Agent**. Users can ask data questions directly in Slack and get AI-generated answers powered by your Cortex Agent, all without leaving Slack.

---

## How It Works

```
User types in Slack
       │
       ▼
  Slack Event (mention or /ask command)
       │
       ▼
  app.py  ←── slack-bolt (Socket Mode, no public URL needed)
       │
       ▼
  thread_store.py  ──► DynamoDB (fetch conversation history)
       │
       ▼
  cortex_chat.py  ──► Snowflake Cortex Agent REST API (SSE stream)
                       (full message history included in every request)
       │
       ▼
  Parse SSE response (extract final answer + citations)
       │
       ▼
  Save updated history to DynamoDB
       │
       ▼
  Update Slack message with the answer (in the same thread)
```

### Key Design Decisions

| Design | Reason |
|---|---|
| **Socket Mode** | Bot runs locally with no public URL or ngrok required — Slack opens a WebSocket to your machine |
| **Threading** | Cortex can take up to 60 seconds; each request runs in a background thread so Slack's 3-second timeout is never hit |
| **Thinking placeholder** | A "⏳ Analyzing..." message is posted immediately, then edited in-place once the answer arrives |
| **Thread replies** | All bot responses go into a Slack thread so the channel stays clean. Follow-up messages inside the thread continue the same conversation |
| **Conversation history in DynamoDB** | The Cortex Agent public REST API does not return a `thread_id` — conversation context is managed by storing the full message history in DynamoDB and including it in every API call. History is capped at 20 messages and auto-expires after 7 days via TTL. |
| **SSE streaming** | Cortex streams the response; intermediate "thinking" chunks (`content_index` events) are filtered — only the final `content` block is shown |
| **Slack formatting** | A system prompt suffix instructs the agent to avoid charts/graphs (unsupported in Slack) and use plain-text tables and bullet points instead |
| **CSV export** | When a user asks for output "as a csv file", the bot returns both the insights as a Slack message and a downloadable `data.csv` file attached to the thread |

---

## Project Structure

```
slack-bot-demo/
├── app.py            # Slack bot: event handlers, threading, message formatting
├── cortex_chat.py    # Snowflake Cortex Agent client (REST + SSE parser)
├── thread_store.py   # DynamoDB-backed conversation history store
├── aws_secrets.py    # Loads secrets from AWS Secrets Manager into env vars
├── requirements.txt  # Python dependencies
└── .env              # Secrets for local dev (never commit this file)
```

---

## Prerequisites

- Python 3.9+
- A **Snowflake account** with a Cortex Agent already created in Snowflake Intelligence
- A **Slack workspace** where you have permission to install apps
- An **AWS account** with access to Secrets Manager and DynamoDB

---

## Step 1 — Create the Slack App

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps) and click **Create New App → From scratch**.
2. Give it a name (e.g., `Cortex Bot`) and pick your workspace.

### Enable Socket Mode
3. In the left sidebar go to **Settings → Socket Mode** and turn it **On**.
4. It will ask you to create an **App-Level Token** — give it the scope `connections:write`. Copy the token (`xapp-...`) — this is your `SLACK_APP_TOKEN`.

### Add Bot Permissions
5. Go to **OAuth & Permissions → Scopes → Bot Token Scopes** and add:
   - `app_mentions:read`
   - `chat:write`
   - `commands`
   - `files:write`

### Enable Events
6. Go to **Event Subscriptions → Enable Events → On**.
7. Under **Subscribe to bot events** add: `app_mention`.

### Add a Slash Command (optional but included)
8. Go to **Slash Commands → Create New Command**.
   - Command: `/ask`
   - Request URL: put any placeholder URL (Socket Mode doesn't use it)
   - Short Description: `Ask the Cortex agent a question`

### Install the App
9. Go to **OAuth & Permissions → Install App to Workspace**. Approve it.
10. Copy the **Bot User OAuth Token** (`xoxb-...`) — this is your `SLACK_BOT_TOKEN`.
11. Copy the **Signing Secret** from **Basic Information → App Credentials** — this is your `SLACK_SIGNING_SECRET`.

---

## Step 2 — Get Your Snowflake Credentials

### What is Snowsight?
Snowsight **is** your Snowflake UI. When you log into Snowflake at `https://app.snowflake.com`, that interface is Snowsight. Any reference to "go to Snowsight" just means log into Snowflake as normal.

### Account Identifier
In Snowsight: click your username (bottom-left) → **Account → View account details**.
Copy the **Account Identifier** (e.g., `abc12345` or `abc12345.us-east-1`).

### Programmatic Access Token (PAT)

> **Important:** A PAT is scoped to a specific role at creation time. The role you select when generating the PAT is the role that all API calls will run as. Choose this carefully — it must have all the permissions the agent needs.

1. In Snowsight go to your **profile icon (bottom-left) → Settings → Authentication tab**.
2. Under **Programmatic access tokens** click **Generate new token**.
3. Give it a name, set an expiry, and **select the role** the token should use (e.g., `CORTEX_AGENT_USER_ROLE`).
4. Copy the token immediately — it is only shown once. This is your `SNOWFLAKE_PAT`.

### Cortex Agent Details
In Snowsight go to **AI & ML → Agents** and find your agent. The object path gives you the three values you need:

```
AGENT_DATABASE  = the database (e.g., ANALYTICS)
AGENT_SCHEMA    = the schema   (e.g., REPORTING)
AGENT_NAME      = the agent    (e.g., VBB_MARKETING_AGENT_DEMO)
```

---

## Step 3 — Snowflake Permissions Setup

This is the most common source of errors. The role your PAT uses must have explicit access to everything the agent touches.

### Required Grants

Run all of the following as `ACCOUNTADMIN`:

```sql
USE ROLE ACCOUNTADMIN;

-- 1. Grant the Cortex Agent database role (required for the REST API)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER
    TO ROLE CORTEX_AGENT_USER_ROLE;

-- 2. Grant warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_WH
    TO ROLE CORTEX_AGENT_USER_ROLE;

-- 3. Grant access to database and schema
GRANT USAGE ON DATABASE ANALYTICS
    TO ROLE CORTEX_AGENT_USER_ROLE;

GRANT USAGE ON SCHEMA ANALYTICS.REPORTING
    TO ROLE CORTEX_AGENT_USER_ROLE;

-- 4. Grant SELECT on the agent object itself
GRANT USAGE ON AGENT ANALYTICS.REPORTING.<YOUR_AGENT_NAME>
    TO ROLE CORTEX_AGENT_USER_ROLE;

-- 5. Grant SELECT on the semantic view
GRANT SELECT ON VIEW ANALYTICS.REPORTING.<YOUR_SEMANTIC_VIEW>
    TO ROLE CORTEX_AGENT_USER_ROLE;

-- 6. Grant SELECT on ALL tables in the schema (covers all tables the semantic view references)
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.REPORTING
    TO ROLE CORTEX_AGENT_USER_ROLE;

-- 7. Cover any future tables automatically
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS.REPORTING
    TO ROLE CORTEX_AGENT_USER_ROLE;
```

### Verify Grants Landed

After running, confirm everything is in place:

```sql
SHOW GRANTS TO ROLE CORTEX_AGENT_USER_ROLE;
```

Check that `DIM_CUSTOMERS`, `FCT_MARKETING_ACTIVITY`, and every table referenced in your semantic view appears in the results with `SELECT` privilege. If any table is missing, grant it individually:

```sql
GRANT SELECT ON TABLE ANALYTICS.REPORTING.<MISSING_TABLE>
    TO ROLE CORTEX_AGENT_USER_ROLE;
```

> **Why individual tables go missing:** Running `GRANT SELECT ON ALL TABLES` only covers tables that exist at that moment. Tables created later, or tables created by a different role, won't be included. The `GRANT SELECT ON FUTURE TABLES` statement fixes this going forward. Always verify with `SHOW GRANTS` after running bulk grants.

---

## Step 4 — Set Up DynamoDB for Conversation History

The bot stores conversation history in DynamoDB so follow-up questions retain context even after restarts.

### Create the Table

1. Go to **AWS Console → DynamoDB → Tables → Create table**.
2. Set:
   - **Table name:** `SlackBotThreadStore`
   - **Partition key:** `thread_ts` (String)
   - **Table settings:** Default (On-demand capacity)
3. Click **Create table**.

### Enable TTL (auto-expire old conversations)

1. Open the table → go to the **Additional settings** tab.
2. Scroll to **Time to Live (TTL)** and click **Enable**.
3. Set the attribute name to `expires_at`.

### Grant DynamoDB Permissions to the Bot

The IAM role or user the bot runs as needs permission to read and write to this table. Add the following inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem"
      ],
      "Resource": "arn:aws:dynamodb:us-east-1:YOUR_ACCOUNT_ID:table/SlackBotThreadStore"
    }
  ]
}
```

Replace `us-east-1` with your region and `YOUR_ACCOUNT_ID` with your AWS account number.

To find the identity the bot is using, run:
```bash
aws sts get-caller-identity
```

---

## Step 5 — Configure Secrets

The bot loads all secrets from **AWS Secrets Manager** at startup. Create a secret named `SlackAIBotSecret` with the following key/value pairs:

```
SLACK_BOT_TOKEN        xoxb-...
SLACK_SIGNING_SECRET   ...
SLACK_APP_TOKEN        xapp-...
SNOWFLAKE_ACCOUNT      abc12345
SNOWFLAKE_PAT          eyJ...
AGENT_DATABASE         ANALYTICS
AGENT_SCHEMA           REPORTING
AGENT_NAME             VBB_MARKETING_AGENT_DEMO
```

For **local development**, you can create a `.env` file in the project root as a fallback (the bot will use it if Secrets Manager is unavailable):

```dotenv
SLACK_BOT_TOKEN=xoxb-...
SLACK_SIGNING_SECRET=...
SLACK_APP_TOKEN=xapp-...
SNOWFLAKE_ACCOUNT=abc12345
SNOWFLAKE_PAT=eyJ...
AGENT_DATABASE=ANALYTICS
AGENT_SCHEMA=REPORTING
AGENT_NAME=VBB_MARKETING_AGENT_DEMO
```

> **Never commit `.env` to git.** Add it to `.gitignore`.

---

## Step 6 — Install Dependencies & Run

```bash
# Create a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# Install packages
pip install -r requirements.txt

# Run the bot
python app.py
```

You should see:

```
Secrets loaded from AWS Secrets Manager: SlackAIBotSecret
Bot starting...
⚡️ Bolt app is running!
```

The bot is now connected to Slack over a persistent WebSocket. No port forwarding or public URL needed.

To stop the bot press `Ctrl + C` in the terminal.

---

## Using the Bot

### Mention the bot in any channel
First invite the bot to the channel:
```
/invite @YourBotName
```

Then ask a question:
```
@YourBotName what was our CAC last week?
```

The bot will:
1. Post a "⏳ Analyzing..." message **in a thread** immediately.
2. Call the Cortex Agent in the background (can take 30–90 seconds for complex queries).
3. Edit the placeholder message with the final answer + any source citations.

### Use the `/ask` slash command
```
/ask what was total revenue in Q1?
```

This posts your question as a root message in the channel, then replies in that thread with the answer.

### Follow-up questions in the same thread
Reply in the same Slack thread:
```
@YourBotName now break that down by channel
```

Or simply reply without mentioning the bot (if you are replying in a thread the bot started):
```
now break that down by channel
```

The bot includes the full conversation history in every Cortex API call, so follow-up questions retain the full context of the thread. Starting a new message outside the thread begins a fresh conversation.

### Request a CSV export
Add any of the following phrases to your question:
```
@YourBotName show me last month's revenue by channel as a csv file
```

The bot will return:
1. The normal insights as a Slack message in the thread
2. A downloadable `data.csv` file attached to the same thread

### Visualizations
Charts and graphs are not supported in Slack. The agent formats all data as plain-text tables and bullet points when responding through Slack. If you need visualizations, use the agent directly in **Snowflake Intelligence** where charts render natively.

---

## Code Walkthrough

### `app.py` — Slack Bot

| Section | What it does |
|---|---|
| `App(token=..., signing_secret=...)` | Initialises the slack-bolt app |
| `SLACK_FORMAT_INSTRUCTION` | String appended to every prompt telling the agent to use Slack-compatible formatting (no charts, plain-text tables, bold numbers with `*asterisks*`) |
| `CSV_FORMAT_INSTRUCTION` | Alternative prompt suffix used when the user asks for CSV output — instructs Cortex to return insights + raw CSV separated by `---CSV---` |
| `@app.event('app_mention')` | Fires when someone `@mentions` the bot; uses `thread_ts` to always reply in the same Slack thread |
| `@app.event('message')` | Fires on plain thread replies — responds only if the thread already has history in DynamoDB |
| `@app.command('/ask')` | Fires when someone uses `/ask`; creates a new thread from the question |
| `threading.Thread(target=run_agent)` | Runs the Cortex API call in a background thread so Slack's 3-second response window is not exceeded |
| `client.chat_postMessage(thread_ts=...)` | Posts the "thinking" placeholder inside the thread |
| `client.chat_update(...)` | Replaces the placeholder with the real answer |
| `client.files_upload_v2(...)` | Uploads the CSV file to the thread when a CSV export is requested |
| `SocketModeHandler` | Opens a persistent WebSocket to Slack — no public URL needed |

### `cortex_chat.py` — Cortex Agent Client

| Section | What it does |
|---|---|
| `get_agent_url()` | Builds the Snowflake REST endpoint from env vars: `https://<account>.snowflakecomputing.com/api/v2/databases/<db>/schemas/<schema>/agents/<name>:run` |
| `ask_agent(prompt, history)` | Builds the full `messages` array from prior history + current prompt, then POSTs to the Cortex Agent with PAT Bearer token auth and SSE streaming enabled |
| `parse_sse(response)` | Iterates SSE `data:` lines. Explicitly ignores intermediate thinking/planning chunks (`content_index` + `text` events). Extracts only the final `content` block containing the answer text and citation annotations |

### `thread_store.py` — Conversation History

| Section | What it does |
|---|---|
| `get_history(thread_ts)` | Fetches the conversation message list for a Slack thread from DynamoDB |
| `save_history(thread_ts, messages)` | Writes the updated message list to DynamoDB with a 7-day TTL. Caps at 20 messages to stay within DynamoDB's item size limits |

> **Why not use Cortex's built-in thread_id?** The Cortex Agent public REST API does not return a `thread_id` in its SSE response (confirmed via Snowflake monitoring traces). Thread state must be managed by the caller. Snowflake Intelligence uses an internal API that handles this automatically — the public API does not.

### `aws_secrets.py` — Secrets Loader

Fetches the `SlackAIBotSecret` JSON secret from AWS Secrets Manager on startup and injects every key into `os.environ`. Falls back to `.env` (via python-dotenv) if AWS is unavailable, making local development straightforward.

---

## Troubleshooting

### Common Errors and Fixes

| Error | Likely Cause | Fix |
|---|---|---|
| `400 Bad Request` | Request body format issue or role doesn't have access to the agent object | Check `X-Snowflake-Role` header is set; grant USAGE on the agent object to your role |
| `401 Unauthorized` | PAT has expired or wrong role was selected when creating the PAT | Generate a new PAT in Snowsight, make sure to select the correct role |
| `403 Forbidden` | Role missing `SNOWFLAKE.CORTEX_AGENT_USER` database role | `GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE <your_role>` |
| `missing_scope: files:write` | Bot is missing the `files:write` Slack scope | Add `files:write` in Slack app OAuth scopes and reinstall the app |
| `Semantic model failed validation: table does not exist or not authorized` | The role has no SELECT on one or more tables referenced in the semantic view | Run `SHOW GRANTS TO ROLE <your_role>` and compare against tables in the semantic view. Grant any missing tables individually |
| Bot posts the agent's internal reasoning ("Let me analyze...") | SSE parser was accumulating `content_index` delta chunks instead of waiting for the final `content` block | The `parse_sse` function explicitly skips `content_index` events — make sure you're using the latest `cortex_chat.py` |
| `(no response)` in Slack | The SSE stream returned no final `content` block — usually means the agent hit an internal error | Add `print(f"SSE event: {payload[:200]}")` in `parse_sse` to log raw events and diagnose |
| Bot replies but says "technical database connection issue" | The agent's SQL query failed silently — permissions issue or bad SQL generation | Run the query manually in a Snowflake worksheet using the role the PAT uses to confirm it works |
| Bot doesn't respond to mentions | `app_mention` event not enabled, or bot not invited to the channel | Enable event in Slack app settings; run `/invite @YourBotName` in the channel |
| Bot doesn't respond to thread replies | The thread has no history in DynamoDB (e.g., bot was restarted mid-thread) | Start a new thread with an `@mention` or `/ask` |
| Slack timeout — bot never replies | Cortex call is taking longer than Slack expects | The background `threading.Thread` pattern handles this — make sure `ack()` or `chat_postMessage` fires before the agent call starts |
| `MissingSchema: Invalid URL` | `SNOWFLAKE_ACCOUNT` env var is missing or malformed | Check secrets — account should be just the identifier e.g. `abc12345`, not the full URL |
| DynamoDB `AccessDeniedException` | IAM role/user missing `dynamodb:GetItem` or `dynamodb:PutItem` | Add the inline DynamoDB policy to the IAM role the bot runs as |

### Debugging the SSE Stream

If you're getting unexpected responses, temporarily add logging to `parse_sse` in `cortex_chat.py` to see exactly what Snowflake is sending:

```python
# Add this inside the for loop in parse_sse, after json.loads:
print(f"SSE event: {payload[:300]}")
```

This prints every event to the terminal. Look for:
- Events with `"status": "planning"` — internal agent reasoning, should be ignored
- Events with `"content_index"` + `"text"` — thinking/planning deltas, should be ignored
- Events with `"content": [...]` — the final answer block you want
- Events with `"error"` inside a `content` block — these surface as `⚠️ Agent error:` messages in Slack

### Verifying Permissions Step by Step

If you keep getting semantic view validation errors, work through this checklist in order:

```sql
-- 1. Check what role the PAT is using
-- (verify this matches what you expect — set in cortex_chat.py as X-Snowflake-Role)

-- 2. List all grants for that role
SHOW GRANTS TO ROLE CORTEX_AGENT_USER_ROLE;

-- 3. Check which tables your semantic view references
-- Open the semantic view YAML in Snowsight → AI & ML → Agents → your agent → Cortex Analyst tool
-- Every base_table listed in the YAML needs SELECT granted to the role

-- 4. Grant any missing tables
GRANT SELECT ON TABLE ANALYTICS.REPORTING.<MISSING_TABLE>
    TO ROLE CORTEX_AGENT_USER_ROLE;
```

---

## Security Notes

- Secrets are loaded from **AWS Secrets Manager** at startup — never hardcoded or committed to git.
- The `.env` file is a local development fallback only. Add it to `.gitignore` and never push it.
- The Snowflake PAT has an expiry date — rotate it before it expires to avoid downtime.
- PATs are scoped to a role at creation time. Use the most restrictive role possible — avoid creating PATs with `ACCOUNTADMIN`.
- The `X-Snowflake-Role` header in `cortex_chat.py` explicitly sets the role for every API call. This must match the role the PAT was created for.
- The Slack Bolt SDK validates request signatures automatically using `SLACK_SIGNING_SECRET`. Keep this secret secure.
- DynamoDB stores only the conversation text — no credentials or PII beyond what users type in Slack. Data auto-expires after 7 days via TTL.

---

## PAT Expiry

Your PAT expires on the date shown in Snowsight under your profile → Settings → Authentication. When it expires the bot will return `401 Unauthorized` errors. To renew:

1. Go to Snowsight → your profile → **Settings → Authentication**.
2. Click **Generate new token**, select the same role (`CORTEX_AGENT_USER_ROLE`), set a new expiry.
3. Update `SNOWFLAKE_PAT` in your AWS Secrets Manager secret.
4. Restart the bot.
