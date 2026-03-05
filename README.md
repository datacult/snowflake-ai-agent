# Snowflake AI Agent Implementation

This repository contains two projects for building and evaluating AI-powered analytics on top of Snowflake Cortex.

---

## Projects

### 1. `cortex_eval/` — Cortex Analyst Evaluation Pipeline

An automated evaluation framework for measuring the accuracy and quality of a Snowflake Cortex Analyst semantic model.

**What it does:**
- Reads a set of golden questions (natural language + expected SQL) from a CSV/Excel file
- Submits each question to the Cortex Analyst REST API via a semantic view
- Executes both the expected SQL and the Cortex-generated SQL against Snowflake
- Compares result sets and scores each response across four dimensions:
  1. **SQL Correctness** — result-set comparison between expected and generated SQL
  2. **Natural Language Quality** — heuristic / LLM-as-judge response scoring
  3. **Failure / Hallucination Detection** — flags unanswerable or fabricated responses
  4. **Tool Execution Accuracy** — parameter-level SQL accuracy scoring

**Key files:**
| File | Description |
|------|-------------|
| `eval_pipeline.py` | Main evaluation script |
| `golden_answers.csv` | Golden question set with expected SQL, category, and difficulty |
| `results/eval_results.csv` | Output scores per question |
| `results/eval_results_data_dictionary.xlsx` | Field definitions for the results |
| `results/VBB_Agent_Feedback_Template.xlsx` | Feedback template for manual review |

**Question categories covered:** Aggregation, Ratio Metric, Segmentation, Share/Mix Analysis, Multi-Metric Ranking, Time Series, Cross-Dimension Aggregation, Ranked Window Function, Conditional Aggregation

**Setup:**
```bash
cd cortex_eval
pip install -r requirements.txt
# Configure config.py with Snowflake credentials, semantic view name, and file paths
python eval_pipeline.py
```

**Dependencies:** `snowflake-connector-python`, `pandas`, `requests`, `tabulate`, `openpyxl`, `sqlglot`

---

### 2. `slack-bot-demo/` — Slack Bot for Cortex Agent

A Slack bot that exposes a Snowflake Cortex Agent as a conversational interface inside Slack, with persistent thread-level memory.

**What it does:**
- Listens for `@mention` events and `/ask` slash commands in Slack
- Routes each message to the Cortex Agent REST API using Server-Sent Events (SSE) streaming
- Maps each Slack thread to a Cortex `thread_id` so follow-up questions maintain conversation context
- Formats all responses for Slack readability (plain text tables, bullet points, bolded numbers)
- Posts a "thinking" indicator immediately, then updates it in-place with the final answer

**Key files:**
| File | Description |
|------|-------------|
| `app.py` | Slack Bolt app — event handlers for `@mention` and `/ask` |
| `cortex_chat.py` | Cortex Agent API client — SSE parsing and response extraction |

**Usage in Slack:**
- `@BotName what was our ROAS last week?` — mention the bot in any channel
- `/ask what is the CAC by paid channel this month?` — use the slash command

**Setup:**
```bash
cd slack-bot-demo
pip install -r requirements.txt

# Create a .env file with:
# SLACK_BOT_TOKEN=xoxb-...
# SLACK_SIGNING_SECRET=...
# SLACK_APP_TOKEN=xapp-...
# SNOWFLAKE_ACCOUNT=...
# SNOWFLAKE_PAT=...
# AGENT_DATABASE=...
# AGENT_SCHEMA=...
# AGENT_NAME=...

python app.py
```

**Dependencies:** `slack-bolt`, `requests`, `python-dotenv`

---

## Architecture Overview

```
Slack User
    |
    | @mention / /ask
    v
slack-bot-demo (Slack Bolt + Socket Mode)
    |
    | REST API (SSE stream)
    v
Snowflake Cortex Agent
    |
    | Semantic view / SQL execution
    v
Snowflake Data Warehouse
```

The `cortex_eval` pipeline independently tests the same Cortex Analyst layer using a golden question set to measure model quality before deploying changes.
