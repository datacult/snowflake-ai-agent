# Snowflake AI Agent Implementation

This repository contains three projects for setting up, building, and evaluating AI-powered analytics on top of Snowflake Cortex.

---

## Projects

### 1. `setup-snowflake-agent/` — Snowflake Cortex Agent Setup Guide

A step-by-step guide for setting up Snowflake Cortex Agent from scratch, including roles, semantic views, and agent configuration.

**What it covers:**
- Creating the required Snowflake role and granting necessary permissions
- Creating and configuring a Cortex Agent in the Snowflake UI (About, Tools, Orchestration, Access tabs)
- Building a Cortex Analyst semantic view — either through the UI or via the `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` stored procedure — with full YAML schema examples covering dimensions, facts, metrics, and table relationships
- Adding context to the agent via semantic view definitions and custom SQL generation instructions
- Writing agent orchestration and response instructions to control routing logic, tone, and output format
- Enabling cross-region inference to access models outside your Snowflake region
- Setting up Cortex Search for unstructured data (in progress)

**Key files:**
| File | Description |
|------|-------------|
| `setup.MD` | Full pictorial setup guide with SQL snippets and YAML examples |
| `images/` | Screenshots referenced throughout the guide |

---

### 2. `cortex_eval/` — Cortex Analyst Evaluation Pipeline

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

### 3. `slack-bot-demo/` — Slack Bot for Cortex Agent

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

## Resources

| Topic | Link |
|-------|------|
| Cortex Agents | [Snowflake Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents) |
| Cortex Analyst | [Snowflake Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst) |
| Cortex Search | [Snowflake Cortex Search Overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview) |
| Semantic Views | [Semantic Views Overview](https://docs.snowflake.com/en/user-guide/views-semantic/overview) |
| Cross-Region Inference | [Cross-Region Inference Parameter](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cross-region-inference#how-to-use-the-cross-region-inference-parameter) |
| Slack Integration | [Integrate Cortex Agents with Slack](https://www.snowflake.com/en/developers/guides/integrate-snowflake-cortex-agents-with-slack/) |
| Agent Evaluation | [Getting Started with Cortex Agent Evaluations](https://www.snowflake.com/en/developers/guides/getting-started-with-cortex-agent-evaluations/) |

---

## Full Workflow

```
 ┌──────────────────────────── PHASE 1: SETUP (setup-snowflake-agent/) ──────────────────────────────┐
 │                                                                                                     │
 │   Create Role & Permissions                                                                         │
 │   Build Semantic View  ──►  Tables · Dimensions · Facts · Metrics · Relationships                  │
 │   Configure Cortex Agent  ──►  Tools · Orchestration Rules · Response Instructions                 │
 │   Enable Cross-Region Inference (optional)                                                          │
 │                                                                                                     │
 └──────────────────────────────────────────────┬────────────────────────────────────────────────────┘
                                                │  deploys
                                                ▼
                                   ┌─────────────────────────┐
                                   │     CORTEX AGENT         │
                                   │   LLM + Routing Layer    │
                                   │  (plans · selects tool · │
                                   │   reflects · responds)   │
                                   └────────┬────────┬────────┘
                                            │        │
                              ┌─────────────┘        └──────────────┐
                              ▼                                      ▼
                 ┌────────────────────────┐          ┌────────────────────────┐
                 │    CORTEX ANALYST       │          │    CORTEX SEARCH        │
                 │   Structured Data       │          │   Unstructured Data     │
                 │   Natural Language      │          │   Semantic Text Search  │
                 │      → SQL              │          │                         │
                 └───────────┬────────────┘          └────────────┬───────────┘
                             └──────────────┬─────────────────────┘
                                            ▼
                                ┌───────────────────────┐
                                │   SNOWFLAKE DATA WH    │
                                │   Tables · Views        │
                                └───────────────────────┘

                                            │
          ┌─────────────────────────────────┴──────────────────────────────────┐
          │                                                                      │
          ▼                                                                      ▼

  ┌──────────────────────────────────────────────────┐    ┌──────────────────────────────────────────────────┐
  │  PHASE 2: LIVE QUERY (Snowflake Intelligence UI) │    │   PHASE 3: EVALUATION & ITERATION (cortex_eval/) │
  └──────────────────────────────────────────────────┘    └──────────────────────────────────────────────────┘

  User (Snowflake Intelligence)                           Golden Questions  (golden_answers.csv)
    │  Types a question in the chat UI                   [ NL question · expected SQL · category · difficulty ]
    ▼                                                            │
  Snowflake Intelligence                                         ▼
    │  • Native Snowflake chat interface                  eval_pipeline.py  ──►  Cortex Analyst API
    │  • No external setup required                             │
    │  • Renders charts, tables, and                            ├──  Run expected SQL   ──►  Snowflake DW
    │    formatted responses natively                           └──  Run generated SQL  ──►  Snowflake DW
    │  • Access controlled via Agent                            │
    │    Access Tab (role-based)                                ▼  Automated Scoring (5 dimensions)
    ▼                                                     ┌──────────────────────────────────────────┐
  Cortex Agent                                            │  SQL Correctness   (result-set match)     │
    │  routes to Cortex Analyst or Search                 │  Param Accuracy    (tables · columns ·    │
    ▼                                                     │                     filters · aggs · joins)│
  Snowflake DW  ──►  structured answer                    │  Compliance        (timezone · JOIN type · │
    │                                                     │                     revenue col · etc.)    │
    ▼                                                     │  Hallucination     (EXPLAIN schema check)  │
  Answer rendered in Snowflake Intelligence UI            │  NL Quality        (0–5 heuristic score)   │
                                                    └──────────────────────┬───────────────────┘
                                                                           │
                                                                           ▼
                                                                  eval_results.csv
                                                                  (46 cols · 1 row per question)
                                                                           │
                                                                           ▼
                                                              ┌────────────────────────────┐
                                                              │      HUMAN IN THE LOOP      │
                                                              │   Feedback Template (.xlsx) │
                                                              │                             │
                                                              │  Review failures by:        │
                                                              │  · category & difficulty    │
                                                              │  · hallucination details    │
                                                              │  · compliance rule failures │
                                                              │  · low NL quality scores    │
                                                              └────────────┬───────────────┘
                                                                           │  identify root causes
                                                                           ▼
                                                              ┌────────────────────────────┐
                                                              │      ITERATE & IMPROVE      │
                                                              │                             │
                                                              │  Hallucinations             │
                                                              │   → fix semantic view cols  │
                                                              │  Low param accuracy         │
                                                              │   → add filters / date logic│
                                                              │  Low compliance             │
                                                              │   → add coding standards    │
                                                              │  Low NL quality             │
                                                              │   → improve response rules  │
                                                              │  Consistent failures        │
                                                              │   → add verified examples   │
                                                              └────────────┬───────────────┘
                                                                           │
                                                                           └──────────────────────────►
                                                                                    Re-deploy · Re-run eval
                                                                                    Measure delta vs baseline
```
