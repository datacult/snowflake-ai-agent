# Cortex Analyst Eval Pipeline

Automated evaluation framework for Snowflake Cortex Analyst — a conversational AI agent that generates SQL from natural language questions. The pipeline scores the agent across five dimensions: SQL correctness, SQL structural accuracy (parameter-level), instruction compliance, hallucination detection, and natural language response quality.

---

## How It Works

For each question in your golden set, the pipeline:

1. Sends the question to the **Cortex Analyst REST API**
2. Extracts the **generated SQL** and **natural language response**
3. Executes both expected and generated SQL against **Snowflake**
4. Compares query results for correctness
5. Parses both SQLs to compare structure across 6 dimensions (tables, columns, filters, aggregations, joins, ordering)
6. Checks the generated SQL against VBB coding standards (instruction compliance)
7. Runs `EXPLAIN` on the generated SQL to detect hallucinations
8. Scores the natural language response quality on a 0–5 scale
9. Writes all scores to `results/eval_results.csv` and prints a console summary

---

## Project Structure

```
cortex_eval/
├── config.py                          # Snowflake credentials & configuration
├── eval_pipeline.py                   # Main evaluation engine
├── requirements.txt                   # Python dependencies
├── golden_answers.csv                 # Input: question bank with expected SQL
├── credentials/                       # API credentials (gitignored)
│   └── google_service_account.json
└── results/                           # Evaluation output (gitignored)
    ├── eval_results.csv               # Main output (one row per question)
    ├── eval_results_data_dictionary.xlsx
    └── VBB_Agent_Feedback_Template.xlsx
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Snowflake credentials

Edit `config.py`:

```python
SNOWFLAKE_ACCOUNT   = "your-account-id"        # e.g. "tcdmdcp-zdb95726"
SNOWFLAKE_USER      = "your-username"
SNOWFLAKE_PASSWORD  = "your-password"
SNOWFLAKE_ROLE      = "ACCOUNTADMIN"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE  = "ANALYTICS"
SNOWFLAKE_SCHEMA    = "REPORTING"

# Semantic view for Cortex Analyst
SEMANTIC_VIEW = "ANALYTICS.REPORTING.YOUR_SEMANTIC_VIEW"

# Input/output paths
GOLDEN_QUESTIONS_FILE = "golden_answers.csv"
RESULTS_OUTPUT_PATH   = "results/eval_results.csv"
```

The Snowflake user needs:
- `CORTEX_USER` role (for Cortex Analyst API access)
- `SELECT` access to the semantic view and underlying tables

### 3. Prepare your golden questions

Create or update `golden_answers.csv` with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `id` | int | Unique question identifier |
| `question` | string | Natural language question to send to Cortex Analyst |
| `expected_sql` | string | Hand-written correct SQL (ground truth) |
| `Category` | string | Question type — e.g. `Aggregation`, `Ratio Metric`, `Segmentation` |
| `Difficulty` | string | `Easy`, `Medium`, or `Hard` |

**Example row:**
```
id,question,expected_sql,Category,Difficulty
1,What was total gross revenue last week by channel?,"SELECT channel_type, SUM(gross_revenue) FROM fct_marketing_activity WHERE ...",Aggregation,Easy
```

### 4. Run the evaluation

```bash
python eval_pipeline.py
```

The script will prompt for your Snowflake MFA passcode if required, then process all questions in the golden set (with a 1-second delay between calls for rate limiting).

---

## Scoring Methodology

### 1. SQL Correctness

Runs both the expected and generated SQL, then compares results:

- **Hash-based match** — MD5 hash of sorted CSV representation; handles column name/order differences
- **Value-only fallback** — compares values only if column names differ but results are the same
- **Row count match** — separate check for whether the number of rows matches

**Output fields:** `sql_executed_successfully`, `expected_executed_successfully`, `results_match`, `row_count_match`, `expected_row_count`, `generated_row_count`

---

### 2. Parameter-Level Accuracy (Structural SQL Comparison)

Parses both SQLs into Abstract Syntax Trees using [SQLGlot](https://github.com/tobymao/sqlglot) and compares them across 6 dimensions. Each dimension uses **Jaccard similarity** (`|A ∩ B| / |A ∪ B|`) to give partial credit.

| Dimension | Weight | What it checks |
|-----------|--------|----------------|
| Tables | 20% | Physical table names referenced (excludes CTE aliases) |
| Columns | 25% | All column references (case-insensitive) |
| Filters | 20% | Individual WHERE predicates split by AND |
| Aggregations | 20% | Aggregate functions (SUM, COUNT, etc.) + GROUP BY columns |
| Joins | 10% | Join type and count |
| Ordering | 5% | ORDER BY columns + LIMIT clause |

**Weighted overall formula:**
```
param_accuracy = (
    0.20 × tables_score +
    0.25 × columns_score +
    0.20 × filters_score +
    0.20 × aggregations_score +
    0.10 × joins_score +
    0.05 × ordering_score
) × 100
```

For each dimension, there are two output fields:
- `param_<dim>_match` (bool) — exact match
- `param_<dim>_score` (float 0.0–1.0) — Jaccard similarity (partial credit)

**Output fields:** `param_accuracy`, `param_tables_match/score`, `param_columns_match/score`, `param_filters_match/score`, `param_aggregations_match/score`, `param_joins_match/score`, `param_ordering_match/score`, `param_details`

---

### 3. Instruction Compliance

Checks whether the generated SQL follows VBB coding standards. Each rule is boolean (pass/fail). Rules that don't apply to a query are treated as N/A and default to `True`.

| Rule | Weight | What it checks |
|------|--------|----------------|
| Timezone conversion | 25% | Date fields use `CONVERT_TIMEZONE('UTC', 'America/New_York', ...)` |
| JOIN type | 25% | Uses `LEFT JOIN` not `LEFT OUTER JOIN` |
| ROAS column | 25% | ROAS uses `gross_less_discount_no_vat_normalized`, not gross revenue |
| Revenue column | 25% | Default revenue uses `net_revenue_no_vat_normalized` |
| Google filter | 25% | Google platform filtered with `LOWER(platform) = 'google'` |

**Overall compliance score** = (rules passed / 5) × 100%

**Output fields:** `compliance_score`, `compliance_timezone`, `compliance_join_type`, `compliance_roas_column`, `compliance_revenue_column`, `compliance_google_filter`, `compliance_details`

---

### 4. Hallucination Detection

Runs `EXPLAIN` on the generated SQL (compiles without executing) to catch references to entities that don't exist in the schema.

Detects:
- Non-existent columns (`UNKNOWN_COLUMN`, `INVALID IDENTIFIER`)
- Non-existent tables (`OBJECT_DOES_NOT_EXIST`)
- Ambiguous column references (`AMBIGUOUS COLUMN`)

**Output fields:** `is_hallucination` (bool), `hallucination_details` (error text)

---

### 5. Natural Language Response Quality (0–5)

Heuristic scoring of the agent's text explanation alongside the SQL.

**Base score logic:**

| Condition | Score |
|-----------|-------|
| No response | 0 |
| Response exists but irrelevant | 1 |
| Partially relevant but misleading | 2 |
| Relevant but incomplete (baseline when SQL generated) | 3 |
| Good with minor issues | 4 |
| Excellent | 5 |

**Adjustments:**
- If response contains refusal language ("cannot", "unable to", etc.) and no SQL was generated → score 4 (appropriate refusal)
- If refusal language used but SQL was generated → score 2 (contradictory)
- If response is fewer than 20 characters → floor score at 2

**Bonus points** (only applied when SQL was generated, max score 5):

| Attribute | Bonus | Checked by |
|-----------|-------|-----------|
| Mentions time period | +0.25 | Keywords: "week", "month", "Q1", date strings |
| Structured format | +0.25 | Bullet points, numbered lists, headers, bold text |
| Includes recommendations | +0.25 | Keywords: "recommend", "suggest", "opportunity", "optimize" |
| Cites specific numbers | +0.25 | Dollar amounts, percentages, formatted numbers |

**Output fields:** `nl_quality_score` (0–5), `nl_quality_notes`, `nl_mentions_time_period`, `nl_is_structured`, `nl_has_recommendations`, `nl_has_specific_numbers`

---

## Output: Results CSV

`results/eval_results.csv` contains one row per evaluated question with 46 columns:

### Identification
| Field | Type | Description |
|-------|------|-------------|
| `question_id` | int | Matches `id` from golden_answers.csv |
| `question` | string | The natural language question asked |
| `category` | string | Question category |
| `difficulty` | string | Easy / Medium / Hard |

### SQL Content
| Field | Type | Description |
|-------|------|-------------|
| `expected_sql` | string | Ground truth SQL from golden set |
| `generated_sql` | string | SQL returned by Cortex Analyst (null if none) |
| `nl_response` | string | Natural language explanation from Cortex Analyst |

### SQL Execution Results
| Field | Type | Description |
|-------|------|-------------|
| `sql_executed_successfully` | bool | Generated SQL ran without error |
| `expected_executed_successfully` | bool | Expected SQL ran without error |
| `results_match` | bool | Query results are identical |
| `row_count_match` | bool | Row counts are equal |
| `expected_row_count` | int | Rows returned by expected SQL |
| `generated_row_count` | int | Rows returned by generated SQL |

### Failure & Hallucination
| Field | Type | Description |
|-------|------|-------------|
| `is_failure` | bool | True when Cortex Analyst returned no SQL at all |
| `is_hallucination` | bool | True when generated SQL references non-existent schema objects |
| `hallucination_details` | string | Snowflake error text when hallucination detected |

### Parameter-Level Accuracy
| Field | Type | Description |
|-------|------|-------------|
| `param_accuracy` | float (0–100) | Overall weighted accuracy across all 6 dimensions |
| `param_tables_match` | bool | Tables used are exactly the same |
| `param_tables_score` | float (0–1) | Jaccard similarity for tables |
| `param_columns_match` | bool | Columns referenced are exactly the same |
| `param_columns_score` | float (0–1) | Jaccard similarity for columns |
| `param_filters_match` | bool | WHERE conditions are exactly the same |
| `param_filters_score` | float (0–1) | Jaccard similarity for filters |
| `param_aggregations_match` | bool | Aggregate functions + GROUP BY are exactly the same |
| `param_aggregations_score` | float (0–1) | Jaccard similarity for aggregations |
| `param_joins_match` | bool | JOIN types and count are exactly the same |
| `param_joins_score` | float (0–1) | Jaccard similarity for joins |
| `param_ordering_match` | bool | ORDER BY + LIMIT are exactly the same |
| `param_ordering_score` | float (0–1) | Jaccard similarity for ordering |
| `param_details` | string | Human-readable diff summary (what was missing/extra) |

### Instruction Compliance
| Field | Type | Description |
|-------|------|-------------|
| `compliance_score` | float (0–100) | Percentage of applicable rules passed |
| `compliance_timezone` | bool | Timezone conversion rule met (or N/A → True) |
| `compliance_join_type` | bool | JOIN type rule met |
| `compliance_roas_column` | bool | ROAS column rule met (or N/A → True) |
| `compliance_revenue_column` | bool | Revenue column rule met (or N/A → True) |
| `compliance_google_filter` | bool | Google filter rule met (or N/A → True) |
| `compliance_details` | string | Which rules passed/failed and why |

### NL Response Quality
| Field | Type | Description |
|-------|------|-------------|
| `nl_quality_score` | int (0–5) | Overall quality score |
| `nl_quality_notes` | string | Explanation of score |
| `nl_mentions_time_period` | bool | Response mentions a date range or period |
| `nl_is_structured` | bool | Response uses lists, headers, or bold formatting |
| `nl_has_recommendations` | bool | Response includes actionable insights |
| `nl_has_specific_numbers` | bool | Response cites specific values |

### Metadata
| Field | Type | Description |
|-------|------|-------------|
| `latency_seconds` | float | API response time in seconds |
| `error_message` | string | Any exception caught during evaluation |
| `timestamp` | string | UTC timestamp of when the question was evaluated |

---

## Console Report

After all questions are evaluated, a summary is printed:

```
======================================================================
CORTEX ANALYST EVALUATION REPORT
Run: 2026-02-25 21:28 UTC
Total questions: 838
======================================================================

📊 PERFORMANCE SUMMARY
  SQL Generation Rate:           95.5%   (SQL returned vs. total questions)
  SQL Execution Rate:            87.3%   (generated SQL ran without error)
  Exact Result Match:            73.3%   (results identical to expected)
  Hallucination Rate:             2.1%   (SQL referenced non-existent objects)
  Instruction Compliance:        85.4%   (VBB coding standards)
  Avg Parameter Accuracy:        78.5%   (structural SQL similarity, weighted)
  Avg NL Quality Score:          3.8/5

📂 BY CATEGORY
          count  accuracy  failures  avg_latency
Aggregation  120     0.850         5        4.21
...

🔧 PARAMETER ACCURACY
  Dimension       Exact Match  Similarity Score
  Tables               92%         0.95
  Columns              85%         0.87
  ...

📋 INSTRUCTION COMPLIANCE
  Timezone Conv.       87%
  JOIN Type            95%
  ...
```

---

## Interpreting Results

| Metric | What it means | Action when low |
|--------|--------------|-----------------|
| **SQL Generation Rate** | % of questions where Cortex Analyst returned any SQL | Review questions that caused refusals; improve semantic model coverage |
| **Exact Result Match** | % where generated query results are identical to expected | Highest-signal metric — investigate SQL diffs for failing questions |
| **Param Accuracy** | Structural SQL similarity (0–100%) | Shows *how close* the SQL is even when results don't match |
| **Hallucination Rate** | % of SQL queries referencing non-existent schema objects | Update semantic view column/table definitions |
| **Instruction Compliance** | % of VBB coding rules met | Identify which rule fails most; add instructions to semantic model |
| **NL Quality Score** | Response explanation quality (0–5) | Score ≤3 means responses lack structure, specifics, or recommendations |

**On the `_match` vs `_score` distinction for parameters:**
- `param_tables_match = True` means the exact same set of tables was used
- `param_tables_score = 0.8` means 80% overlap by Jaccard — the agent got most tables right but missed one or used an extra one

**On compliance N/A rules:**
- Rules that don't apply to a query (e.g. no ROAS column, no Google filter) default to `True`. This means `compliance_score = 100%` for a simple query that triggers none of the rules — that is expected and correct.

---

## Improving Your Semantic Model

After each evaluation run:

1. **Hallucinations** → Fix column/table names in the semantic view that the agent is hallucinating
2. **Low param accuracy on filters** → Add custom filters or date logic to the semantic view instructions
3. **Low compliance scores** → Add explicit coding standards as instructions in the semantic model
4. **Low NL quality** → Add response format guidelines to the semantic model system prompt
5. **Consistent failures on a question category** → Add verified example queries for that category

Re-run evals after changes to measure improvement over baseline.

---

## Authentication Notes

The pipeline uses **session token auth** by default — the Snowflake connector extracts a token from your active connection. This requires the user to authenticate interactively (including MFA if enabled).

For automated/CI usage, switch to **key-pair authentication** by updating `get_snowflake_connection()` in `eval_pipeline.py`:

```python
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    private_key_file="/path/to/rsa_key.p8",
    private_key_file_pwd=os.environ["PRIVATE_KEY_PASSPHRASE"],
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
)
```

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `snowflake-connector-python` | Snowflake connection and SQL execution |
| `pandas` | Data manipulation, CSV I/O, reporting |
| `requests` | HTTP calls to Cortex Analyst REST API |
| `sqlglot` | SQL parsing for parameter-level accuracy scoring |
| `tabulate` | Console table formatting |
| `openpyxl` | Excel output for data dictionary and feedback template |

---

## Security Notes

- `config.py` stores credentials in plain text. For production, use environment variables or a secrets manager instead.
- The `results/` directory may contain sensitive SQL and query results — handle per your data governance policy.
- Both `config.py` credentials and `credentials/` are gitignored by default.
