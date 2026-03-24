"""
Cortex Analyst Eval Pipeline
=============================
Reads golden questions from a local Excel file, sends them to Cortex Analyst REST API
(using a semantic view), runs both expected and generated SQL, compares results,
and scores across:
  1. SQL correctness (result comparison)
  2. Natural language response quality (heuristic / LLM-as-judge)
  3. Failure/hallucination detection
  4. Parameter-level SQL accuracy (Tool Execution Accuracy)

Requirements:
  pip install snowflake-connector-python pandas tabulate openpyxl sqlglot
"""

import re
import json
import time
import argparse
import hashlib
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional

import requests
import pandas as pd
import snowflake.connector
import sqlglot
from sqlglot import exp

from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SEMANTIC_VIEW,
    GOLDEN_QUESTIONS_FILE,
)
from results import check_duplicate_name, save_results, save_manifest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# =============================================================================
# Data classes
# =============================================================================
@dataclass
class GoldenQuestion:
    id: int
    question: str
    expected_sql: str
    category: str = ""  # e.g. "simple", "aggregation", "comparison", "edge_case"
    difficulty: str = ""  # e.g. "easy", "medium", "hard"


@dataclass
class EvalResult:
    question_id: int
    question: str
    category: str
    difficulty: str
    expected_sql: str
    generated_sql: Optional[str] = None
    nl_response: Optional[str] = None
    # SQL correctness
    sql_executed_successfully: bool = False
    expected_executed_successfully: bool = False
    results_match: bool = False
    row_count_match: bool = False
    expected_row_count: int = 0
    generated_row_count: int = 0
    # NL quality
    nl_quality_score: int = 0  # 0-5
    nl_quality_notes: str = ""
    # Failure / hallucination
    is_failure: bool = False  # Cortex Analyst returned no SQL
    is_hallucination: bool = False  # references non-existent columns/tables
    hallucination_details: str = ""
    # Parameter-level accuracy (Tool Execution Accuracy)
    param_accuracy: float = 0.0  # overall 0-100% (weighted)
    param_tables_match: bool = False
    param_columns_match: bool = False
    param_filters_match: bool = False
    param_aggregations_match: bool = False
    param_joins_match: bool = False
    param_ordering_match: bool = False
    # Partial scores per dimension (0.0 – 1.0, Jaccard similarity)
    param_tables_score: float = 0.0
    param_columns_score: float = 0.0
    param_filters_score: float = 0.0
    param_aggregations_score: float = 0.0
    param_joins_score: float = 0.0
    param_ordering_score: float = 0.0
    param_details: str = ""  # human-readable diff summary
    # Instruction compliance (0.0 – 1.0 per rule, overall 0-100%)
    compliance_score: float = 0.0
    compliance_timezone: bool = False
    compliance_join_type: bool = False
    compliance_roas_column: bool = True  # True if N/A (no ROAS calc)
    compliance_revenue_column: bool = True  # True if N/A (no revenue calc)
    compliance_google_filter: bool = True  # True if N/A (no Google filter)
    compliance_details: str = ""
    # NL response quality (enhanced)
    nl_mentions_time_period: bool = False
    nl_is_structured: bool = False
    nl_has_recommendations: bool = False
    nl_has_specific_numbers: bool = False
    # Metadata
    latency_seconds: float = 0.0
    error_message: str = ""
    timestamp: str = ""


# =============================================================================
# Golden questions reader
# =============================================================================
def load_golden_questions(file_path: str) -> list[GoldenQuestion]:
    """
    Load golden questions from a local CSV file.
    Expected columns: id, question, expected_sql, category, difficulty
    """
    df = pd.read_csv(file_path)

    questions = []
    for i, row in df.iterrows():
        questions.append(GoldenQuestion(
            id=row.get("id", i + 1),
            question=str(row.get("question", "")),
            expected_sql=str(row.get("expected_sql", "")),
            category=str(row.get("category", "")),
            difficulty=str(row.get("difficulty", "")),
        ))

    logger.info(f"Loaded {len(questions)} golden questions from {file_path}")
    return questions


# =============================================================================
# Snowflake connection
# =============================================================================
def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    passcode = input("Enter your Snowflake MFA passcode: ").strip()
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        passcode=passcode,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def get_session_token(conn) -> str:
    """Extract session token from Snowflake connection for REST API auth."""
    return conn.rest.token


def execute_sql(conn, sql: str, timeout: int = 60) -> Optional[pd.DataFrame]:
    """Execute SQL and return results as a DataFrame."""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        logger.warning(f"SQL execution failed: {e}")
        return None


# =============================================================================
# Cortex Analyst API
# =============================================================================
def call_cortex_analyst(
    account: str,
    token: str,
    question: str,
    semantic_view: str,
) -> dict:
    """
    Call the Cortex Analyst REST API using a semantic view.
    Returns dict with keys: sql, text, suggestions, raw_response, latency
    """
    url = f"https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message"

    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
    }

    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": question}],
            }
        ],
        "semantic_view": semantic_view,
    }

    start = time.time()
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=120)
        latency = time.time() - start
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        latency = time.time() - start
        return {"sql": None, "text": None, "suggestions": None, "error": str(e), "latency": latency}

    # Parse response
    result = {"sql": None, "text": None, "suggestions": None, "latency": latency, "error": None}

    message = data.get("message", {})
    for content_block in message.get("content", []):
        block_type = content_block.get("type", "")
        if block_type == "sql":
            result["sql"] = content_block.get("statement", "")
        elif block_type == "text":
            result["text"] = content_block.get("text", "")
        elif block_type == "suggestions":
            result["suggestions"] = content_block.get("suggestions", [])

    return result


# =============================================================================
# Scoring functions
# =============================================================================
def score_sql_correctness(
    conn,
    expected_sql: str,
    generated_sql: Optional[str],
) -> dict:
    """
    Compare results of expected vs generated SQL.
    Returns scoring details.
    """
    result = {
        "expected_executed": False,
        "generated_executed": False,
        "results_match": False,
        "row_count_match": False,
        "expected_row_count": 0,
        "generated_row_count": 0,
        "error": "",
    }

    if not generated_sql:
        result["error"] = "No SQL generated"
        return result

    # Run expected SQL
    expected_df = execute_sql(conn, expected_sql)
    if expected_df is not None:
        result["expected_executed"] = True
        result["expected_row_count"] = len(expected_df)
    else:
        result["error"] = "Expected SQL failed to execute"
        return result

    # Run generated SQL
    generated_df = execute_sql(conn, generated_sql)
    if generated_df is not None:
        result["generated_executed"] = True
        result["generated_row_count"] = len(generated_df)
    else:
        result["error"] = "Generated SQL failed to execute"
        return result

    # Compare row counts
    result["row_count_match"] = len(expected_df) == len(generated_df)

    # Compare actual results (normalize for comparison)
    try:
        # Sort both DataFrames by all columns for consistent comparison
        expected_sorted = expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True)
        generated_sorted = generated_df.sort_values(by=list(generated_df.columns)).reset_index(drop=True)

        # Convert to comparable types
        for col in expected_sorted.columns:
            expected_sorted[col] = expected_sorted[col].astype(str)
        for col in generated_sorted.columns:
            generated_sorted[col] = generated_sorted[col].astype(str)

        # Hash-based comparison (handles column name differences)
        expected_hash = hashlib.md5(expected_sorted.to_csv(index=False).encode()).hexdigest()
        generated_hash = hashlib.md5(generated_sorted.to_csv(index=False).encode()).hexdigest()
        result["results_match"] = expected_hash == generated_hash

        # If hashes don't match, try value-only comparison (ignore column names)
        if not result["results_match"] and result["row_count_match"]:
            if expected_sorted.shape == generated_sorted.shape:
                expected_vals = expected_sorted.values.tolist()
                generated_vals = generated_sorted.values.tolist()
                expected_vals.sort()
                generated_vals.sort()
                result["results_match"] = expected_vals == generated_vals

    except Exception as e:
        result["error"] = f"Comparison error: {e}"

    return result


def _jaccard(set_a: set, set_b: set) -> float:
    """Jaccard similarity between two sets. Returns 1.0 if both are empty."""
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    if not union:
        return 1.0
    return len(set_a & set_b) / len(union)


def _get_cte_names(ast) -> set[str]:
    """Extract CTE alias names from a SQL AST so we can exclude them from table lists."""
    cte_names = set()
    for cte in ast.find_all(exp.CTE):
        alias = cte.args.get("alias")
        if alias:
            cte_names.add(alias.name.upper() if hasattr(alias, "name") else str(alias).upper())
    return cte_names


def _extract_real_tables(ast) -> set[str]:
    """Extract physical table names, excluding CTE aliases and subquery aliases."""
    cte_names = _get_cte_names(ast)
    tables = set()
    for t in ast.find_all(exp.Table):
        name = t.name.upper() if t.name else ""
        if name and name not in cte_names:
            tables.add(name)
    return tables


def _normalize_agg_func(agg_sql: str) -> str:
    """Normalize an aggregate function string for comparison.
    Strips table/alias qualifiers so SUM(t.col) matches SUM(col)."""
    import re
    s = agg_sql.upper().strip()
    # Remove table qualifiers inside function args: SUM(alias.col) -> SUM(col)
    s = re.sub(r'(\w+)\.(\w+)', r'\2', s)
    return s


def _extract_filter_predicates(ast) -> set[str]:
    """Extract individual filter predicates from WHERE clauses.
    Splits AND-connected conditions so we can compare them as sets
    rather than monolithic strings."""
    predicates = set()
    for where in ast.find_all(exp.Where):
        _collect_predicates(where.this, predicates)
    return predicates


def _collect_predicates(node, predicates: set):
    """Recursively split AND nodes into individual predicates."""
    if isinstance(node, exp.And):
        _collect_predicates(node.left, predicates)
        _collect_predicates(node.right, predicates)
    else:
        # Normalize: uppercase, strip outer parens, collapse whitespace
        import re
        sql = node.sql(dialect="snowflake").upper().strip()
        sql = re.sub(r'\s+', ' ', sql)
        predicates.add(sql)


def _extract_group_columns(ast) -> set[str]:
    """Extract GROUP BY column names (leaf column references only)."""
    cols = set()
    for group in ast.find_all(exp.Group):
        for col in group.find_all(exp.Column):
            if col.name:
                cols.add(col.name.upper())
    return cols


def score_sql_parameters(expected_sql: str, generated_sql: Optional[str]) -> dict:
    """
    Compare SQL structure at the parameter level using AST parsing.
    Checks 6 dimensions: tables, columns, filters, aggregations, joins, ordering.

    Improvements over the naive version:
      - CTE aliases are excluded from table comparison (only real tables count)
      - Jaccard similarity gives partial credit per dimension (0.0 – 1.0)
      - Filters are split into individual predicates and compared as sets
      - Aggregation comparison normalises away table qualifiers
      - Weighted overall score (tables/columns count more than ordering)
    """
    result = {
        "tables_match": False,
        "columns_match": False,
        "filters_match": False,
        "aggregations_match": False,
        "joins_match": False,
        "ordering_match": False,
        # Partial scores (0.0 – 1.0)
        "tables_score": 0.0,
        "columns_score": 0.0,
        "filters_score": 0.0,
        "aggregations_score": 0.0,
        "joins_score": 0.0,
        "ordering_score": 0.0,
        "accuracy": 0.0,
        "details": "",
    }

    if not generated_sql or not expected_sql:
        result["details"] = "Missing SQL for comparison"
        return result

    try:
        expected_ast = sqlglot.parse(expected_sql, dialect="snowflake")[0]
        generated_ast = sqlglot.parse(generated_sql, dialect="snowflake")[0]
    except Exception as e:
        result["details"] = f"SQL parse error: {e}"
        return result

    details = []

    # 1. Tables — exclude CTE aliases, compare only physical tables
    expected_tables = _extract_real_tables(expected_ast)
    generated_tables = _extract_real_tables(generated_ast)
    result["tables_score"] = _jaccard(expected_tables, generated_tables)
    result["tables_match"] = expected_tables == generated_tables
    if not result["tables_match"]:
        missing = expected_tables - generated_tables
        extra = generated_tables - expected_tables
        parts = []
        if missing:
            parts.append(f"missing: {missing}")
        if extra:
            parts.append(f"extra: {extra}")
        details.append(f"Tables ({result['tables_score']:.0%}): {'; '.join(parts)}")

    # 2. Columns — Jaccard partial credit
    expected_cols = {c.name.upper() for c in expected_ast.find_all(exp.Column) if c.name}
    generated_cols = {c.name.upper() for c in generated_ast.find_all(exp.Column) if c.name}
    result["columns_score"] = _jaccard(expected_cols, generated_cols)
    result["columns_match"] = expected_cols == generated_cols
    if not result["columns_match"]:
        missing = expected_cols - generated_cols
        extra = generated_cols - expected_cols
        parts = []
        if missing:
            parts.append(f"missing: {missing}")
        if extra:
            parts.append(f"extra: {extra}")
        details.append(f"Columns ({result['columns_score']:.0%}): {'; '.join(parts)}")

    # 3. Filters — split into individual predicates, compare as sets
    expected_preds = _extract_filter_predicates(expected_ast)
    generated_preds = _extract_filter_predicates(generated_ast)
    result["filters_score"] = _jaccard(expected_preds, generated_preds)
    result["filters_match"] = expected_preds == generated_preds
    if not result["filters_match"]:
        missing = expected_preds - generated_preds
        extra = generated_preds - expected_preds
        parts = []
        if missing:
            parts.append(f"missing: {len(missing)} predicate(s)")
        if extra:
            parts.append(f"extra: {len(extra)} predicate(s)")
        details.append(f"Filters ({result['filters_score']:.0%}): {'; '.join(parts)}")

    # 4. Aggregations — normalize qualifiers, compare function names + GROUP BY cols
    expected_aggs = {_normalize_agg_func(a.sql(dialect="snowflake")) for a in expected_ast.find_all(exp.AggFunc)}
    generated_aggs = {_normalize_agg_func(a.sql(dialect="snowflake")) for a in generated_ast.find_all(exp.AggFunc)}
    agg_score = _jaccard(expected_aggs, generated_aggs)

    expected_group_cols = _extract_group_columns(expected_ast)
    generated_group_cols = _extract_group_columns(generated_ast)
    group_score = _jaccard(expected_group_cols, generated_group_cols)

    # Combined: 60% agg functions, 40% group by columns
    result["aggregations_score"] = round(0.6 * agg_score + 0.4 * group_score, 4)
    result["aggregations_match"] = (expected_aggs == generated_aggs) and (expected_group_cols == generated_group_cols)
    if not result["aggregations_match"]:
        parts = []
        if expected_aggs != generated_aggs:
            parts.append(f"agg functions differ ({agg_score:.0%})")
        if expected_group_cols != generated_group_cols:
            parts.append(f"GROUP BY differs ({group_score:.0%})")
        details.append(f"Aggregations ({result['aggregations_score']:.0%}): {'; '.join(parts)}")

    # 5. Joins — compare join count and types
    expected_joins = sorted([j.sql(dialect="snowflake").upper() for j in expected_ast.find_all(exp.Join)])
    generated_joins = sorted([j.sql(dialect="snowflake").upper() for j in generated_ast.find_all(exp.Join)])
    if not expected_joins and not generated_joins:
        result["joins_score"] = 1.0
    elif expected_joins == generated_joins:
        result["joins_score"] = 1.0
    else:
        # Partial: compare join count similarity
        max_joins = max(len(expected_joins), len(generated_joins))
        matching = sum(1 for e, g in zip(expected_joins, generated_joins) if e == g)
        result["joins_score"] = matching / max_joins if max_joins > 0 else 1.0
    result["joins_match"] = expected_joins == generated_joins
    if not result["joins_match"]:
        details.append(f"Joins ({result['joins_score']:.0%}): expected {len(expected_joins)}, got {len(generated_joins)}")

    # 6. Ordering (ORDER BY + LIMIT)
    expected_order = [o.sql(dialect="snowflake").upper() for o in expected_ast.find_all(exp.Order)]
    generated_order = [o.sql(dialect="snowflake").upper() for o in generated_ast.find_all(exp.Order)]
    expected_limit = [l.sql(dialect="snowflake").upper() for l in expected_ast.find_all(exp.Limit)]
    generated_limit = [l.sql(dialect="snowflake").upper() for l in generated_ast.find_all(exp.Limit)]
    order_match = expected_order == generated_order
    limit_match = expected_limit == generated_limit
    result["ordering_score"] = (0.7 * float(order_match) + 0.3 * float(limit_match))
    result["ordering_match"] = order_match and limit_match
    if not result["ordering_match"]:
        parts = []
        if not order_match:
            parts.append("ORDER BY differs")
        if not limit_match:
            parts.append("LIMIT differs")
        details.append(f"Ordering ({result['ordering_score']:.0%}): {'; '.join(parts)}")

    # Weighted overall accuracy
    # Tables and columns are the most important, filters/aggs next, joins/ordering less so
    weights = {
        "tables": 0.20,
        "columns": 0.25,
        "filters": 0.20,
        "aggregations": 0.20,
        "joins": 0.10,
        "ordering": 0.05,
    }
    weighted_sum = (
        weights["tables"] * result["tables_score"]
        + weights["columns"] * result["columns_score"]
        + weights["filters"] * result["filters_score"]
        + weights["aggregations"] * result["aggregations_score"]
        + weights["joins"] * result["joins_score"]
        + weights["ordering"] * result["ordering_score"]
    )
    result["accuracy"] = round(weighted_sum * 100, 1)
    result["details"] = " | ".join(details) if details else "All parameters match"

    return result


def score_instruction_compliance(generated_sql: Optional[str], question: str) -> dict:
    """
    Check whether generated SQL follows the VBB instruction rules.

    Rules checked:
      1. Timezone conversion — dates must use CONVERT_TIMEZONE('UTC','America/New_York',…)
      2. JOIN type — should use LEFT JOIN, not LEFT OUTER JOIN
      3. ROAS column — ROAS calculations must use gross_less_discount_no_vat_normalized
      4. Revenue column — default revenue should be net_revenue_no_vat_normalized
      5. Google filter — Google platform filters should use LOWER(platform…) = 'google'
    """
    result = {
        "timezone": False,
        "join_type": True,  # innocent until proven guilty
        "roas_column": True,  # True = N/A or correct
        "revenue_column": True,
        "google_filter": True,
        "score": 0.0,
        "details": "",
    }

    if not generated_sql:
        result["details"] = "No SQL generated"
        result["score"] = 0.0
        return result

    sql_upper = generated_sql.upper()
    details = []

    # ── 1. Timezone conversion ──────────────────────────────────────────
    # If the query references conversion_date it must wrap it with CONVERT_TIMEZONE
    references_date = "CONVERSION_DATE" in sql_upper
    has_tz_convert = "CONVERT_TIMEZONE" in sql_upper
    if references_date:
        result["timezone"] = has_tz_convert
        if not has_tz_convert:
            details.append("Missing CONVERT_TIMEZONE on date field")
    else:
        result["timezone"] = True  # N/A — no date field

    # ── 2. JOIN type ────────────────────────────────────────────────────
    if "LEFT OUTER JOIN" in sql_upper:
        result["join_type"] = False
        details.append("Uses LEFT OUTER JOIN instead of LEFT JOIN")

    # ── 3. ROAS column ─────────────────────────────────────────────────
    # If the query or question involves ROAS, the revenue side must use
    # gross_less_discount_no_vat_normalized (not gross_revenue_no_vat_normalized)
    question_lower = question.lower()
    is_roas_query = any(kw in question_lower for kw in ["roas", "return on ad spend"])
    is_roas_sql = "ROAS" in sql_upper or (
        "GROSS" in sql_upper and "WEIGHTED_SPEND" in sql_upper and "/" in sql_upper
    )

    if is_roas_query or is_roas_sql:
        uses_correct = "GROSS_LESS_DISCOUNT_NO_VAT_NORMALIZED" in sql_upper
        uses_wrong = (
            "GROSS_REVENUE_NO_VAT_NORMALIZED" in sql_upper
            and "GROSS_LESS_DISCOUNT" not in sql_upper
        )
        if uses_wrong:
            result["roas_column"] = False
            details.append("ROAS uses gross_revenue instead of gross_less_discount")
        elif uses_correct:
            result["roas_column"] = True
        # else: can't determine, default True (benefit of doubt)

    # ── 4. Revenue column ──────────────────────────────────────────────
    # Default revenue = net_revenue_no_vat_normalized
    # Only flag if it uses gross_revenue for plain "revenue" (not ROAS)
    is_revenue_query = any(kw in question_lower for kw in [
        "revenue", "total revenue", "attributed revenue",
    ]) and not is_roas_query
    if is_revenue_query:
        uses_net = "NET_REVENUE_NO_VAT_NORMALIZED" in sql_upper
        uses_gross_only = (
            "GROSS_REVENUE_NO_VAT_NORMALIZED" in sql_upper
            and "NET_REVENUE" not in sql_upper
        )
        if uses_gross_only:
            result["revenue_column"] = False
            details.append("Revenue uses gross_revenue instead of net_revenue")
        elif uses_net:
            result["revenue_column"] = True

    # ── 5. Google filter ───────────────────────────────────────────────
    is_google_query = "google" in question_lower
    if is_google_query and "GOOGLE" in sql_upper:
        # Check for LOWER() wrapping any platform-like column
        has_lower_pattern = bool(
            re.search(r"LOWER\s*\([^)]*PLATFORM[^)]*\)\s*=\s*'GOOGLE'", sql_upper)
        )
        # Also accept direct string comparison (case-insensitive search)
        has_direct_pattern = bool(
            re.search(r"PLATFORM\w*\s*=\s*'google'", generated_sql, re.IGNORECASE)
        )
        result["google_filter"] = has_lower_pattern or has_direct_pattern
        if not result["google_filter"]:
            details.append("Google filter missing LOWER() or direct comparison")

    # ── Overall score ──────────────────────────────────────────────────
    checks = [
        result["timezone"],
        result["join_type"],
        result["roas_column"],
        result["revenue_column"],
        result["google_filter"],
    ]
    result["score"] = round(sum(checks) / len(checks) * 100, 1)
    result["details"] = " | ".join(details) if details else "All rules followed"

    return result


def detect_hallucinations(conn, generated_sql: Optional[str]) -> dict:
    """
    Check if generated SQL references columns/tables that don't exist.
    Uses Snowflake's DESCRIBE to validate.
    """
    result = {"is_hallucination": False, "details": ""}

    if not generated_sql:
        return result

    # Try to compile/explain the query without executing
    try:
        cursor = conn.cursor()
        cursor.execute(f"EXPLAIN {generated_sql}")
        return result  # If EXPLAIN works, no hallucination
    except Exception as e:
        error_str = str(e)
        if any(keyword in error_str.lower() for keyword in [
            "does not exist", "invalid identifier", "unknown column",
            "object does not exist", "ambiguous column"
        ]):
            result["is_hallucination"] = True
            result["details"] = error_str
        return result


def score_nl_quality(question: str, nl_response: Optional[str], generated_sql: Optional[str]) -> dict:
    """
    Score natural language response quality on a 0-5 scale.
    Aligned with VBB response instructions:
      - Provide data-driven answers with relevant metrics
      - Include specific numbers and percentages
      - Specify time period for any metrics
      - Suggest actionable recommendations when relevant
      - Format with clear structure: key metrics first, then details, then insights

    0: No response or complete failure
    1: Response exists but is irrelevant
    2: Partially relevant but misleading
    3: Relevant but incomplete or imprecise
    4: Good response with minor issues
    5: Excellent, accurate, and complete
    """
    result = {
        "score": 0,
        "notes": "",
        "mentions_time_period": False,
        "is_structured": False,
        "has_recommendations": False,
        "has_specific_numbers": False,
    }

    if not nl_response:
        result["notes"] = "No natural language response returned"
        return result

    nl_lower = nl_response.lower()
    score = 3  # baseline
    notes = []

    # ── Appropriate refusal ─────────────────────────────────────────────
    cant_answer_phrases = ["cannot", "can't", "unable to", "don't have", "not able",
                           "i need more information", "more context"]
    if any(phrase in nl_lower for phrase in cant_answer_phrases):
        if not generated_sql:
            score = 4  # appropriate refusal
            notes.append("Appropriately declined to answer")
        else:
            score = 2  # contradictory
            notes.append("Contradictory: claims inability but generated SQL")

    # ── Response length ─────────────────────────────────────────────────
    if len(nl_response) < 20:
        score = min(score, 2)
        notes.append("Response too brief")

    # ── SQL generated alongside NL ─────────────────────────────────────
    if generated_sql and not notes:
        score = 4
        notes.append("SQL generated with explanation")

    # ── Enhanced checks (aligned to VBB response instructions) ─────────

    # 1. Time period mentioned?
    time_keywords = [
        "month", "quarter", "week", "year", "day", "period",
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
        "q1", "q2", "q3", "q4", "last 3", "last 6", "ytd", "mtd",
        "2024", "2025", "2026",
    ]
    result["mentions_time_period"] = any(kw in nl_lower for kw in time_keywords)

    # 2. Structured response? (bullet points, numbered lists, headers)
    structure_indicators = ["\n-", "\n*", "\n1.", "\n2.", "**", "##", "| "]
    result["is_structured"] = any(ind in nl_response for ind in structure_indicators)

    # 3. Actionable recommendations?
    recommendation_phrases = [
        "recommend", "suggest", "consider", "opportunity", "optimize",
        "improve", "increase", "decrease", "focus on", "allocate",
        "shift", "invest", "reduce", "scale",
    ]
    result["has_recommendations"] = any(ph in nl_lower for ph in recommendation_phrases)

    # 4. Specific numbers / percentages?
    has_numbers = bool(re.search(r'\$[\d,]+\.?\d*|\d+\.?\d*%|\d{1,3}(,\d{3})+', nl_response))
    result["has_specific_numbers"] = has_numbers

    # ── Bonus points for instruction compliance ────────────────────────
    if generated_sql:
        bonus = 0
        if result["mentions_time_period"]:
            bonus += 0.25
        if result["is_structured"]:
            bonus += 0.25
        if result["has_specific_numbers"]:
            bonus += 0.25
        if result["has_recommendations"]:
            bonus += 0.25
        score = min(5, score + bonus)

    result["score"] = round(score, 1)
    result["notes"] = "; ".join(notes) if notes else "Baseline score"
    return result


# =============================================================================
# Main eval pipeline
# =============================================================================
def run_eval(questions: list[GoldenQuestion]) -> list[EvalResult]:
    """Run the full evaluation pipeline."""

    conn = get_snowflake_connection()
    token = get_session_token(conn)
    results = []

    for i, q in enumerate(questions):
        logger.info(f"[{i+1}/{len(questions)}] Evaluating: {q.question[:80]}...")

        eval_result = EvalResult(
            question_id=q.id,
            question=q.question,
            category=q.category,
            difficulty=q.difficulty,
            expected_sql=q.expected_sql,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        # 1. Call Cortex Analyst
        api_response = call_cortex_analyst(
            account=SNOWFLAKE_ACCOUNT,
            token=token,
            question=q.question,
            semantic_view=SEMANTIC_VIEW,
        )

        eval_result.generated_sql = api_response.get("sql")
        eval_result.nl_response = api_response.get("text")
        eval_result.latency_seconds = api_response.get("latency", 0)

        if api_response.get("error"):
            eval_result.error_message = api_response["error"]

        # Check for failure (no SQL returned)
        if not eval_result.generated_sql:
            eval_result.is_failure = True
            if api_response.get("suggestions"):
                eval_result.nl_quality_notes = f"Suggestions returned: {api_response['suggestions']}"

        # 2. Score SQL correctness
        if q.expected_sql and eval_result.generated_sql:
            sql_scores = score_sql_correctness(conn, q.expected_sql, eval_result.generated_sql)
            eval_result.expected_executed_successfully = sql_scores["expected_executed"]
            eval_result.sql_executed_successfully = sql_scores["generated_executed"]
            eval_result.results_match = sql_scores["results_match"]
            eval_result.row_count_match = sql_scores["row_count_match"]
            eval_result.expected_row_count = sql_scores["expected_row_count"]
            eval_result.generated_row_count = sql_scores["generated_row_count"]
            if sql_scores["error"]:
                eval_result.error_message += f" | SQL: {sql_scores['error']}"

        # 3. Score SQL parameters (Tool Execution Accuracy)
        if q.expected_sql and eval_result.generated_sql:
            param_scores = score_sql_parameters(q.expected_sql, eval_result.generated_sql)
            eval_result.param_accuracy = param_scores["accuracy"]
            eval_result.param_tables_match = param_scores["tables_match"]
            eval_result.param_columns_match = param_scores["columns_match"]
            eval_result.param_filters_match = param_scores["filters_match"]
            eval_result.param_aggregations_match = param_scores["aggregations_match"]
            eval_result.param_joins_match = param_scores["joins_match"]
            eval_result.param_ordering_match = param_scores["ordering_match"]
            eval_result.param_tables_score = param_scores["tables_score"]
            eval_result.param_columns_score = param_scores["columns_score"]
            eval_result.param_filters_score = param_scores["filters_score"]
            eval_result.param_aggregations_score = param_scores["aggregations_score"]
            eval_result.param_joins_score = param_scores["joins_score"]
            eval_result.param_ordering_score = param_scores["ordering_score"]
            eval_result.param_details = param_scores["details"]

        # 4. Score instruction compliance
        if eval_result.generated_sql:
            compliance = score_instruction_compliance(eval_result.generated_sql, q.question)
            eval_result.compliance_score = compliance["score"]
            eval_result.compliance_timezone = compliance["timezone"]
            eval_result.compliance_join_type = compliance["join_type"]
            eval_result.compliance_roas_column = compliance["roas_column"]
            eval_result.compliance_revenue_column = compliance["revenue_column"]
            eval_result.compliance_google_filter = compliance["google_filter"]
            eval_result.compliance_details = compliance["details"]

        # 5. Detect hallucinations
        if eval_result.generated_sql:
            hallucination = detect_hallucinations(conn, eval_result.generated_sql)
            eval_result.is_hallucination = hallucination["is_hallucination"]
            eval_result.hallucination_details = hallucination["details"]

        # 6. Score NL quality (enhanced with instruction alignment)
        nl_scores = score_nl_quality(q.question, eval_result.nl_response, eval_result.generated_sql)
        eval_result.nl_quality_score = nl_scores["score"]
        eval_result.nl_quality_notes = nl_scores["notes"]
        eval_result.nl_mentions_time_period = nl_scores["mentions_time_period"]
        eval_result.nl_is_structured = nl_scores["is_structured"]
        eval_result.nl_has_recommendations = nl_scores["has_recommendations"]
        eval_result.nl_has_specific_numbers = nl_scores["has_specific_numbers"]

        results.append(eval_result)

        # Small delay to avoid rate limiting
        time.sleep(1)

    conn.close()
    return results


# =============================================================================
# Reporting
# =============================================================================
def generate_report(results: list[EvalResult]) -> pd.DataFrame:
    """Generate a summary report from eval results."""

    df = pd.DataFrame([asdict(r) for r in results])
    total = len(results)

    print("\n" + "=" * 70)
    print("CORTEX ANALYST EVALUATION REPORT")
    print(f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Total questions: {total}")
    print("=" * 70)

    # Overall metrics
    has_expected_sql = df["expected_sql"].str.len() > 0
    evaluated = df[has_expected_sql]
    has_sql = df["generated_sql"].notna() & (df["generated_sql"].str.len() > 0)
    has_params = df["param_accuracy"] > 0

    # 1. SQL Generation Rate
    sql_gen_rate = has_sql.sum() / total * 100

    # 2. SQL Execution Rate (of those that generated SQL)
    sql_exec_rate = evaluated["sql_executed_successfully"].sum() / len(evaluated) * 100 if len(evaluated) > 0 else 0

    # 3. Table Accuracy (avg Jaccard similarity for tables)
    table_accuracy = df.loc[has_params, "param_tables_score"].mean() * 100 if has_params.any() else 0

    # 4. Avg Parameter Similarity (weighted across all dimensions)
    avg_param_sim = df.loc[has_params, "param_accuracy"].mean() if has_params.any() else 0

    # 5. Exact Result Match
    exact_match = evaluated["results_match"].sum() / len(evaluated) * 100 if len(evaluated) > 0 else 0

    # 6. Hallucination Rate
    hallucination_rate = df["is_hallucination"].sum() / total * 100

    # 7. Instruction Compliance
    avg_compliance = df.loc[has_sql, "compliance_score"].mean() if has_sql.any() else 0

    # 8. NL Quality Score
    avg_nl_score = df["nl_quality_score"].mean()

    print(f"\n📊 PERFORMANCE SUMMARY (8 Key Metrics)")
    print(f"  {'Metric':35s} {'Score':>8s}")
    print(f"  {'-'*35} {'-'*8}")
    print(f"  {'1. SQL Generation Rate':35s} {sql_gen_rate:>7.1f}%")
    print(f"  {'2. SQL Execution Rate':35s} {sql_exec_rate:>7.1f}%")
    print(f"  {'3. Table Accuracy':35s} {table_accuracy:>7.1f}%")
    print(f"  {'4. Avg Parameter Similarity':35s} {avg_param_sim:>7.1f}%")
    print(f"  {'5. Exact Result Match':35s} {exact_match:>7.1f}%")
    print(f"  {'6. Hallucination Rate':35s} {hallucination_rate:>7.1f}%")
    print(f"  {'7. Instruction Compliance':35s} {avg_compliance:>7.1f}%")
    print(f"  {'8. NL Quality Score':35s} {avg_nl_score:>5.1f}/5")

    # By category
    if df["category"].str.len().any():
        print(f"\n📂 BY CATEGORY")
        cat_summary = df.groupby("category").agg(
            count=("question_id", "count"),
            accuracy=("results_match", "mean"),
            failures=("is_failure", "sum"),
            avg_latency=("latency_seconds", "mean"),
        ).round(3)
        print(cat_summary.to_string())

    # By difficulty
    if df["difficulty"].str.len().any():
        print(f"\n🎯 BY DIFFICULTY")
        diff_summary = df.groupby("difficulty").agg(
            count=("question_id", "count"),
            accuracy=("results_match", "mean"),
            failures=("is_failure", "sum"),
            avg_latency=("latency_seconds", "mean"),
        ).round(3)
        print(diff_summary.to_string())

    # Parameter accuracy (Tool Execution Accuracy)
    if has_params.any():
        avg_param = df.loc[has_params, "param_accuracy"].mean()
        print(f"\n🔧 PARAMETER ACCURACY (Tool Execution Accuracy)")
        print(f"  Overall Weighted Accuracy:     {avg_param:.1f}%")
        print(f"  Weights: Tables 20% | Columns 25% | Filters 20% | Aggs 20% | Joins 10% | Order 5%")

        # Show both exact match % and partial (Jaccard) %
        param_dims = {
            "Tables":       ("param_tables_match", "param_tables_score"),
            "Columns":      ("param_columns_match", "param_columns_score"),
            "Filters":      ("param_filters_match", "param_filters_score"),
            "Aggregations": ("param_aggregations_match", "param_aggregations_score"),
            "Joins":        ("param_joins_match", "param_joins_score"),
            "Ordering":     ("param_ordering_match", "param_ordering_score"),
        }
        print(f"  {'Dimension':15s} {'Exact Match':>12s}  {'Similarity':>12s}")
        print(f"  {'-'*15} {'-'*12}  {'-'*12}")
        for label, (match_col, score_col) in param_dims.items():
            exact_pct = df.loc[has_params, match_col].mean() * 100
            sim_pct = df.loc[has_params, score_col].mean() * 100
            print(f"    {label:15s} {exact_pct:>10.0f}%  {sim_pct:>10.0f}%")

        # Show mismatches for each question
        mismatches = df[has_params & (df["param_accuracy"] < 100)]
        if len(mismatches) > 0:
            print(f"\n  Mismatches:")
            for _, row in mismatches.iterrows():
                print(f"    Q{row['question_id']} ({row['param_accuracy']:.0f}%): {row['param_details']}")

    # Instruction compliance
    if has_sql.any():
        print(f"\n📋 INSTRUCTION COMPLIANCE")
        print(f"  Overall Compliance Score:      {avg_compliance:.1f}%")
        compliance_rules = {
            "Timezone Conv.":  "compliance_timezone",
            "JOIN Type":       "compliance_join_type",
            "ROAS Column":     "compliance_roas_column",
            "Revenue Column":  "compliance_revenue_column",
            "Google Filter":   "compliance_google_filter",
        }
        for label, col in compliance_rules.items():
            pct = df.loc[has_sql, col].mean() * 100
            print(f"    {label:18s} {pct:.0f}%")

        # Show non-compliant questions
        non_compliant = df[has_sql & (df["compliance_score"] < 100)]
        if len(non_compliant) > 0:
            print(f"\n  Non-compliant queries:")
            for _, row in non_compliant.iterrows():
                print(f"    Q{row['question_id']} ({row['compliance_score']:.0f}%): {row['compliance_details']}")

    # NL Response Quality (enhanced)
    print(f"\n💬 NL RESPONSE QUALITY")
    print(f"  Avg NL Quality Score:          {avg_nl_score:.1f}/5")
    if has_sql.any():
        nl_dims = {
            "Mentions Time Period": "nl_mentions_time_period",
            "Structured Format":   "nl_is_structured",
            "Has Recommendations":  "nl_has_recommendations",
            "Specific Numbers":     "nl_has_specific_numbers",
        }
        for label, col in nl_dims.items():
            pct = df[col].mean() * 100
            print(f"    {label:22s} {pct:.0f}%")

    # Failures & hallucinations detail
    failures = df[df["is_failure"]]
    if len(failures) > 0:
        print(f"\n❌ FAILURES ({len(failures)}):")
        for _, row in failures.iterrows():
            print(f"  Q{row['question_id']}: {row['question'][:60]}...")

    hallucinations = df[df["is_hallucination"]]
    if len(hallucinations) > 0:
        print(f"\n⚠️  HALLUCINATIONS ({len(hallucinations)}):")
        for _, row in hallucinations.iterrows():
            print(f"  Q{row['question_id']}: {row['hallucination_details'][:80]}...")

    print("\n" + "=" * 70)
    return df


# =============================================================================
# Entry point
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Cortex Analyst Evaluation Pipeline")
    parser.add_argument(
        "--name",
        required=True,
        help="Short name for this eval run, e.g. 'baseline' or 'updated_roas_docs'",
    )
    args = parser.parse_args()

    name = args.name.strip().replace(" ", "_")

    # Guard: fail fast on duplicate name
    if check_duplicate_name(name):
        logger.error(
            f"A run named '{name}' already exists in the manifest. "
            "Choose a different name or check results/manifest.csv."
        )
        return

    logger.info(f"Starting Cortex Analyst evaluation — run: '{name}'")

    # Load golden questions
    questions = load_golden_questions(GOLDEN_QUESTIONS_FILE)

    if not questions:
        logger.error("No questions loaded. Check your CSV file.")
        return

    # Run evaluation
    results = run_eval(questions)

    # Generate report
    df = generate_report(results)

    # Save detailed results + update manifest
    output_path = save_results(df, name)
    save_manifest(df, name, output_path)

    logger.info(f"Evaluation complete. Results: {output_path}")


if __name__ == "__main__":
    main()