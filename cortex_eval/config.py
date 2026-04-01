"""
Configuration for Cortex Analyst Eval Pipeline
================================================
Update these values with your Snowflake credentials.
"""
import os
# =============================================================================
# Snowflake connection
# =============================================================================
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")

# =============================================================================
# Cortex Analyst — Semantic View
# =============================================================================
SEMANTIC_VIEW = os.getenv("SEMANTIC_VIEW")

# =============================================================================
# Golden Questions — Local CSV file
# =============================================================================
# Expected columns: id, question, expected_sql, category, difficulty
GOLDEN_QUESTIONS_FILE = "golden_answers.csv"

# =============================================================================
# Output
# =============================================================================
RESULTS_DIR ="results"