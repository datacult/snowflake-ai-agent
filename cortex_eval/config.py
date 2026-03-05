"""
Configuration for Cortex Analyst Eval Pipeline
================================================
Update these values with your Snowflake credentials.
"""

# =============================================================================
# Snowflake connection
# =============================================================================
SNOWFLAKE_ACCOUNT = "your-account-id"       # e.g. "abc12345-us-east-1"
SNOWFLAKE_USER = "your-username"
SNOWFLAKE_PASSWORD = "your-password"
SNOWFLAKE_ROLE = "ACCOUNTADMIN"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "ANALYTICS"
SNOWFLAKE_SCHEMA = "REPORTING"

# =============================================================================
# Cortex Analyst — Semantic View
# =============================================================================
SEMANTIC_VIEW = "ANALYTICS.REPORTING.AGENT_MARKETING_ANALYTICS"

# =============================================================================
# Golden Questions — Local CSV file
# =============================================================================
# Expected columns: id, question, expected_sql, category, difficulty
GOLDEN_QUESTIONS_FILE = "golden_answers.csv"

# =============================================================================
# Output
# =============================================================================
RESULTS_OUTPUT_PATH = "results/eval_results.csv"
