"""
Configuration for Cortex Analyst Eval Pipeline
================================================
Update these values with your Snowflake credentials.
"""

# =============================================================================
# Snowflake connection
# =============================================================================
SNOWFLAKE_ACCOUNT = "*******"       # e.g. "abc12345-us-east-1"
SNOWFLAKE_USER = "*****"
SNOWFLAKE_PASSWORD = "****"
SNOWFLAKE_ROLE = "*****"
SNOWFLAKE_WAREHOUSE = "AI_COMPUTE_WH"
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
RESULTS_DIR ="results"