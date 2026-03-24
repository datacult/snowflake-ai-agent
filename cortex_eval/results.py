import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import RESULTS_DIR

logger = logging.getLogger(__name__)

MANIFEST_FILE = "manifest.csv"
MANIFEST_COLUMNS = [
    "name", "timestamp", "total_questions",
    "sql_generation_rate", "sql_execution_rate", "exact_match",
    "param_accuracy", "compliance_score", "hallucination_rate", "nl_quality_score",
    "output_file",
]


def _results_dir() -> Path:
    """Return the results directory, creating it if necessary."""
    path = Path(RESULTS_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path


def check_duplicate_name(name: str) -> bool:
    """Return True if this name already exists in the manifest."""
    manifest_path = _results_dir() / MANIFEST_FILE
    if not manifest_path.exists():
        return False
    with open(manifest_path, newline="") as f:
        reader = csv.DictReader(f)
        return any(row.get("name") == name for row in reader)


def save_results(df: pd.DataFrame, name: str) -> Path:
    """Save detailed per-question results to a named CSV file."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = f"{name}_{date_str}.csv"
    output_path = _results_dir() / filename
    df.to_csv(output_path, index=False)
    logger.info(f"Results saved to {output_path}")
    return output_path


def save_manifest(df: pd.DataFrame, name: str, output_path: Path):
    """Append a one-row summary of this run to manifest.csv."""
    manifest_path = _results_dir() / MANIFEST_FILE
    total = len(df)
    has_sql = df["generated_sql"].notna() & (df["generated_sql"].str.len() > 0)
    has_expected = df["expected_sql"].str.len() > 0
    evaluated = df[has_expected]
    has_params = df["param_accuracy"] > 0

    row = {
        "name": name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_questions": total,
        "sql_generation_rate": round(has_sql.sum() / total * 100, 1) if total else 0,
        "sql_execution_rate": round(evaluated["sql_executed_successfully"].sum() / len(evaluated) * 100, 1) if len(evaluated) else 0,
        "exact_match": round(evaluated["results_match"].sum() / len(evaluated) * 100, 1) if len(evaluated) else 0,
        "param_accuracy": round(df.loc[has_params, "param_accuracy"].mean(), 1) if has_params.any() else 0,
        "compliance_score": round(df.loc[has_sql, "compliance_score"].mean(), 1) if has_sql.any() else 0,
        "hallucination_rate": round(df["is_hallucination"].sum() / total * 100, 1) if total else 0,
        "nl_quality_score": round(df["nl_quality_score"].mean(), 2),
        "output_file": output_path.name,
    }

    file_exists = manifest_path.exists()
    with open(manifest_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    logger.info(f"Manifest updated: {manifest_path}")