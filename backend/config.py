"""Centralized configuration loaded from environment variables / .env file."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _clean_prefix(prefix: str) -> str:
    p = (prefix or "").strip().lstrip("/")
    return p if (not p or p.endswith("/")) else f"{p}/"


@dataclass(frozen=True)
class Settings:
    team_id: str = os.getenv("TEAM_ID", "team01")
    team_seed: int = int(os.getenv("TEAM_SEED", "417"))

    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    kinesis_stream: str = os.getenv("KINESIS_STREAM", "airline-events")
    s3_bucket: str = os.getenv("S3_BUCKET", "airline-p3-team01-REPLACE-ME")
    raw_events_prefix: str = _clean_prefix(os.getenv("RAW_EVENTS_PREFIX", "raw/events/"))
    star_schema_prefix: str = _clean_prefix(os.getenv("STAR_SCHEMA_PREFIX", "star/"))
    athena_results_prefix: str = _clean_prefix(os.getenv("ATHENA_RESULTS_PREFIX", "athena-results/"))
    athena_database: str = os.getenv("ATHENA_DATABASE", "airline_star")
    athena_workgroup: str = os.getenv("ATHENA_WORKGROUP", "primary")

    sqlite_db_path: str = os.getenv("SQLITE_DB_PATH", str(PROJECT_ROOT / "data" / "airline.db"))
    ref_data_dir: str = os.getenv("REF_DATA_DIR", str(PROJECT_ROOT / "ref"))

    kinesis_flush_interval_sec: float = float(os.getenv("KINESIS_FLUSH_INTERVAL_SEC", "5.0"))
    kinesis_flush_max_records: int = int(os.getenv("KINESIS_FLUSH_MAX_RECORDS", "2500"))
    kinesis_start_minutes_ago: int = int(os.getenv("KINESIS_START_MINUTES_AGO", "15"))
    live_max_retention_minutes: int = int(os.getenv("LIVE_MAX_RETENTION_MINUTES", "60"))

    dry_run_no_aws: bool = os.getenv("DRY_RUN_NO_AWS", "false").strip().lower() in ("1", "true", "yes")

    @property
    def raw_events_uri(self) -> str:
        return f"s3://{self.s3_bucket}/{self.raw_events_prefix}"

    @property
    def star_schema_uri(self) -> str:
        return f"s3://{self.s3_bucket}/{self.star_schema_prefix}"

    @property
    def athena_output_uri(self) -> str:
        return f"s3://{self.s3_bucket}/{self.athena_results_prefix}"


def get_settings() -> Settings:
    return Settings()
