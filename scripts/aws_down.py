"""Deletes billable hourly streaming resources (Kinesis Data Stream) when pausing work.

Protects your $50 AWS Academy Learner Lab budget ($0.015/hour per provisioned shard).
S3 objects and Glue Data Catalog metadata remain intact at near-zero cost so you can
recreate the stream in seconds with `python scripts/aws_up.py` before your next session.

Usage:
    python scripts/aws_down.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.config import get_settings  # noqa: E402


def delete_kinesis_stream(region: str, stream_name: str) -> None:
    kinesis = boto3.client("kinesis", region_name=region)
    try:
        kinesis.describe_stream_summary(StreamName=stream_name)
    except kinesis.exceptions.ResourceNotFoundException:
        print(f"[OK] Kinesis stream '{stream_name}' is already deleted.")
        return

    print(f"[..] Deleting Kinesis stream '{stream_name}' to stop hourly shard billing...")
    kinesis.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)

    for _ in range(30):
        try:
            kinesis.describe_stream_summary(StreamName=stream_name)
            time.sleep(2)
        except kinesis.exceptions.ResourceNotFoundException:
            print(f"[OK] Kinesis stream '{stream_name}' deleted.")
            return
    print(f"[WARN] Stream '{stream_name}' deletion still in progress.")


if __name__ == "__main__":
    s = get_settings()
    delete_kinesis_stream(s.aws_region, s.kinesis_stream)
