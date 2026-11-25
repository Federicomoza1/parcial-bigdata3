"""Provisions and configures all AWS resources required for Parcial 3 (R5, R10, R11):
1. S3 bucket (landing zone, star schema Parquet, Athena query results)
2. Kinesis Data Stream: PROVISIONED mode, 1 shard, 24h retention, 0 enhanced fan-out consumers (R11)
3. Glue Data Catalog database (`airline_star`)
4. Athena Workgroup (`primary`): BytesScannedCutoffPerQuery = 900 MB (<= 1 GB limit for R10) + OutputLocation
5. Initial Star Schema bootstrap (creates all 5 Parquet tables in Glue so they exist prior to pytest)

Usage:
    python scripts/aws_up.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.config import get_settings  # noqa: E402
from etl.star_builder import StarSchemaAWSJob  # noqa: E402


def ensure_s3_bucket(s3, bucket: str, region: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"[OK] S3 bucket exists: s3://{bucket}")
        return
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("403", "AccessDenied"):
            raise RuntimeError(
                f"Bucket '{bucket}' is owned by another AWS account! Choose a unique S3_BUCKET in .env."
            ) from exc

    print(f"[..] Creating S3 bucket: s3://{bucket} ({region})...")
    kwargs = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)
    print(f"[OK] Created S3 bucket: s3://{bucket}")


def ensure_minimal_kinesis_stream(kinesis, stream_name: str) -> None:
    """Creates 1-shard PROVISIONED Kinesis stream with 24h retention & 0 EFO consumers (R11)."""
    exists = False
    try:
        desc = kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
        exists = True
        status = desc["StreamStatus"]
        print(f"[OK] Kinesis stream '{stream_name}' found (status={status})")
    except kinesis.exceptions.ResourceNotFoundException:
        pass

    if not exists:
        print(f"[..] Creating Kinesis stream '{stream_name}' (PROVISIONED, 1 shard)...")
        kinesis.create_stream(
            StreamName=stream_name,
            ShardCount=1,
            StreamModeDetails={"StreamMode": "PROVISIONED"},
        )

    # Wait until ACTIVE
    for _ in range(60):
        desc = kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
        if desc["StreamStatus"] == "ACTIVE":
            break
        time.sleep(2)
    else:
        raise TimeoutError(f"Kinesis stream '{stream_name}' did not become ACTIVE in 120s")

    desc = kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
    mode = desc.get("StreamModeDetails", {}).get("StreamMode", "PROVISIONED")
    if mode != "PROVISIONED":
        print("[..] Switching Kinesis stream mode back to PROVISIONED...")
        kinesis.update_stream_mode(
            StreamARN=desc["StreamARN"],
            StreamModeDetails={"StreamMode": "PROVISIONED"},
        )
        time.sleep(5)

    retention = int(desc.get("RetentionPeriodHours", 24))
    if retention > 24:
        print(f"[..] Decreasing retention from {retention}h to 24h (R11)...")
        kinesis.decrease_stream_retention_period(
            StreamName=stream_name,
            RetentionPeriodHours=24,
        )

    # Ensure 0 Enhanced Fan-Out consumers (R11)
    try:
        consumers = kinesis.list_stream_consumers(StreamARN=desc["StreamARN"]).get("Consumers", [])
        for c in consumers:
            print(f"[..] Deregistering Enhanced Fan-Out consumer {c['ConsumerName']} (R11)...")
            kinesis.deregister_stream_consumer(ConsumerARN=c["ConsumerARN"])
    except Exception:
        pass

    final_desc = kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
    print(
        f"[OK] Kinesis stream '{stream_name}' ready: mode={final_desc.get('StreamModeDetails', {}).get('StreamMode')}, "
        f"open_shards={final_desc['OpenShardCount']}, retention={final_desc['RetentionPeriodHours']}h"
    )


def configure_athena_workgroup(athena, workgroup: str, output_uri: str, cutoff_bytes: int = 900_000_000) -> None:
    """Configures Athena workgroup per-query scan limit <= 1 GB (1,000,000,000 bytes, R10)."""
    print(f"[..] Configuring Athena workgroup '{workgroup}' (cutoff={cutoff_bytes:,} B, output={output_uri})...")
    try:
        athena.get_work_group(WorkGroup=workgroup)
        athena.update_work_group(
            WorkGroup=workgroup,
            State="ENABLED",
            ConfigurationUpdates={
                "BytesScannedCutoffPerQuery": cutoff_bytes,
                "EnforceWorkGroupConfiguration": False,
                "ResultConfigurationUpdates": {"OutputLocation": output_uri},
            },
        )
    except ClientError as exc:
        if "not found" in str(exc).lower() or "InvalidRequestException" in str(exc):
            athena.create_work_group(
                Name=workgroup,
                Configuration={
                    "BytesScannedCutoffPerQuery": cutoff_bytes,
                    "EnforceWorkGroupConfiguration": False,
                    "ResultConfiguration": {"OutputLocation": output_uri},
                },
            )
        else:
            raise
    print(f"[OK] Athena workgroup '{workgroup}' configured with {cutoff_bytes:,} bytes cutoff (<= 1 GB)")


def main() -> None:
    s = get_settings()
    if "REPLACE-ME" in s.s3_bucket:
        print("ERROR: Please set a unique S3_BUCKET name in .env before running aws_up.py!")
        sys.exit(1)

    session = boto3.Session(region_name=s.aws_region)
    ensure_s3_bucket(session.client("s3"), s.s3_bucket, s.aws_region)
    ensure_minimal_kinesis_stream(session.client("kinesis"), s.kinesis_stream)
    configure_athena_workgroup(session.client("athena"), s.athena_workgroup, s.athena_output_uri)

    print("[..] Bootstrapping Glue Data Catalog & initial Star Schema tables...")
    job = StarSchemaAWSJob(
        s3_bucket=s.s3_bucket,
        raw_prefix=s.raw_events_prefix,
        star_prefix=s.star_schema_prefix,
        glue_database=s.athena_database,
        seed=s.team_seed,
        ref_dir=s.ref_data_dir,
        region_name=s.aws_region,
    )
    stats = job.run()
    print(f"[OK] Star Schema tables registered in Glue database '{s.athena_database}': {stats}")
    print("\nAll AWS infrastructure for Parcial 3 is READY!")


if __name__ == "__main__":
    main()
