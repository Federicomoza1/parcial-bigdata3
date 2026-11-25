"""Unified Kinesis stream consumer feeding:
1. In-memory sliding-window Live View counted strictly by `event_ts` (R3)
2. Gzipped JSONL micro-batches in the S3 Landing Zone within seconds (R4)

Using a single shared-throughput consumer (no Enhanced Fan-Out, R11) avoids
Kinesis GetRecords throttling (5 calls/sec/shard) and eliminates Firehose cost/IAM issues.
"""
from __future__ import annotations

import collections
import gzip
import json
import logging
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("airline.consumer")


def parse_event_ts_epoch(ts_str: str) -> Optional[float]:
    """Parses ISO-8601 UTC timestamp string (e.g. 2025-01-15T12:30:00Z) into epoch seconds."""
    if not ts_str or not isinstance(ts_str, str):
        return None
    s = ts_str.strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt_obj = datetime.fromisoformat(s)
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        return dt_obj.timestamp()
    except Exception:
        return None


class LiveWindowTracker:
    """Thread-safe event-time counter for GET /analytics/live?window=N (R3).

    - Counts strictly by `event_ts` (when the event happened), NEVER arrival time.
    - Deduplicates by `event_id` so stream retries or dirty replay duplicates aren't counted twice.
    - Automatically ignores old replay events (e.g. 2025-01..2025-03) and prunes expired entries.
    """

    def __init__(self, max_retention_minutes: int = 60, max_seen_ids: int = 100_000) -> None:
        self.max_retention_sec = max(max_retention_minutes, 10) * 60
        self.max_seen_ids = max_seen_ids
        self._lock = threading.Lock()
        # Each entry: (event_ts_epoch, event_type, event_id)
        self._events: List[Tuple[float, str, str]] = []
        self._seen_ids: Set[str] = set()
        self._seen_order: Deque[str] = collections.deque()

    def ingest(self, event: Dict[str, Any], now_epoch: Optional[float] = None) -> bool:
        event_id = event.get("event_id")
        event_type = event.get("event_type")
        ts_epoch = parse_event_ts_epoch(event.get("event_ts", ""))
        if not event_id or not event_type or ts_epoch is None:
            return False
        if event_type not in ("search_performed", "reservation_created", "reservation_cancelled"):
            return False

        now = now_epoch if now_epoch is not None else time.time()
        # Fast path discard for historical replay events older than max_retention_sec
        if ts_epoch < (now - self.max_retention_sec):
            return False
        # Also ignore absurdly far-future corrupted timestamps (> 10 minutes ahead of wall clock)
        if ts_epoch > (now + 600):
            return False

        with self._lock:
            if event_id in self._seen_ids:
                return False
            self._seen_ids.add(event_id)
            self._seen_order.append(event_id)
            if len(self._seen_order) > self.max_seen_ids:
                oldest = self._seen_order.popleft()
                self._seen_ids.discard(oldest)

            self._events.append((ts_epoch, event_type, event_id))
            if len(self._events) > 10_000:
                self._prune_locked(now)
        return True

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self.max_retention_sec
        self._events = [item for item in self._events if item[0] >= cutoff]

    def snapshot(self, window_minutes: int = 5, now_epoch: Optional[float] = None) -> Dict[str, Any]:
        w_min = max(1, int(window_minutes))
        now = now_epoch if now_epoch is not None else time.time()
        lower = now - (w_min * 60.0)
        upper = now + 300.0  # allow minor clock skew

        searches = 0
        reservations = 0
        cancellations = 0

        with self._lock:
            self._prune_locked(now)
            for ts_epoch, etype, _ in self._events:
                if lower <= ts_epoch <= upper:
                    if etype == "search_performed":
                        searches += 1
                    elif etype == "reservation_created":
                        reservations += 1
                    elif etype == "reservation_cancelled":
                        cancellations += 1

        conversion = (reservations / searches) if searches > 0 else 0.0
        return {
            "window_minutes": w_min,
            "searches": searches,
            "reservations": reservations,
            "cancellations": cancellations,
            "conversion": round(conversion, 6),
        }


class S3LandingFlusher:
    """Buffers raw JSON records from Kinesis and writes gzipped JSONL objects to S3 (R4).

    Append-only landing zone (`s3://<bucket>/raw/events/dt=YYYY-MM-DD/batch-...jsonl.gz`).
    """

    def __init__(
        self,
        s3_bucket: str,
        raw_prefix: str,
        region_name: str = "us-east-1",
        flush_interval_sec: float = 5.0,
        flush_max_records: int = 2500,
        dry_run: bool = False,
    ) -> None:
        self.s3_bucket = s3_bucket
        self.raw_prefix = raw_prefix if raw_prefix.endswith("/") else f"{raw_prefix}/"
        self.flush_interval_sec = flush_interval_sec
        self.flush_max_records = flush_max_records
        self.dry_run = dry_run

        self._lock = threading.Lock()
        self._buffer: List[str] = []
        self._last_flush = time.monotonic()
        self._s3 = None if dry_run else boto3.client("s3", region_name=region_name)
        self.flushed_records = 0
        self.flushed_objects = 0

    def add_raw_line(self, json_line: str) -> None:
        line = json_line.strip()
        if not line:
            return
        to_flush: Optional[List[str]] = None
        with self._lock:
            self._buffer.append(line)
            if len(self._buffer) >= self.flush_max_records:
                to_flush = self._buffer
                self._buffer = []
                self._last_flush = time.monotonic()
        if to_flush:
            self._upload_lines(to_flush)

    def flush_if_due(self, force: bool = False) -> None:
        to_flush: Optional[List[str]] = None
        now_m = time.monotonic()
        with self._lock:
            if self._buffer and (force or (now_m - self._last_flush) >= self.flush_interval_sec):
                to_flush = self._buffer
                self._buffer = []
                self._last_flush = now_m
        if to_flush:
            self._upload_lines(to_flush)

    def _upload_lines(self, lines: List[str]) -> None:
        if self.dry_run:
            self.flushed_records += len(lines)
            self.flushed_objects += 1
            return

        assert self._s3 is not None
        now_utc = datetime.now(timezone.utc)
        day_str = now_utc.strftime("%Y-%m-%d")
        ms = int(now_utc.timestamp() * 1000)
        key = f"{self.raw_prefix}dt={day_str}/events-{ms}-{uuid.uuid4().hex[:8]}.jsonl.gz"
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        compressed = gzip.compress(payload, compresslevel=6)

        for attempt in range(5):
            try:
                self._s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=key,
                    Body=compressed,
                    ContentType="application/x-ndjson",
                    ContentEncoding="gzip",
                )
                self.flushed_records += len(lines)
                self.flushed_objects += 1
                logger.info("Flushed %d events to s3://%s/%s (%d B)", len(lines), self.s3_bucket, key, len(compressed))
                return
            except Exception as exc:
                logger.warning("S3 put_object attempt %d failed for %s: %s", attempt + 1, key, exc)
                time.sleep(0.5 * (2 ** attempt))

        # Re-queue lines on persistent transient failure so R4 never loses records
        with self._lock:
            self._buffer = lines + self._buffer
        logger.error("Re-buffered %d events after S3 upload retries failed", len(lines))


class KinesisStreamConsumer:
    """Polls Kinesis shards using standard shared throughput (0 Enhanced Fan-Out consumers, R11)."""

    def __init__(
        self,
        stream_name: str,
        live_tracker: LiveWindowTracker,
        s3_flusher: S3LandingFlusher,
        region_name: str = "us-east-1",
        start_minutes_ago: int = 15,
        dry_run: bool = False,
    ) -> None:
        self.stream_name = stream_name
        self.live_tracker = live_tracker
        self.s3_flusher = s3_flusher
        self.region_name = region_name
        self.start_minutes_ago = max(1, start_minutes_ago)
        self.dry_run = dry_run

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._kinesis = None if dry_run else boto3.client("kinesis", region_name=region_name)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        if not self.dry_run:
            self._thread = threading.Thread(target=self._run_loop, name="kinesis-consumer", daemon=True)
            self._thread.start()
        logger.info("KinesisStreamConsumer started (stream=%s, dry_run=%s)", self.stream_name, self.dry_run)

    def stop(self, timeout: float = 10.0) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        self.s3_flusher.flush_if_due(force=True)

    def ingest_raw_bytes(self, data: bytes) -> None:
        """Processes a single Kinesis record payload (supports single JSON or JSON lines)."""
        text = data.decode("utf-8", errors="replace").strip()
        if not text:
            return
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            self.s3_flusher.add_raw_line(line)
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    self.live_tracker.ingest(obj)
            except Exception:
                pass

    def _init_shard_iterators(self) -> Dict[str, str]:
        assert self._kinesis is not None
        iterators: Dict[str, str] = {}
        start_ts = datetime.now(timezone.utc) - timedelta(minutes=self.start_minutes_ago)
        shards = self._kinesis.list_shards(StreamName=self.stream_name).get("Shards", [])
        for sh in shards:
            shard_id = sh["ShardId"]
            try:
                it = self._kinesis.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="AT_TIMESTAMP",
                    Timestamp=start_ts,
                )["ShardIterator"]
            except Exception:
                it = self._kinesis.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="LATEST",
                )["ShardIterator"]
            iterators[shard_id] = it
        return iterators

    def _run_loop(self) -> None:
        assert self._kinesis is not None
        iterators: Dict[str, str] = {}
        last_shard_refresh = 0.0

        while not self._stop_event.is_set():
            try:
                now_m = time.monotonic()
                if not iterators or (now_m - last_shard_refresh) > 120.0:
                    try:
                        current_shards = self._kinesis.list_shards(StreamName=self.stream_name).get("Shards", [])
                        if not iterators:
                            iterators = self._init_shard_iterators()
                        else:
                            for sh in current_shards:
                                sid = sh["ShardId"]
                                if sid not in iterators:
                                    iterators[sid] = self._kinesis.get_shard_iterator(
                                        StreamName=self.stream_name,
                                        ShardId=sid,
                                        ShardIteratorType="TRIM_HORIZON",
                                    )["ShardIterator"]
                        last_shard_refresh = now_m
                    except ClientError as ce:
                        code = ce.response.get("Error", {}).get("Code", "")
                        if code == "ResourceNotFoundException":
                            logger.warning("Kinesis stream '%s' not found yet; retrying in 5s...", self.stream_name)
                            time.sleep(5.0)
                            continue
                        raise

                behind_ms_max = 0
                records_read = 0

                for shard_id, it in list(iterators.items()):
                    if not it:
                        iterators.pop(shard_id, None)
                        continue
                    try:
                        resp = self._kinesis.get_records(ShardIterator=it, Limit=1000)
                        next_it = resp.get("NextShardIterator")
                        if next_it:
                            iterators[shard_id] = next_it
                        else:
                            iterators.pop(shard_id, None)

                        behind_ms_max = max(behind_ms_max, int(resp.get("MillisBehindLatest", 0)))
                        recs = resp.get("Records", [])
                        records_read += len(recs)
                        for rec in recs:
                            self.ingest_raw_bytes(rec["Data"])
                    except ClientError as ce:
                        code = ce.response.get("Error", {}).get("Code", "")
                        if code == "ProvisionedThroughputExceededException":
                            # Back off politely when sharing shard read quota with pytest harness
                            time.sleep(0.6 + random.uniform(0.1, 0.4))
                        elif code == "ExpiredIteratorException":
                            iterators[shard_id] = self._kinesis.get_shard_iterator(
                                StreamName=self.stream_name,
                                ShardId=shard_id,
                                ShardIteratorType="LATEST",
                            )["ShardIterator"]
                        elif code == "ResourceNotFoundException":
                            iterators.clear()
                            break
                        else:
                            logger.warning("Kinesis get_records error on %s: %s", shard_id, ce)
                            time.sleep(1.0)

                self.s3_flusher.flush_if_due(force=False)

                # Adaptive polling: fast (0.35s) during 20k replay catchup, gentle (0.9s) when caught up
                if records_read > 0 or behind_ms_max > 1000:
                    time.sleep(0.35)
                else:
                    time.sleep(0.90)

            except Exception as exc:
                logger.warning("Consumer loop error: %s", exc)
                time.sleep(2.0)
