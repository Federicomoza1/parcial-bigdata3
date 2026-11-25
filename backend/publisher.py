"""Asynchronous batch Kinesis publisher (R1, R2).

Decouples HTTP latency from Kinesis network I/O via an unbounded thread-safe queue
and a background daemon worker that coalesces bursts up to 500 records per `put_records`
call, retrying ONLY failed records (`FailedRecordCount > 0`) with exponential backoff + jitter.
"""
from __future__ import annotations

import logging
import queue
import random
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3

from .events import encode_event

logger = logging.getLogger("airline.publisher")


class KinesisEventPublisher:
    def __init__(
        self,
        stream_name: str,
        region_name: str = "us-east-1",
        linger_sec: float = 0.03,
        max_batch_size: int = 500,
        dry_run: bool = False,
        local_sink: Optional[Callable[[str, bytes], None]] = None,
    ) -> None:
        self.stream_name = stream_name
        self.region_name = region_name
        self.linger_sec = linger_sec
        self.max_batch_size = min(max_batch_size, 500)
        self.dry_run = dry_run
        self.local_sink = local_sink

        self._queue: "queue.Queue[Optional[Tuple[str, bytes]]]" = queue.Queue()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._client = None if dry_run else boto3.client("kinesis", region_name=region_name)

        self.published_count = 0
        self.failed_count = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="kinesis-publisher", daemon=True)
        self._thread.start()
        logger.info("KinesisEventPublisher started (stream=%s, dry_run=%s)", self.stream_name, self.dry_run)

    def stop(self, timeout: float = 10.0) -> None:
        self._stop_event.set()
        self._queue.put(None)
        if self._thread:
            self._thread.join(timeout=timeout)

    def publish(self, partition_key: str, event: Dict[str, Any]) -> None:
        """Non-blocking O(1) enqueue so HTTP latency remains unaffected under burst."""
        payload = encode_event(event)
        self._queue.put_nowait((partition_key, payload))

    def _run_loop(self) -> None:
        while True:
            try:
                first = self._queue.get(timeout=0.5)
            except queue.Empty:
                if self._stop_event.is_set():
                    break
                continue

            if first is None:
                # Drain any remaining items before exiting
                batch = self._drain_remaining()
                if batch:
                    self._send_batch_with_retry(batch)
                break

            batch: List[Tuple[str, bytes]] = [first]
            deadline = time.monotonic() + self.linger_sec
            while len(batch) < self.max_batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = self._queue.get(timeout=remaining)
                except queue.Empty:
                    break
                if item is None:
                    self._stop_event.set()
                    break
                batch.append(item)

            self._send_batch_with_retry(batch)

    def _drain_remaining(self) -> List[Tuple[str, bytes]]:
        out: List[Tuple[str, bytes]] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is not None:
                out.append(item)
        return out

    def _send_batch_with_retry(self, batch: List[Tuple[str, bytes]]) -> None:
        for i in range(0, len(batch), 500):
            chunk = batch[i : i + 500]
            self._send_chunk(chunk)

    def _send_chunk(self, chunk: List[Tuple[str, bytes]]) -> None:
        if self.dry_run:
            if self.local_sink:
                for pk, data in chunk:
                    self.local_sink(pk, data)
            self.published_count += len(chunk)
            return

        assert self._client is not None
        pending = list(chunk)
        for attempt in range(10):
            try:
                resp = self._client.put_records(
                    StreamName=self.stream_name,
                    Records=[{"Data": data, "PartitionKey": pk} for pk, data in pending],
                )
                failed_n = int(resp.get("FailedRecordCount", 0))
                if failed_n == 0:
                    self.published_count += len(pending)
                    return

                retry_next: List[Tuple[str, bytes]] = []
                for idx, rec_res in enumerate(resp.get("Records", [])):
                    if "ErrorCode" in rec_res:
                        retry_next.append(pending[idx])
                    else:
                        self.published_count += 1
                pending = retry_next
            except Exception as exc:
                logger.warning("put_records attempt %d failed (%d records): %s", attempt + 1, len(pending), exc)

            sleep_s = min(4.0, (0.15 * (2 ** attempt)) + random.uniform(0.0, 0.15))
            time.sleep(sleep_s)

        self.failed_count += len(pending)
        logger.error("Dropped %d records after exhausting retries to stream %s", len(pending), self.stream_name)
