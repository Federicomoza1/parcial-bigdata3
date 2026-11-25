"""Strict Kinesis event builders conforming to contract/EVENTS.md (R1).

Rules enforced:
- Common envelope: event_id (UUID), event_type, event_ts (UTC YYYY-MM-DDTHH:MM:SSZ), schema_version=1
- Partition key: session_id for search_performed; flight_id for reservation_created / reservation_cancelled
- Zero PII: passenger_name and any other personal data are NEVER included.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

SCHEMA_VERSION = 1


def utc_event_ts(dt_obj: datetime | None = None) -> str:
    """Formats UTC timestamp strictly as YYYY-MM-DDTHH:MM:SSZ (no fractional seconds)."""
    now = dt_obj.astimezone(timezone.utc) if dt_obj else datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


def encode_event(event: Dict[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def build_search_performed(
    session_id: str,
    origin: str,
    destination: str,
    travel_date: str,
    results_count: int,
    event_ts: str | None = None,
) -> Tuple[str, Dict[str, Any]]:
    sid = session_id or f"sess-{uuid.uuid4().hex}"
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "search_performed",
        "event_ts": event_ts or utc_event_ts(),
        "schema_version": SCHEMA_VERSION,
        "session_id": sid,
        "origin": origin.upper(),
        "destination": destination.upper(),
        "travel_date": travel_date,
        "results_count": int(results_count),
    }
    return sid, event


def build_reservation_event(
    event_type: str,
    reservation_id: str,
    flight_id: str,
    route: str,
    flight_date: str,
    fare_class: str,
    price_cents: int,
    event_ts: str | None = None,
) -> Tuple[str, Dict[str, Any]]:
    if event_type not in ("reservation_created", "reservation_cancelled"):
        raise ValueError(f"Invalid reservation event_type: {event_type}")
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": event_ts or utc_event_ts(),
        "schema_version": SCHEMA_VERSION,
        "reservation_id": str(reservation_id),
        "flight_id": str(flight_id),
        "route": str(route).upper(),
        "flight_date": str(flight_date),
        "fare_class": str(fare_class).upper(),
        "price_cents": int(price_cents),
    }
    return str(flight_id), event
