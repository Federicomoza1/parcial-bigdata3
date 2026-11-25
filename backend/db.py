"""Transactional SQLite store (WAL mode) for flights and reservations.

Supports concurrent reads during bursts (R2: 50 threads / 200 searches) and
serialized atomic seat allocation / cancellation via BEGIN IMMEDIATE.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class SeatUnavailableError(Exception):
    """Raised when a flight has no available seats left."""


class FlightNotFoundError(Exception):
    """Raised when a referenced flight_id does not exist."""


class ReservationNotFoundError(Exception):
    """Raised when a referenced reservation_id does not exist."""


class AlreadyCancelledError(Exception):
    """Raised when attempting to cancel an already cancelled reservation."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class AirlineDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=15.0, isolation_level=None, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=15000;")
            conn.execute("PRAGMA foreign_keys=ON;")
            self._local.conn = conn
        return conn

    @contextmanager
    def _write_tx(self):
        with self._write_lock:
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE;")
            try:
                yield conn
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise

    def _init_schema(self) -> None:
        with self._write_tx() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS flights (
                    flight_id TEXT PRIMARY KEY,
                    route TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    flight_date TEXT NOT NULL,
                    capacity INTEGER NOT NULL CHECK (capacity >= 0),
                    seats_booked INTEGER NOT NULL DEFAULT 0 CHECK (seats_booked >= 0),
                    price_cents INTEGER NOT NULL CHECK (price_cents >= 0),
                    created_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_flights_search
                ON flights (origin, destination, flight_date);
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reservations (
                    reservation_id TEXT PRIMARY KEY,
                    flight_id TEXT NOT NULL REFERENCES flights(flight_id),
                    route TEXT NOT NULL,
                    flight_date TEXT NOT NULL,
                    passenger_name TEXT NOT NULL,
                    fare_class TEXT NOT NULL,
                    price_cents INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    cancelled_at TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reservations_flight
                ON reservations (flight_id);
                """
            )

    def upsert_flight(
        self,
        flight_id: str,
        route: str,
        flight_date: str,
        capacity: int,
        price_cents: int,
    ) -> Dict[str, Any]:
        parts = route.split("-")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"Invalid route format '{route}', expected 'AAA-BBB'")
        origin, destination = parts[0].upper(), parts[1].upper()
        norm_route = f"{origin}-{destination}"
        now = _utc_now_iso()

        with self._write_tx() as conn:
            conn.execute(
                """
                INSERT INTO flights (
                    flight_id, route, origin, destination, flight_date,
                    capacity, seats_booked, price_cents, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
                ON CONFLICT(flight_id) DO UPDATE SET
                    route = excluded.route,
                    origin = excluded.origin,
                    destination = excluded.destination,
                    flight_date = excluded.flight_date,
                    capacity = excluded.capacity,
                    price_cents = excluded.price_cents;
                """,
                (flight_id, norm_route, origin, destination, flight_date, int(capacity), int(price_cents), now),
            )
            row = conn.execute("SELECT * FROM flights WHERE flight_id = ?", (flight_id,)).fetchone()
        return self._flight_dict(row)

    def search_flights(self, origin: str, destination: str, flight_date: str) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT * FROM flights
            WHERE origin = ? AND destination = ? AND flight_date = ?
            ORDER BY flight_id ASC;
            """,
            (origin.upper(), destination.upper(), flight_date),
        ).fetchall()
        return [self._flight_dict(r) for r in rows]

    def create_reservation(
        self,
        flight_id: str,
        passenger_name: str,
        fare_class: str = "ECONOMY",
    ) -> Dict[str, Any]:
        fc = (fare_class or "ECONOMY").upper()
        if fc not in ("ECONOMY", "BUSINESS"):
            raise ValueError(f"Unsupported fare_class '{fare_class}'")

        with self._write_tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE flight_id = ?", (flight_id,)).fetchone()
            if flight is None:
                raise FlightNotFoundError(flight_id)

            cur = conn.execute(
                """
                UPDATE flights
                SET seats_booked = seats_booked + 1
                WHERE flight_id = ? AND seats_booked < capacity;
                """,
                (flight_id,),
            )
            if cur.rowcount == 0:
                raise SeatUnavailableError(flight_id)

            base_price = int(flight["price_cents"])
            final_price = int(round(base_price * 2.2)) if fc == "BUSINESS" else base_price
            reservation_id = f"res-{uuid.uuid4().hex[:12]}"
            now = _utc_now_iso()

            conn.execute(
                """
                INSERT INTO reservations (
                    reservation_id, flight_id, route, flight_date,
                    passenger_name, fare_class, price_cents, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'CONFIRMED', ?);
                """,
                (
                    reservation_id,
                    flight_id,
                    flight["route"],
                    flight["flight_date"],
                    passenger_name,
                    fc,
                    final_price,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
        return self._reservation_dict(row)

    def get_reservation(self, reservation_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
        ).fetchone()
        return self._reservation_dict(row) if row else None

    def cancel_reservation(self, reservation_id: str) -> Dict[str, Any]:
        with self._write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
            if row is None:
                raise ReservationNotFoundError(reservation_id)
            if row["status"] == "CANCELLED":
                raise AlreadyCancelledError(reservation_id)

            now = _utc_now_iso()
            conn.execute(
                """
                UPDATE reservations
                SET status = 'CANCELLED', cancelled_at = ?
                WHERE reservation_id = ?;
                """,
                (now, reservation_id),
            )
            conn.execute(
                """
                UPDATE flights
                SET seats_booked = CASE WHEN seats_booked > 0 THEN seats_booked - 1 ELSE 0 END
                WHERE flight_id = ?;
                """,
                (row["flight_id"],),
            )
            updated = conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?", (reservation_id,)
            ).fetchone()
        return self._reservation_dict(updated)

    @staticmethod
    def _flight_dict(row: sqlite3.Row) -> Dict[str, Any]:
        cap = int(row["capacity"])
        booked = int(row["seats_booked"])
        return {
            "flight_id": row["flight_id"],
            "route": row["route"],
            "origin": row["origin"],
            "destination": row["destination"],
            "flight_date": row["flight_date"],
            "capacity": cap,
            "seats_booked": booked,
            "seats_available": max(0, cap - booked),
            "price_cents": int(row["price_cents"]),
        }

    @staticmethod
    def _reservation_dict(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "reservation_id": row["reservation_id"],
            "flight_id": row["flight_id"],
            "route": row["route"],
            "flight_date": row["flight_date"],
            "passenger_name": row["passenger_name"],
            "fare_class": row["fare_class"],
            "price_cents": int(row["price_cents"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "cancelled_at": row["cancelled_at"],
        }
