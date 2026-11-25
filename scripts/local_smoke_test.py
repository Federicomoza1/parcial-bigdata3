"""Offline verification suite: validates R1-R3 API/Live View logic and R5-R9 Q1-Q8 Star Schema math
against the official `StarTruth` oracle from `parcial3-harness-estudiantes` WITHOUT needing AWS!

Usage:
    python scripts/local_smoke_test.py --seed 417
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HARNESS_DIR = ROOT / "parcial3-harness-estudiantes"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HARNESS_DIR))

from fastapi.testclient import TestClient  # noqa: E402

from airline_data.events import StarTruth, calendar_dates  # noqa: E402
from backend.config import Settings  # noqa: E402
from backend.main import create_app  # noqa: E402
from etl.star_builder import transform_events_to_star  # noqa: E402


def verify_api_and_live_view(seed: int, tmp_dir: str) -> None:
    print("=== [1/2] Verifying FastAPI + Burst Queue + Event-Time Live View (R1, R2, R3) ===")
    settings = Settings(
        team_id="smoke",
        team_seed=seed,
        sqlite_db_path=os.path.join(tmp_dir, "smoke.db"),
        ref_data_dir=os.path.join(tmp_dir, "ref"),
        dry_run_no_aws=True,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        r = client.post(
            "/admin/flights",
            json={
                "flight_id": "HSMOKE001",
                "route": "BOG-MDE",
                "flight_date": "2031-03-10",
                "capacity": 20,
                "price_cents": 120000,
            },
        )
        assert r.status_code == 201, r.text

        # Burst 200 searches across 20 threads
        codes = []
        lock = threading.Lock()

        def burst_worker(count: int):
            for _ in range(count):
                resp = client.get(
                    "/flights/search",
                    params={"origin": "BOG", "destination": "MDE", "date": "2031-03-10"},
                    headers={"X-Session-Id": f"smoke-{uuid.uuid4().hex}"},
                )
                with lock:
                    codes.append(resp.status_code)

        threads = [threading.Thread(target=burst_worker, args=(10,)) for _ in range(20)]
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.perf_counter() - t0
        assert set(codes) == {200}

        # Create 3 reservations + 1 cancellation (+ check 409 double cancel)
        res = []
        for i in range(3):
            rr = client.post(
                "/reservations",
                json={"flight_id": "HSMOKE001", "passenger_name": f"Pax {i}", "fare_class": "ECONOMY"},
            )
            assert rr.status_code == 201
            res.append(rr.json())

        cx = client.post(f"/reservations/{res[0]['reservation_id']}/cancel")
        assert cx.status_code == 200 and cx.json()["status"] == "CANCELLED"
        cx_dup = client.post(f"/reservations/{res[0]['reservation_id']}/cancel")
        assert cx_dup.status_code == 409

        # Wait briefly for async publisher -> local consumer sink
        time.sleep(0.3)
        live = client.get("/analytics/live?window=5").json()
        assert live["searches"] == 200, live
        assert live["reservations"] == 3, live
        assert live["cancellations"] == 1, live
        assert abs(live["conversion"] - (3 / 200)) < 1e-5, live

        # Feed old 2025 event -> must NOT pollute live window!
        container = app.state.container
        container.live_tracker.ingest(
            {
                "event_id": "rp-old-1",
                "event_type": "search_performed",
                "event_ts": "2025-01-15T10:00:00Z",
                "schema_version": 1,
            }
        )
        live_after = client.get("/analytics/live?window=5").json()
        assert live_after["searches"] == 200, "Old replay event polluted live window!"
        print(f"  [PASS] 200 burst searches in {elapsed:.3f}s + reservations/cancels + live view exact!")


def verify_star_schema_against_oracle(seed: int, tmp_dir: str) -> None:
    print(f"=== [2/2] Verifying Star Schema ETL vs StarTruth Oracle (seed={seed}, 20k dirty replay) ===")
    truth = StarTruth(seed=seed, n=20000, salt=0)
    ref_dir = os.path.join(tmp_dir, "ref_oracle")
    star = transform_events_to_star(truth.arrivals, seed=seed, ref_dir=ref_dir)

    dim_date = star["dim_date"].to_pandas()
    dim_route = star["dim_route"].to_pandas()
    dim_fare = star["dim_fare_class"].to_pandas()
    fact_booking = star["fact_booking_event"].to_pandas()

    import pandas as pd

    fact_search = pd.concat(
        [tbl.to_pandas().assign(month=ym) for ym, tbl in star["fact_search_partitions"].items()],
        ignore_index=True,
    )

    # R5: dim_date exact match
    exp_cal = calendar_dates().set_index("date_key")
    assert len(dim_date) == 1826
    for r in dim_date.itertuples(index=False):
        e = exp_cal.loc[r.date_key]
        assert str(r.full_date) == e.full_date
        assert int(r.weekday) == int(e.weekday)
        assert r.weekday_name == e.weekday_name
        assert int(r.quarter) == int(e.quarter)
        assert bool(r.is_holiday) == bool(e.is_holiday)
        assert bool(r.is_long_weekend) == bool(e.is_long_weekend)
        if e.is_holiday:
            assert r.holiday_name != ""

    # R8: Deduplication exact counts
    assert len(fact_search) == truth.n_search_unique
    assert len(fact_booking) == truth.n_booking_unique

    # R7 & R9: Q1 - Q8 verification
    fb = fact_booking.merge(dim_date, on="date_key").merge(dim_route, on="route_key").merge(dim_fare, on="fare_class_key")
    fs = fact_search.merge(dim_date, on="date_key").merge(dim_route, on="route_key")

    q1 = {(int(r.year), int(r.month), r.fare_class): int(r.revenue_cents)
          for r in fb.groupby(["year", "month", "fare_class"]).revenue_cents.sum().reset_index().itertuples()}
    assert q1 == truth.q1(), "Q1 mismatch"

    q2 = {k: (int(v.cancellations), int(v.bookings))
          for k, v in fb.groupby("route")[["cancellations", "bookings"]].sum().iterrows()}
    assert q2 == truth.q2(), "Q2 mismatch"

    q3 = {k: int(v) for k, v in fb.groupby("weekday_name").bookings.sum().items()}
    assert q3 == truth.q3(), "Q3 mismatch"

    q4_df = fb.groupby("route").revenue_cents.sum().reset_index().sort_values(["revenue_cents", "route"], ascending=[False, True]).head(10)
    q4 = [(r.route, int(r.revenue_cents)) for r in q4_df.itertuples()]
    assert q4 == truth.q4(10), "Q4 mismatch"

    q5 = {(r.route_category, int(r.year), int(r.month)): int(r.revenue_cents)
          for r in fb.groupby(["route_category", "year", "month"]).revenue_cents.sum().reset_index().itertuples()}
    assert q5 == truth.q5(), "Q5 SCD2 mismatch"

    s_rt = fs.groupby("route").size()
    b_rt = fb.groupby("route").bookings.sum()
    q6 = {r: (int(s_rt.get(r, 0)), int(b_rt.get(r, 0))) for r in truth.q6()}
    assert q6 == truth.q6(), "Q6 mismatch"

    s_hol = fs.groupby("is_holiday").size()
    b_hol = fb.groupby("is_holiday").bookings.sum()
    q7 = {k: (int(s_hol.get(k, 0)), int(b_hol.get(k, 0))) for k in (False, True)}
    assert q7 == truth.q7(), "Q7 mismatch"

    b_lwe = fb.groupby("is_long_weekend").bookings.sum()
    q8 = {k: int(b_lwe.get(k, 0)) for k in (False, True)}
    assert q8 == truth.q8(), "Q8 mismatch"

    print("  [PASS] dim_date (1,826 rows), SCD2 dim_route, deduplication, and Q1..Q8 match StarTruth 100%!")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=417)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        verify_api_and_live_view(args.seed, tmp_dir)
        verify_star_schema_against_oracle(args.seed, tmp_dir)

    print("\nALL LOCAL SMOKE TESTS PASSED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
