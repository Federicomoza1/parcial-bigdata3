r"""Interactive live demo against your deployed AWS system!

Shows both paths working end-to-end in real time:
1. FAST PATH (Seconds): Creates a flight, performs 5 searches, 2 reservations, and 1 cancellation
   against your EC2 FastAPI backend, then watches `GET /analytics/live?window=5` update from Kinesis!
2. ANALYTICAL PATH (Athena Star Schema): Runs live SQL queries (Q1, Q2, Q4, Q5 SCD2, Q8) on Amazon Athena
   against `airline_star` and prints the formatted results in your terminal!

Usage:
    .\.venv\Scripts\python.exe scripts/demo_live.py
"""
from __future__ import annotations

import sys
import time
import uuid
from pathlib import Path

import boto3
import httpx
import yaml

ROOT = Path(__file__).resolve().parent.parent
CFG_PATH = ROOT / "parcial3-harness-estudiantes" / "config.yaml"


def run_athena_sql(athena, database: str, workgroup: str, output_uri: str, sql: str):
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_uri},
    )["QueryExecutionId"]
    while True:
        qe = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        st = qe["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.8)
    if st != "SUCCEEDED":
        raise RuntimeError(qe["Status"].get("StateChangeReason"))

    res = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    headers = [c.get("VarCharValue", "") for c in res[0]["Data"]]
    rows = [[c.get("VarCharValue", "") for c in r["Data"]] for r in res[1:]]
    scanned = int(qe.get("Statistics", {}).get("DataScannedInBytes", 0))
    return headers, rows, scanned


def print_table(title: str, headers: list[str], rows: list[list[str]], scanned_bytes: int) -> None:
    print(f"\n--- {title} (Athena escaneo: {scanned_bytes:,} bytes) ---")
    widths = [len(h) for h in headers]
    for r in rows:
        for i, val in enumerate(r):
            widths[i] = max(widths[i], len(str(val)))
    fmt = " | ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("-+-".join("-" * w for w in widths))
    for r in rows:
        print(fmt.format(*r))


def main() -> None:
    cfg = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))
    base = cfg["backend"]["base_url"].rstrip("/")
    aws_cfg = cfg["aws"]

    print("=" * 78)
    print(f" DEMO EN VIVO - AIRLINE RESERVATION SYSTEM ({base})")
    print("=" * 78)

    with httpx.Client(timeout=15.0) as client:
        before = client.get(f"{base}/analytics/live?window=5").json()
        print(f"\n[1] Estado inicial de Live View (ventana 5 min): {before}")

        flight_id = f"DEMO-{uuid.uuid4().hex[:6].upper()}"
        flight_resp = client.post(
            f"{base}/admin/flights",
            json={
                "flight_id": flight_id,
                "route": "BOG-CTG",
                "flight_date": "2026-12-15",
                "capacity": 10,
                "price_cents": 350000,
            },
        ).json()
        print(f"\n[2] Vuelo creado: {flight_resp['flight_id']} ({flight_resp['route']}, cupos={flight_resp['seats_available']})")

        print("[3] Simulando 5 busquedas de usuarios hacia BOG-CTG...")
        for i in range(5):
            r = client.get(
                f"{base}/flights/search",
                params={"origin": "BOG", "destination": "CTG", "date": "2026-12-15"},
                headers={"X-Session-Id": f"demo-user-{i+1}"},
            )
            r.raise_for_status()

        print("[4] Creando 2 reservas (1 ECONOMY y 1 BUSINESS) y cancelando 1...")
        r1 = client.post(
            f"{base}/reservations",
            json={"flight_id": flight_id, "passenger_name": "Ana Gomez", "fare_class": "ECONOMY"},
        ).json()
        r2 = client.post(
            f"{base}/reservations",
            json={"flight_id": flight_id, "passenger_name": "Carlos Ruiz", "fare_class": "BUSINESS"},
        ).json()
        print(f"    -> Reserva 1 creada: {r1['reservation_id']} ({r1['fare_class']}, ${r1['price_cents']/100:,.2f})")
        print(f"    -> Reserva 2 creada: {r2['reservation_id']} ({r2['fare_class']}, ${r2['price_cents']/100:,.2f})")

        cx = client.post(f"{base}/reservations/{r1['reservation_id']}/cancel").json()
        print(f"    -> Reserva 1 cancelada: estado={cx['status']} (silla liberada)")

        print("\n[5] Esperando ~2.5s a que el consumidor lea los eventos desde Amazon Kinesis...")
        time.sleep(2.5)
        after = client.get(f"{base}/analytics/live?window=5").json()
        print(f"    -> Live View actualizada: {after}")
        print(
            f"    -> Deltas en tiempo real: +{after['searches'] - before['searches']} busquedas, "
            f"+{after['reservations'] - before['reservations']} reservas, "
            f"+{after['cancellations'] - before['cancellations']} cancelaciones!"
        )

    print("\n" + "=" * 78)
    print(" CONSULTANDO EL ESQUEMA ESTRELLA EN AMAZON ATHENA (airline_star)")
    print("=" * 78)
    athena = boto3.client("athena", region_name=aws_cfg["region"])
    db = aws_cfg["athena_database"]
    wg = aws_cfg["athena_workgroup"]
    out = aws_cfg["athena_output"]

    h, rows, b = run_athena_sql(
        athena,
        db,
        wg,
        out,
        """
        SELECT r.route, SUM(f.bookings) AS reservas, SUM(f.cancellations) AS cancelaciones,
               SUM(f.revenue_cents) / 100 AS ingreso_neto_usd
        FROM fact_booking_event f
        JOIN dim_route r ON f.route_key = r.route_key
        GROUP BY r.route
        ORDER BY ingreso_neto_usd DESC
        LIMIT 5
        """,
    )
    print_table("Q4: Top 5 Rutas por Ingreso Neto", h, rows, b)

    h, rows, b = run_athena_sql(
        athena,
        db,
        wg,
        out,
        """
        SELECT d.year, d.month, r.route_category, SUM(f.revenue_cents) / 100 AS ingreso_neto_usd
        FROM fact_booking_event f
        JOIN dim_route r ON f.route_key = r.route_key
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE f.event_id LIKE 'rp-%'
        GROUP BY d.year, d.month, r.route_category
        ORDER BY d.year, d.month, r.route_category
        """,
    )
    print_table("Q5: Ingreso Mensual por Categoria SCD Tipo 2 (vigente al dia del evento)", h, rows, b)


if __name__ == "__main__":
    main()
