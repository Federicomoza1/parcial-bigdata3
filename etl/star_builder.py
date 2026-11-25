"""Idempotent Star Schema ETL Builder (R5, R6, R7, R8, R9, R10).

Reads raw JSON/JSONL (.gz) events from the S3 Landing Zone, deduplicates by `event_id`,
resolves event-time dates (`event_ts`) and SCD Type 2 `dim_route` surrogate keys via as-of join,
writes Snappy-compressed Parquet files to S3, and registers tables & partitions in AWS Glue.
"""
from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .reference_data import airport_region, ensure_reference_data

logger = logging.getLogger("airline.etl")

PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

FAR_FUTURE_DATE = dt.date(9999, 12, 31)
CALENDAR_START = dt.date(2023, 1, 1)
CALENDAR_END = dt.date(2027, 12, 31)


def parse_json_records(text: str) -> Iterable[Dict[str, Any]]:
    """Parses both JSON Lines and concatenated JSON streams (e.g. Firehose output)."""
    decoder = json.JSONDecoder()
    for raw_line in text.splitlines():
        s = raw_line.strip()
        if not s:
            continue
        idx = 0
        n = len(s)
        while idx < n:
            while idx < n and s[idx].isspace():
                idx += 1
            if idx >= n:
                break
            try:
                obj, next_idx = decoder.raw_decode(s, idx)
                if isinstance(obj, dict):
                    yield obj
                idx = next_idx
            except json.JSONDecodeError:
                break


# --------------------------------------------------------------------------- Pure Dimension Builders
def build_dim_date_table(holidays_df: pd.DataFrame) -> pa.Table:
    """Builds enriched `dim_date` for every day 2023-01-01..2027-12-31 (exact 1,826 rows, R5)."""
    hol_map: Dict[str, str] = {}
    for row in holidays_df.itertuples(index=False):
        d_str = str(row.date).strip()
        h_name = "" if pd.isna(row.holiday_name) else str(row.holiday_name).strip()
        hol_map[d_str] = h_name

    hol_set = set(hol_map.keys())
    days = pd.date_range(CALENDAR_START, CALENDAR_END, freq="D")

    date_keys: List[int] = []
    full_dates: List[dt.date] = []
    years: List[int] = []
    months: List[int] = []
    quarters: List[int] = []
    weekdays: List[int] = []
    weekday_names: List[str] = []
    is_weekends: List[bool] = []
    is_holidays: List[bool] = []
    holiday_names: List[str] = []
    is_long_weekends: List[bool] = []
    # Extra analytical hierarchy attributes (Section 5)
    month_names: List[str] = []
    day_of_months: List[int] = []
    year_months: List[str] = []

    one_day = pd.Timedelta(days=1)

    for ts in days:
        d_iso = ts.strftime("%Y-%m-%d")
        dow = ts.dayofweek  # 0=Monday .. 6=Sunday
        is_hol = d_iso in hol_set
        h_name = hol_map.get(d_iso, "") if is_hol else ""

        if dow == 0:  # Monday
            long_we = is_hol
        elif dow == 4:  # Friday
            long_we = is_hol
        elif dow == 5:  # Saturday: Friday before or Monday after
            long_we = ((ts - one_day).strftime("%Y-%m-%d") in hol_set) or (
                (ts + 2 * one_day).strftime("%Y-%m-%d") in hol_set
            )
        elif dow == 6:  # Sunday: Friday before or Monday after
            long_we = ((ts - 2 * one_day).strftime("%Y-%m-%d") in hol_set) or (
                (ts + one_day).strftime("%Y-%m-%d") in hol_set
            )
        else:
            long_we = False

        date_keys.append(int(ts.strftime("%Y%m%d")))
        full_dates.append(ts.date())
        years.append(int(ts.year))
        months.append(int(ts.month))
        quarters.append(int(ts.quarter))
        weekdays.append(int(dow + 1))
        weekday_names.append(str(ts.day_name()))
        is_weekends.append(bool(dow >= 5))
        is_holidays.append(bool(is_hol))
        holiday_names.append(h_name)
        is_long_weekends.append(bool(long_we))
        month_names.append(str(ts.month_name()))
        day_of_months.append(int(ts.day))
        year_months.append(ts.strftime("%Y-%m"))

    schema = pa.schema(
        [
            ("date_key", pa.int32()),
            ("full_date", pa.date32()),
            ("year", pa.int32()),
            ("month", pa.int32()),
            ("quarter", pa.int32()),
            ("weekday", pa.int32()),
            ("weekday_name", pa.string()),
            ("is_weekend", pa.bool_()),
            ("is_holiday", pa.bool_()),
            ("holiday_name", pa.string()),
            ("is_long_weekend", pa.bool_()),
            ("month_name", pa.string()),
            ("day_of_month", pa.int32()),
            ("year_month", pa.string()),
        ]
    )
    return pa.Table.from_pydict(
        {
            "date_key": date_keys,
            "full_date": full_dates,
            "year": years,
            "month": months,
            "quarter": quarters,
            "weekday": weekdays,
            "weekday_name": weekday_names,
            "is_weekend": is_weekends,
            "is_holiday": is_holidays,
            "holiday_name": holiday_names,
            "is_long_weekend": is_long_weekends,
            "month_name": month_names,
            "day_of_month": day_of_months,
            "year_month": year_months,
        },
        schema=schema,
    )


def build_dim_route_rows(
    routes_df: pd.DataFrame,
    history_df: pd.DataFrame,
    extra_routes: Iterable[str] = (),
) -> List[Dict[str, Any]]:
    """Builds SCD Type 2 rows for `dim_route` (R9).

    `valid_from` is inclusive, `valid_to` is exclusive (`9999-12-31` for current version).
    """
    route_meta: Dict[str, Tuple[str, str, str]] = {}
    for r in routes_df.itertuples(index=False):
        route_meta[str(r.route)] = (str(r.origin), str(r.destination), str(r.region))

    rows: List[Dict[str, Any]] = []
    next_key = 1

    # Preserve deterministic order across routes
    for route, group in history_df.sort_values(["route", "effective_date"]).groupby("route", sort=True):
        route_str = str(route)
        if route_str in route_meta:
            origin, dest, region = route_meta[route_str]
        else:
            parts = route_str.split("-")
            origin = parts[0] if len(parts) == 2 else route_str
            dest = parts[1] if len(parts) == 2 else "UNK"
            region = airport_region(origin)

        versions = group.sort_values("effective_date").to_dict("records")
        for idx, ver in enumerate(versions):
            is_last = idx == (len(versions) - 1)
            vf = dt.date.fromisoformat(str(ver["effective_date"])[:10])
            vt = (
                FAR_FUTURE_DATE
                if is_last
                else dt.date.fromisoformat(str(versions[idx + 1]["effective_date"])[:10])
            )
            cat = str(ver["route_category"])
            rows.append(
                {
                    "route_key": next_key,
                    "route": route_str,
                    "origin": origin,
                    "destination": dest,
                    "region": region,
                    "route_category": cat,
                    "valid_from": vf,
                    "valid_to": vt,
                    "is_current": bool(is_last),
                    "destination_region": airport_region(dest),
                    "is_trunk": cat == "TRUNK",
                }
            )
            next_key += 1

    known_routes = {r["route"] for r in rows}
    for extra in sorted(set(extra_routes)):
        if not extra or extra in known_routes:
            continue
        parts = extra.split("-")
        origin = parts[0] if len(parts) == 2 else extra
        dest = parts[1] if len(parts) == 2 else "UNK"
        rows.append(
            {
                "route_key": next_key,
                "route": extra,
                "origin": origin,
                "destination": dest,
                "region": airport_region(origin),
                "route_category": "REGIONAL",
                "valid_from": CALENDAR_START,
                "valid_to": FAR_FUTURE_DATE,
                "is_current": True,
                "destination_region": airport_region(dest),
                "is_trunk": False,
            }
        )
        known_routes.add(extra)
        next_key += 1

    return rows


def dim_route_to_table(route_rows: List[Dict[str, Any]]) -> pa.Table:
    schema = pa.schema(
        [
            ("route_key", pa.int32()),
            ("route", pa.string()),
            ("origin", pa.string()),
            ("destination", pa.string()),
            ("region", pa.string()),
            ("route_category", pa.string()),
            ("valid_from", pa.date32()),
            ("valid_to", pa.date32()),
            ("is_current", pa.bool_()),
            ("destination_region", pa.string()),
            ("is_trunk", pa.bool_()),
        ]
    )
    cols = {field.name: [r[field.name] for r in route_rows] for field in schema}
    return pa.Table.from_pydict(cols, schema=schema)


def build_dim_fare_class_rows(extra_classes: Iterable[str] = ()) -> Tuple[pa.Table, Dict[str, int]]:
    base = ["ECONOMY", "BUSINESS"]
    for c in sorted(set(extra_classes)):
        if c and c not in base:
            base.append(c)

    keys: List[int] = []
    names: List[str] = []
    cabin_groups: List[str] = []
    multipliers: List[float] = []
    mapping: Dict[str, int] = {}

    for idx, fc in enumerate(base, start=1):
        keys.append(idx)
        names.append(fc)
        cabin_groups.append("PREMIUM_CABIN" if fc == "BUSINESS" else "MAIN_CABIN")
        multipliers.append(2.2 if fc == "BUSINESS" else 1.0)
        mapping[fc] = idx

    schema = pa.schema(
        [
            ("fare_class_key", pa.int32()),
            ("fare_class", pa.string()),
            ("cabin_group", pa.string()),
            ("price_multiplier", pa.float64()),
        ]
    )
    table = pa.Table.from_pydict(
        {
            "fare_class_key": keys,
            "fare_class": names,
            "cabin_group": cabin_groups,
            "price_multiplier": multipliers,
        },
        schema=schema,
    )
    return table, mapping


class RouteAsOfResolver:
    """Resolves `(route, event_date)` to the SCD Type 2 `route_key` valid on `event_date` (R9)."""

    def __init__(self, route_rows: List[Dict[str, Any]]) -> None:
        self._by_route: Dict[str, List[Tuple[dt.date, dt.date, int]]] = {}
        for r in route_rows:
            self._by_route.setdefault(r["route"], []).append(
                (r["valid_from"], r["valid_to"], int(r["route_key"]))
            )
        for versions in self._by_route.values():
            versions.sort(key=lambda x: x[0])

    def resolve(self, route: str, event_date: dt.date) -> Optional[int]:
        versions = self._by_route.get(route)
        if not versions:
            return None
        for vf, vt, rkey in versions:
            if vf <= event_date < vt:
                return rkey
        if event_date < versions[0][0]:
            return versions[0][2]
        return versions[-1][2]


def transform_events_to_star(
    raw_events: Iterable[Dict[str, Any]],
    seed: int,
    ref_dir: str,
) -> Dict[str, Any]:
    """Pure transformation from dirty raw event dicts -> PyArrow Star Schema tables.

    Handles:
    - Deduplication by `event_id` (deterministic earliest arrival/timestamp order)
    - Event-time assignment (`date_key` and `month` strictly from `event_ts`)
    - SCD Type 2 as-of join on `dim_route` (`valid_from <= event_date < valid_to`)
    - Strict foreign-key integrity against `dim_date`, `dim_route`, and `dim_fare_class`
    """
    refs = ensure_reference_data(seed, ref_dir)
    dim_date_tbl = build_dim_date_table(refs["holidays"])
    valid_date_keys = set(dim_date_tbl["date_key"].to_pylist())

    # Deduplicate by event_id deterministically (sort by event_ts, event_id first)
    by_id: Dict[str, Dict[str, Any]] = {}
    for ev in raw_events:
        eid = ev.get("event_id")
        etype = ev.get("event_type")
        ets = ev.get("event_ts")
        if not eid or not isinstance(eid, str) or not etype or not isinstance(ets, str) or len(ets) < 10:
            continue
        if etype not in ("search_performed", "reservation_created", "reservation_cancelled"):
            continue
        if eid not in by_id:
            by_id[eid] = ev

    clean_events = sorted(by_id.values(), key=lambda e: (str(e.get("event_ts", "")), str(e.get("event_id", ""))))

    # Collect any ad-hoc routes or fare classes so R6 never sees orphan foreign keys
    extra_routes: set[str] = set()
    extra_fares: set[str] = set()
    for ev in clean_events:
        etype = ev["event_type"]
        if etype == "search_performed":
            o = str(ev.get("origin", "")).upper().strip()
            d = str(ev.get("destination", "")).upper().strip()
            if o and d:
                extra_routes.add(f"{o}-{d}")
        else:
            r = str(ev.get("route", "")).upper().strip()
            fc = str(ev.get("fare_class", "")).upper().strip()
            if r:
                extra_routes.add(r)
            if fc:
                extra_fares.add(fc)

    route_rows = build_dim_route_rows(refs["routes"], refs["history"], extra_routes=extra_routes)
    dim_route_tbl = dim_route_to_table(route_rows)
    route_resolver = RouteAsOfResolver(route_rows)
    dim_fare_tbl, fare_map = build_dim_fare_class_rows(extra_classes=extra_fares)

    # Fact accumulators
    search_by_month: Dict[str, Dict[str, List[Any]]] = {}
    booking_cols: Dict[str, List[Any]] = {
        "event_id": [],
        "date_key": [],
        "route_key": [],
        "fare_class_key": [],
        "reservation_id": [],
        "event_type": [],
        "bookings": [],
        "cancellations": [],
        "revenue_cents": [],
    }

    for ev in clean_events:
        ets = str(ev["event_ts"])
        date_str = ets[:10]
        try:
            ev_date = dt.date.fromisoformat(date_str)
            dkey = int(date_str.replace("-", ""))
        except ValueError:
            continue

        if dkey not in valid_date_keys:
            continue

        etype = ev["event_type"]
        if etype == "search_performed":
            o = str(ev.get("origin", "")).upper().strip()
            d = str(ev.get("destination", "")).upper().strip()
            route_str = f"{o}-{d}"
            rkey = route_resolver.resolve(route_str, ev_date)
            if rkey is None:
                continue
            ym = date_str[:7]
            bucket = search_by_month.setdefault(
                ym,
                {"event_id": [], "date_key": [], "route_key": [], "results_count": [], "searches": []},
            )
            bucket["event_id"].append(str(ev["event_id"]))
            bucket["date_key"].append(dkey)
            bucket["route_key"].append(rkey)
            bucket["results_count"].append(int(ev.get("results_count", 0)))
            bucket["searches"].append(1)
        else:
            route_str = str(ev.get("route", "")).upper().strip()
            fc_str = str(ev.get("fare_class", "ECONOMY")).upper().strip()
            rkey = route_resolver.resolve(route_str, ev_date)
            fkey = fare_map.get(fc_str)
            if rkey is None or fkey is None:
                continue
            price = int(ev.get("price_cents", 0))
            is_created = etype == "reservation_created"
            bookings = 1 if is_created else 0
            cancellations = 0 if is_created else 1
            revenue = price if is_created else -price

            booking_cols["event_id"].append(str(ev["event_id"]))
            booking_cols["date_key"].append(dkey)
            booking_cols["route_key"].append(rkey)
            booking_cols["fare_class_key"].append(fkey)
            booking_cols["reservation_id"].append(str(ev.get("reservation_id", "")))
            booking_cols["event_type"].append(etype)
            booking_cols["bookings"].append(bookings)
            booking_cols["cancellations"].append(cancellations)
            booking_cols["revenue_cents"].append(revenue)

    fact_search_schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("date_key", pa.int32()),
            ("route_key", pa.int32()),
            ("results_count", pa.int32()),
            ("searches", pa.int32()),
        ]
    )
    fact_search_partitions: Dict[str, pa.Table] = {}
    for ym in sorted(search_by_month.keys()):
        fact_search_partitions[ym] = pa.Table.from_pydict(search_by_month[ym], schema=fact_search_schema)

    fact_booking_schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("date_key", pa.int32()),
            ("route_key", pa.int32()),
            ("fare_class_key", pa.int32()),
            ("reservation_id", pa.string()),
            ("event_type", pa.string()),
            ("bookings", pa.int32()),
            ("cancellations", pa.int32()),
            ("revenue_cents", pa.int64()),
        ]
    )
    fact_booking_tbl = pa.Table.from_pydict(booking_cols, schema=fact_booking_schema)

    return {
        "dim_date": dim_date_tbl,
        "dim_route": dim_route_tbl,
        "dim_fare_class": dim_fare_tbl,
        "fact_search_partitions": fact_search_partitions,
        "fact_search_schema": fact_search_schema,
        "fact_booking_event": fact_booking_tbl,
        "stats": {
            "distinct_events": len(clean_events),
            "fact_search_rows": sum(t.num_rows for t in fact_search_partitions.values()),
            "fact_search_months": sorted(fact_search_partitions.keys()),
            "fact_booking_rows": fact_booking_tbl.num_rows,
        },
    }


# --------------------------------------------------------------------------- AWS S3 + Glue Publisher
def _arrow_to_glue_type(dt_type: pa.DataType) -> str:
    if pa.types.is_int64(dt_type):
        return "bigint"
    if pa.types.is_integer(dt_type):
        return "int"
    if pa.types.is_floating(dt_type):
        return "double"
    if pa.types.is_boolean(dt_type):
        return "boolean"
    if pa.types.is_date(dt_type):
        return "date"
    return "string"


def _glue_columns_from_schema(schema: pa.Schema) -> List[Dict[str, str]]:
    return [{"Name": f.name, "Type": _arrow_to_glue_type(f.type)} for f in schema]


class StarSchemaAWSJob:
    """Executes the full Landing Zone -> S3 Parquet -> Glue Data Catalog pipeline."""

    def __init__(
        self,
        s3_bucket: str,
        raw_prefix: str,
        star_prefix: str,
        glue_database: str,
        seed: int,
        ref_dir: str,
        region_name: str = "us-east-1",
    ) -> None:
        self.s3_bucket = s3_bucket
        self.raw_prefix = raw_prefix if raw_prefix.endswith("/") else f"{raw_prefix}/"
        self.star_prefix = star_prefix if star_prefix.endswith("/") else f"{star_prefix}/"
        self.glue_database = glue_database
        self.seed = seed
        self.ref_dir = ref_dir
        self.region_name = region_name

        self._s3 = boto3.client("s3", region_name=region_name)
        self._glue = boto3.client("glue", region_name=region_name)

    def run(self) -> Dict[str, Any]:
        t0 = time.time()
        raw_events = self._read_landing_zone()
        star = transform_events_to_star(raw_events, seed=self.seed, ref_dir=self.ref_dir)

        self._ensure_glue_database()

        # 1. Unpartitioned tables
        for name in ("dim_date", "dim_route", "dim_fare_class", "fact_booking_event"):
            tbl: pa.Table = star[name]
            table_prefix = f"{self.star_prefix}{name}/"
            self._clear_s3_prefix(table_prefix)
            self._write_parquet_to_s3(tbl, f"{table_prefix}part-00000.parquet")
            self._upsert_glue_table(
                name=name,
                columns=_glue_columns_from_schema(tbl.schema),
                location=f"s3://{self.s3_bucket}/{table_prefix}",
                partition_keys=[],
            )

        # 2. Partitioned table: fact_search (partitioned by `month` YYYY-MM)
        fs_prefix = f"{self.star_prefix}fact_search/"
        self._clear_s3_prefix(fs_prefix)
        fs_partitions: Dict[str, pa.Table] = star["fact_search_partitions"]
        fs_cols = _glue_columns_from_schema(star["fact_search_schema"])

        for ym, part_tbl in fs_partitions.items():
            part_key = f"{fs_prefix}month={ym}/part-00000.parquet"
            self._write_parquet_to_s3(part_tbl, part_key)

        self._upsert_glue_table(
            name="fact_search",
            columns=fs_cols,
            location=f"s3://{self.s3_bucket}/{fs_prefix}",
            partition_keys=[{"Name": "month", "Type": "string"}],
        )
        self._replace_glue_partitions(
            table_name="fact_search",
            columns=fs_cols,
            base_prefix=fs_prefix,
            months=sorted(fs_partitions.keys()),
        )

        elapsed = round(time.time() - t0, 2)
        stats = dict(star["stats"], duration_seconds=elapsed)
        logger.info("Star schema refresh SUCCEEDED in %.2fs: %s", elapsed, stats)
        return stats

    def _read_landing_zone(self) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        keys: List[str] = []
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.raw_prefix):
            for obj in page.get("Contents", []):
                if obj["Size"] > 0 and not obj["Key"].endswith("/"):
                    keys.append(obj["Key"])

        logger.info("Reading %d S3 landing zone objects from s3://%s/%s", len(keys), self.s3_bucket, self.raw_prefix)
        for key in keys:
            body = self._s3.get_object(Bucket=self.s3_bucket, Key=key)["Body"].read()
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            text = body.decode("utf-8", errors="replace")
            events.extend(parse_json_records(text))
        return events

    def _clear_s3_prefix(self, prefix: str) -> None:
        paginator = self._s3.get_paginator("list_objects_v2")
        to_delete: List[Dict[str, str]] = []
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                to_delete.append({"Key": obj["Key"]})
                if len(to_delete) == 1000:
                    self._s3.delete_objects(Bucket=self.s3_bucket, Delete={"Objects": to_delete})
                    to_delete = []
        if to_delete:
            self._s3.delete_objects(Bucket=self.s3_bucket, Delete={"Objects": to_delete})

    def _write_parquet_to_s3(self, table: pa.Table, key: str) -> None:
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy", use_dictionary=True)
        self._s3.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )

    def _ensure_glue_database(self) -> None:
        try:
            self._glue.get_database(Name=self.glue_database)
        except self._glue.exceptions.EntityNotFoundException:
            self._glue.create_database(
                DatabaseInput={
                    "Name": self.glue_database,
                    "Description": "Airline Reservation System Star Schema (Parcial 3)",
                }
            )

    def _storage_descriptor(self, columns: List[Dict[str, str]], location: str) -> Dict[str, Any]:
        return {
            "Columns": columns,
            "Location": location,
            "InputFormat": PARQUET_INPUT_FORMAT,
            "OutputFormat": PARQUET_OUTPUT_FORMAT,
            "Compressed": True,
            "SerdeInfo": {
                "SerializationLibrary": PARQUET_SERDE,
                "Parameters": {"serialization.format": "1"},
            },
            "Parameters": {"classification": "parquet"},
        }

    def _upsert_glue_table(
        self,
        name: str,
        columns: List[Dict[str, str]],
        location: str,
        partition_keys: List[Dict[str, str]],
    ) -> None:
        table_input = {
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "parquet",
                "EXTERNAL": "TRUE",
                "parquet.compression": "SNAPPY",
            },
            "StorageDescriptor": self._storage_descriptor(columns, location),
            "PartitionKeys": partition_keys,
        }
        try:
            self._glue.get_table(DatabaseName=self.glue_database, Name=name)
            self._glue.update_table(DatabaseName=self.glue_database, TableInput=table_input)
        except self._glue.exceptions.EntityNotFoundException:
            self._glue.create_table(DatabaseName=self.glue_database, TableInput=table_input)

    def _replace_glue_partitions(
        self,
        table_name: str,
        columns: List[Dict[str, str]],
        base_prefix: str,
        months: List[str],
    ) -> None:
        # Delete any existing partitions first so stale months never linger
        existing: List[List[str]] = []
        paginator = self._glue.get_paginator("get_partitions")
        for page in paginator.paginate(DatabaseName=self.glue_database, TableName=table_name):
            for p in page.get("Partitions", []):
                existing.append(p["Values"])

        for i in range(0, len(existing), 25):
            batch_del = [{"Values": vals} for vals in existing[i : i + 25]]
            self._glue.batch_delete_partition(
                DatabaseName=self.glue_database,
                TableName=table_name,
                PartitionsToDelete=batch_del,
            )

        if not months:
            return

        inputs = []
        for ym in months:
            loc = f"s3://{self.s3_bucket}/{base_prefix}month={ym}/"
            inputs.append(
                {
                    "Values": [ym],
                    "StorageDescriptor": self._storage_descriptor(columns, loc),
                    "Parameters": {"classification": "parquet"},
                }
            )

        for i in range(0, len(inputs), 100):
            self._glue.batch_create_partition(
                DatabaseName=self.glue_database,
                TableName=table_name,
                PartitionInputList=inputs[i : i + 100],
            )


class StarSchemaRefreshManager:
    """Manages asynchronous execution and status polling for POST /admin/refresh-star (R8)."""

    def __init__(
        self,
        job_factory: Callable[[], StarSchemaAWSJob],
        pre_refresh_hook: Optional[Callable[[], None]] = None,
        dry_run: bool = False,
    ) -> None:
        self._job_factory = job_factory
        self._pre_refresh_hook = pre_refresh_hook
        self._dry_run = dry_run
        self._lock = threading.Lock()
        self._state: str = "SUCCEEDED"
        self._job_id: str = "init"
        self._last_stats: Dict[str, Any] = {}
        self._last_error: Optional[str] = None

    def trigger(self) -> Dict[str, Any]:
        with self._lock:
            if self._state == "RUNNING":
                return {"job_id": self._job_id, "state": "RUNNING"}
            job_id = f"star-{uuid.uuid4().hex[:10]}"
            self._job_id = job_id
            self._state = "RUNNING"
            self._last_error = None

        t = threading.Thread(target=self._execute, args=(job_id,), name=f"etl-{job_id}", daemon=True)
        t.start()
        return {"job_id": job_id, "state": "RUNNING"}

    def status(self) -> Dict[str, Any]:
        with self._lock:
            res: Dict[str, Any] = {"state": self._state, "job_id": self._job_id}
            if self._last_stats:
                res["stats"] = self._last_stats
            if self._last_error:
                res["error"] = self._last_error
            return res

    def _execute(self, job_id: str) -> None:
        try:
            if self._pre_refresh_hook:
                self._pre_refresh_hook()
            if self._dry_run:
                with self._lock:
                    self._state = "SUCCEEDED"
                    self._last_stats = {"dry_run": True}
                return

            job = self._job_factory()
            stats = job.run()
            with self._lock:
                self._last_stats = stats
                self._state = "SUCCEEDED"
        except Exception as exc:
            logger.exception("Star schema refresh job %s FAILED: %s", job_id, exc)
            with self._lock:
                self._last_error = str(exc)
                self._state = "FAILED"
