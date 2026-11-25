"""FastAPI application for Parcial 3: Airline Reservation System.

Implements:
- Parcial 2 operational endpoints (/health, POST /admin/flights, GET /flights/search,
  POST /reservations, GET /reservations/{id})
- Parcial 3 event-driven & analytical endpoints (POST /reservations/{id}/cancel,
  GET /analytics/live, POST /admin/refresh-star, GET /admin/refresh-status)
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Query, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from etl.reference_data import ensure_reference_data
from etl.star_builder import StarSchemaAWSJob, StarSchemaRefreshManager

from .config import Settings, get_settings
from .consumer import KinesisStreamConsumer, LiveWindowTracker, S3LandingFlusher
from .db import (
    AirlineDB,
    AlreadyCancelledError,
    FlightNotFoundError,
    ReservationNotFoundError,
    SeatUnavailableError,
)
from .events import build_reservation_event, build_search_performed
from .publisher import KinesisEventPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("airline.api")


# --------------------------------------------------------------------------- Request Schemas
class CreateFlightRequest(BaseModel):
    flight_id: str = Field(..., min_length=1)
    route: str = Field(..., min_length=3, description="Origin-Destination e.g. BOG-MDE")
    flight_date: str = Field(..., min_length=10, description="YYYY-MM-DD")
    capacity: int = Field(..., ge=1)
    price_cents: int = Field(..., ge=0)


class CreateReservationRequest(BaseModel):
    flight_id: str = Field(..., min_length=1)
    passenger_name: str = Field(..., min_length=1)
    fare_class: str = Field(default="ECONOMY")


# --------------------------------------------------------------------------- App State Container
class AppContainer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = AirlineDB(settings.sqlite_db_path)
        self.live_tracker = LiveWindowTracker(max_retention_minutes=settings.live_max_retention_minutes)
        self.s3_flusher = S3LandingFlusher(
            s3_bucket=settings.s3_bucket,
            raw_prefix=settings.raw_events_prefix,
            region_name=settings.aws_region,
            flush_interval_sec=settings.kinesis_flush_interval_sec,
            flush_max_records=settings.kinesis_flush_max_records,
            dry_run=settings.dry_run_no_aws,
        )
        self.consumer = KinesisStreamConsumer(
            stream_name=settings.kinesis_stream,
            live_tracker=self.live_tracker,
            s3_flusher=self.s3_flusher,
            region_name=settings.aws_region,
            start_minutes_ago=settings.kinesis_start_minutes_ago,
            dry_run=settings.dry_run_no_aws,
        )
        self.publisher = KinesisEventPublisher(
            stream_name=settings.kinesis_stream,
            region_name=settings.aws_region,
            dry_run=settings.dry_run_no_aws,
            local_sink=(lambda _pk, data: self.consumer.ingest_raw_bytes(data))
            if settings.dry_run_no_aws
            else None,
        )
        self.refresh_manager = StarSchemaRefreshManager(
            job_factory=self._make_etl_job,
            pre_refresh_hook=lambda: self.s3_flusher.flush_if_due(force=True),
            dry_run=settings.dry_run_no_aws,
        )

    def _make_etl_job(self) -> StarSchemaAWSJob:
        s = self.settings
        return StarSchemaAWSJob(
            s3_bucket=s.s3_bucket,
            raw_prefix=s.raw_events_prefix,
            star_prefix=s.star_schema_prefix,
            glue_database=s.athena_database,
            seed=s.team_seed,
            ref_dir=s.ref_data_dir,
            region_name=s.aws_region,
        )

    def start(self) -> None:
        ensure_reference_data(self.settings.team_seed, self.settings.ref_data_dir)
        self.publisher.start()
        self.consumer.start()

    def stop(self) -> None:
        self.publisher.stop()
        self.consumer.stop()


def create_app(custom_settings: Optional[Settings] = None) -> FastAPI:
    settings = custom_settings or get_settings()
    container = AppContainer(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        container.start()
        yield
        container.stop()

    app = FastAPI(
        title="Airline Reservation System - Parcial 3",
        version="3.0.0",
        lifespan=lifespan,
    )
    app.state.container = container

    static_dir = Path(__file__).resolve().parent.parent / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

        @app.get("/", include_in_schema=False)
        def dashboard_home():
            return FileResponse(str(static_dir / "index.html"))

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {
            "status": "ok",
            "team_id": settings.team_id,
            "seed": settings.team_seed,
            "kinesis_stream": settings.kinesis_stream,
            "published_events": container.publisher.published_count,
            "flushed_s3_events": container.s3_flusher.flushed_records,
        }

    @app.post("/admin/flights", status_code=status.HTTP_201_CREATED)
    def create_flight(req: CreateFlightRequest) -> Dict[str, Any]:
        try:
            return container.db.upsert_flight(
                flight_id=req.flight_id,
                route=req.route,
                flight_date=req.flight_date,
                capacity=req.capacity,
                price_cents=req.price_cents,
            )
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    @app.get("/flights/search")
    def search_flights(
        origin: str = Query(..., min_length=3, max_length=3),
        destination: str = Query(..., min_length=3, max_length=3),
        date: str = Query(..., min_length=10),
        x_session_id: Optional[str] = Header(default=None, alias="X-Session-Id"),
    ) -> Dict[str, Any]:
        flights = container.db.search_flights(origin=origin, destination=destination, flight_date=date)
        pk, event = build_search_performed(
            session_id=x_session_id or "",
            origin=origin,
            destination=destination,
            travel_date=date,
            results_count=len(flights),
        )
        # Non-blocking publish to in-memory queue (R1, R2)
        container.publisher.publish(pk, event)
        return {"flights": flights, "results_count": len(flights)}

    @app.post("/reservations", status_code=status.HTTP_201_CREATED)
    def create_reservation(req: CreateReservationRequest) -> Dict[str, Any]:
        try:
            res = container.db.create_reservation(
                flight_id=req.flight_id,
                passenger_name=req.passenger_name,
                fare_class=req.fare_class,
            )
        except FlightNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Flight not found") from exc
        except SeatUnavailableError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="No available seats") from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        # Emit AFTER DB transaction commit, strictly excluding passenger_name (R1)
        pk, event = build_reservation_event(
            event_type="reservation_created",
            reservation_id=res["reservation_id"],
            flight_id=res["flight_id"],
            route=res["route"],
            flight_date=res["flight_date"],
            fare_class=res["fare_class"],
            price_cents=res["price_cents"],
        )
        container.publisher.publish(pk, event)
        return res

    @app.get("/reservations/{reservation_id}")
    def get_reservation(reservation_id: str) -> Dict[str, Any]:
        res = container.db.get_reservation(reservation_id)
        if res is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reservation not found")
        return res

    @app.post("/reservations/{reservation_id}/cancel")
    def cancel_reservation(reservation_id: str) -> Dict[str, Any]:
        try:
            res = container.db.cancel_reservation(reservation_id)
        except ReservationNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reservation not found") from exc
        except AlreadyCancelledError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Reservation already cancelled") from exc

        pk, event = build_reservation_event(
            event_type="reservation_cancelled",
            reservation_id=res["reservation_id"],
            flight_id=res["flight_id"],
            route=res["route"],
            flight_date=res["flight_date"],
            fare_class=res["fare_class"],
            price_cents=res["price_cents"],
        )
        container.publisher.publish(pk, event)
        return res

    @app.get("/analytics/live")
    def analytics_live(window: int = Query(default=5, ge=1, le=60)) -> Dict[str, Any]:
        return container.live_tracker.snapshot(window_minutes=window)

    @app.post("/admin/refresh-star", status_code=status.HTTP_202_ACCEPTED)
    def refresh_star() -> Dict[str, Any]:
        return container.refresh_manager.trigger()

    @app.get("/admin/refresh-status")
    def refresh_status() -> Dict[str, Any]:
        return container.refresh_manager.status()

    return app


app = create_app()
