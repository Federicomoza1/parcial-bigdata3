"""Deterministic loader & generator for Parcial 3 reference data:
- routes_ref.csv
- route_category_history.csv
- holidays_ref.csv (Colombian public holidays 2023-2027)

Reads from `REF_DATA_DIR` if already generated via `python -m airline_data.generate_p3`,
or automatically generates identical files from `TEAM_SEED` so EC2 deployments work out-of-the-box.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

AIRPORTS = ["BOG", "MDE", "CLO", "CTG", "BAQ", "SMR", "BGA", "PEI", "CUC", "ADZ", "LET", "PSO"]
REGIONS = ["ANDINA", "CARIBE", "PACIFICO"]
CATEGORIES = ["REGIONAL", "TRUNK"]
SCD_DATE = "2025-02-01"

# Exact Colombian holidays 2023-2027 matching parcial3-harness-estudiantes/airline_data/holidays_co.csv
HOLIDAYS_CO_2023_2027: Tuple[Tuple[str, str], ...] = (
    ("2023-01-01", "Año Nuevo"),
    ("2023-01-09", "Día de los Reyes Magos (observado)"),
    ("2023-03-20", "Día de San José (observado)"),
    ("2023-04-06", "Jueves Santo"),
    ("2023-04-07", "Viernes Santo"),
    ("2023-05-01", "Día del Trabajo"),
    ("2023-05-22", "Ascensión del señor (observado)"),
    ("2023-06-12", "Corpus Christi (observado)"),
    ("2023-06-19", "Sagrado Corazón (observado)"),
    ("2023-07-03", "San Pedro y San Pablo (observado)"),
    ("2023-07-20", "Día de la Independencia"),
    ("2023-08-07", "Batalla de Boyacá"),
    ("2023-08-21", "La Asunción (observado)"),
    ("2023-10-16", "Día de la Raza (observado)"),
    ("2023-11-06", "Día de Todos los Santos (observado)"),
    ("2023-11-13", "Independencia de Cartagena (observado)"),
    ("2023-12-08", "La Inmaculada Concepción"),
    ("2023-12-25", "Navidad"),
    ("2024-01-01", "Año Nuevo"),
    ("2024-01-08", "Día de los Reyes Magos (observado)"),
    ("2024-03-25", "Día de San José (observado)"),
    ("2024-03-28", "Jueves Santo"),
    ("2024-03-29", "Viernes Santo"),
    ("2024-05-01", "Día del Trabajo"),
    ("2024-05-13", "Ascensión del señor (observado)"),
    ("2024-06-03", "Corpus Christi (observado)"),
    ("2024-06-10", "Sagrado Corazón (observado)"),
    ("2024-07-01", "San Pedro y San Pablo (observado)"),
    ("2024-07-20", "Día de la Independencia"),
    ("2024-08-07", "Batalla de Boyacá"),
    ("2024-08-19", "La Asunción (observado)"),
    ("2024-10-14", "Día de la Raza (observado)"),
    ("2024-11-04", "Día de Todos los Santos (observado)"),
    ("2024-11-11", "Independencia de Cartagena"),
    ("2024-12-08", "La Inmaculada Concepción"),
    ("2024-12-25", "Navidad"),
    ("2025-01-01", "Año Nuevo"),
    ("2025-01-06", "Día de los Reyes Magos"),
    ("2025-03-24", "Día de San José (observado)"),
    ("2025-04-17", "Jueves Santo"),
    ("2025-04-18", "Viernes Santo"),
    ("2025-05-01", "Día del Trabajo"),
    ("2025-06-02", "Ascensión del señor (observado)"),
    ("2025-06-23", "Corpus Christi (observado)"),
    ("2025-06-30", "Sagrado Corazón (observado); San Pedro y San Pablo (observado)"),
    ("2025-07-20", "Día de la Independencia"),
    ("2025-08-07", "Batalla de Boyacá"),
    ("2025-08-18", "La Asunción (observado)"),
    ("2025-10-13", "Día de la Raza (observado)"),
    ("2025-11-03", "Día de Todos los Santos (observado)"),
    ("2025-11-17", "Independencia de Cartagena (observado)"),
    ("2025-12-08", "La Inmaculada Concepción"),
    ("2025-12-25", "Navidad"),
    ("2026-01-01", "Año Nuevo"),
    ("2026-01-12", "Día de los Reyes Magos (observado)"),
    ("2026-03-23", "Día de San José (observado)"),
    ("2026-04-02", "Jueves Santo"),
    ("2026-04-03", "Viernes Santo"),
    ("2026-05-01", "Día del Trabajo"),
    ("2026-05-18", "Ascensión del señor (observado)"),
    ("2026-06-08", "Corpus Christi (observado)"),
    ("2026-06-15", "Sagrado Corazón (observado)"),
    ("2026-06-29", "San Pedro y San Pablo"),
    ("2026-07-13", "Día de Nuestra Señora del Rosario de Chiquinquirá (observado)"),
    ("2026-07-20", "Día de la Independencia"),
    ("2026-08-07", "Batalla de Boyacá"),
    ("2026-08-17", "La Asunción (observado)"),
    ("2026-10-12", "Día de la Raza"),
    ("2026-11-02", "Día de Todos los Santos (observado)"),
    ("2026-11-16", "Independencia de Cartagena (observado)"),
    ("2026-12-08", "La Inmaculada Concepción"),
    ("2026-12-25", "Navidad"),
    ("2027-01-01", "Año Nuevo"),
    ("2027-01-11", "Día de los Reyes Magos (observado)"),
    ("2027-03-22", "Día de San José (observado)"),
    ("2027-03-25", "Jueves Santo"),
    ("2027-03-26", "Viernes Santo"),
    ("2027-05-01", "Día del Trabajo"),
    ("2027-05-10", "Ascensión del señor (observado)"),
    ("2027-05-31", "Corpus Christi (observado)"),
    ("2027-06-07", "Sagrado Corazón (observado)"),
    ("2027-07-05", "San Pedro y San Pablo (observado)"),
    ("2027-07-12", "Día de Nuestra Señora del Rosario de Chiquinquirá (observado)"),
    ("2027-07-20", "Día de la Independencia"),
    ("2027-08-07", "Batalla de Boyacá"),
    ("2027-08-16", "La Asunción (observado)"),
    ("2027-10-18", "Día de la Raza (observado)"),
    ("2027-11-01", "Día de Todos los Santos"),
    ("2027-11-15", "Independencia de Cartagena (observado)"),
    ("2027-12-08", "La Inmaculada Concepción"),
    ("2027-12-25", "Navidad"),
)


def airport_region(code: str) -> str:
    if code in AIRPORTS:
        return REGIONS[AIRPORTS.index(code) % 3]
    return "OTHER"


def _team_routes(seed: int) -> list[str]:
    rs = np.random.RandomState(seed)
    n_routes = int(rs.randint(8, 15))
    pairs = [(a, b) for a in AIRPORTS for b in AIRPORTS if a != b]
    idx = rs.permutation(len(pairs))[:n_routes]
    return [f"{pairs[i][0]}-{pairs[i][1]}" for i in idx]


def generate_routes_df(seed: int) -> pd.DataFrame:
    rows = []
    for r in _team_routes(seed):
        o, d = r.split("-")
        rows.append({"route": r, "origin": o, "destination": d, "region": airport_region(o)})
    return pd.DataFrame(rows)


def generate_category_history_df(seed: int) -> pd.DataFrame:
    routes = _team_routes(seed)
    rs = np.random.RandomState((seed * 7919 + 11) % (2**31 - 1))
    changing = set(rs.choice(len(routes), size=max(2, len(routes) // 3), replace=False).tolist())
    rows = []
    for i, r in enumerate(routes):
        c0 = CATEGORIES[int(rs.randint(0, 2))]
        rows.append({"route": r, "route_category": c0, "effective_date": "2023-01-01"})
        if i in changing:
            c1 = CATEGORIES[1 - CATEGORIES.index(c0)]
            rows.append({"route": r, "route_category": c1, "effective_date": SCD_DATE})
    return pd.DataFrame(rows)


def generate_holidays_df() -> pd.DataFrame:
    harness_csv = (
        Path(__file__).resolve().parent.parent
        / "parcial3-harness-estudiantes"
        / "airline_data"
        / "holidays_co.csv"
    )
    if harness_csv.exists():
        return pd.read_csv(harness_csv)
    return pd.DataFrame(HOLIDAYS_CO_2023_2027, columns=["date", "holiday_name"])


def ensure_reference_data(seed: int, ref_dir: str) -> Dict[str, pd.DataFrame]:
    """Loads reference CSVs from `ref_dir`, generating them first if missing."""
    os.makedirs(ref_dir, exist_ok=True)
    routes_path = os.path.join(ref_dir, "routes_ref.csv")
    hist_path = os.path.join(ref_dir, "route_category_history.csv")
    holidays_path = os.path.join(ref_dir, "holidays_ref.csv")

    if not os.path.exists(routes_path):
        generate_routes_df(seed).to_csv(routes_path, index=False)
    if not os.path.exists(hist_path):
        generate_category_history_df(seed).to_csv(hist_path, index=False)
    if not os.path.exists(holidays_path):
        generate_holidays_df().to_csv(holidays_path, index=False)

    return {
        "routes": pd.read_csv(routes_path),
        "history": pd.read_csv(hist_path),
        "holidays": pd.read_csv(holidays_path),
    }
