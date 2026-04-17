"""Central configuration for the Fortaleza screener."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
SEEDS_DIR = DATA_DIR / "seeds"
DB_PATH = DATA_DIR / "fortaleza.duckdb"

# HTTP politeness
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
REQUEST_DELAY_SEC = 2.0   # between listing-page requests
GEOCODE_DELAY_SEC = 1.1   # Nominatim fair-use

# Tier 1 STR bairros — focus universe for v1
TIER1_BAIRROS: list[str] = [
    "meireles",
    "mucuripe",
    "praia-de-iracema",
    "praia-do-futuro-ii",
    "praia-do-futuro-i",
]

# Listing-level hard filters (adjust freely)
MIN_AREA_M2 = 28
MAX_AREA_M2 = 250
MIN_PRICE_BRL = 200_000
MAX_PRICE_BRL = 5_000_000
MIN_BEDROOMS = 1

# Scoring weights — must sum to 1.0
SCORE_WEIGHTS = {
    "usd_irr_10y": 0.40,
    "revpar_vs_median": 0.20,
    "price_per_m2_vs_median": 0.15,
    "condo_burden": 0.15,
    "building_quality": 0.10,
}
