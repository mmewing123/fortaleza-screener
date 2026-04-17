"""v0 ingestion: load the condohotel universe from seed YAML."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import yaml

from ..config import SEEDS_DIR
from ..db import connect, upsert_listing_snapshot


def load_condohotels(seed_path: Path = SEEDS_DIR / "condohotels.yaml") -> int:
    """Populate `condohotels` table AND write a synthetic listing_snapshot per building.

    Synthetic snapshots let condohotels flow through the same scoring pipeline
    as residential listings — price is imputed from reported ADR + occupancy.
    Returns number of buildings loaded.
    """
    data = yaml.safe_load(seed_path.read_text(encoding="utf-8"))
    buildings = data.get("buildings", [])

    con = connect()
    today = date.today()

    # Upsert reference rows
    for b in buildings:
        con.execute(
            """
            INSERT OR REPLACE INTO condohotels VALUES (
                $slug, $name, $bairro, $address, $latitude, $longitude,
                $pool_letting, $str_legal, $reported_occupancy, $reported_adr_brl,
                $mgmt_fee_pct, $notes, $verify
            )
            """,
            b,
        )

    # Create synthetic listing snapshots so scoring treats them uniformly.
    # Price is derived via a gross-income capitalization at a target 6% cap rate,
    # as a placeholder until per-unit asking prices are pulled.
    for b in buildings:
        adr = b.get("reported_adr_brl") or 0
        occ = b.get("reported_occupancy") or 0
        annual_gross = adr * occ * 365
        # Strip out mgmt fee + 30% opex to estimate NOI
        net = annual_gross * (1 - (b.get("mgmt_fee_pct") or 0.25)) * 0.70
        implied_price = net / 0.06 if net > 0 else None

        row = {
            "source": "condohotel",
            "external_id": b["slug"],
            "snapshot_date": today,
            "url": None,
            "title": b["name"],
            "bairro": b["bairro"],
            "address": b["address"],
            "latitude": b["latitude"],
            "longitude": b["longitude"],
            "product_type": "condohotel",
            "bedrooms": 1,
            "suites": 1,
            "bathrooms": 1,
            "parking_spots": 1,
            "area_m2": 35.0,  # typical condohotel unit
            "price_brl": implied_price,
            "condo_fee_brl": None,
            "iptu_brl_year": None,
            "year_built": None,
            "amenities": "pool|restaurant|reception|housekeeping",
            "description": b.get("notes"),
            "raw_payload": json.dumps(b, ensure_ascii=False),
        }
        upsert_listing_snapshot(con, row)

    con.close()
    return len(buildings)


if __name__ == "__main__":
    n = load_condohotels()
    print(f"Loaded {n} condohotel buildings")
