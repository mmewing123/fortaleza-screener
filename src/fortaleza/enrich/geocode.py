"""Geocode listing addresses to lat/lon using Nominatim (OSM).

Respects the public Nominatim usage policy: 1 request/sec max and a
meaningful User-Agent. For production-scale use, swap to a paid provider.
"""
from __future__ import annotations

import time
from typing import Optional

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderServiceError
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import GEOCODE_DELAY_SEC
from ..db import connect


_geocoder = Nominatim(user_agent="fortaleza-screener/0.1 (personal-research)")


@retry(wait=wait_exponential(multiplier=2, min=2, max=20), stop=stop_after_attempt(3))
def _geocode(q: str) -> Optional[tuple[float, float]]:
    try:
        loc = _geocoder.geocode(q, country_codes="br", timeout=15)
    except GeocoderServiceError:
        raise
    if not loc:
        return None
    return (loc.latitude, loc.longitude)


def backfill_missing_coords(limit: int = 200) -> int:
    """Fill latitude/longitude on the most recent snapshot where missing."""
    con = connect()
    rows = con.execute(
        """
        SELECT source, external_id, address
        FROM listings_latest
        WHERE (latitude IS NULL OR longitude IS NULL)
          AND address IS NOT NULL
        LIMIT ?
        """,
        [limit],
    ).fetchall()

    updated = 0
    for source, ext_id, address in rows:
        q = f"{address}, Fortaleza, Ceará, Brasil"
        try:
            result = _geocode(q)
        except Exception as e:
            print(f"  geocode failed for {ext_id}: {e}")
            time.sleep(GEOCODE_DELAY_SEC)
            continue
        if result:
            lat, lon = result
            con.execute(
                """
                UPDATE listing_snapshots
                SET latitude = ?, longitude = ?
                WHERE source = ? AND external_id = ?
                  AND snapshot_date = (
                    SELECT MAX(snapshot_date) FROM listing_snapshots
                    WHERE source = ? AND external_id = ?
                  )
                """,
                [lat, lon, source, ext_id, source, ext_id],
            )
            updated += 1
        time.sleep(GEOCODE_DELAY_SEC)

    con.close()
    return updated


if __name__ == "__main__":
    n = backfill_missing_coords()
    print(f"Geocoded {n} listings")
