"""v1 ingestion: scrape VivaReal residential listings for Tier 1 bairros.

Strategy
--------
VivaReal / ZAP (same backend) render server-side and embed the full
search-result JSON in a `__NEXT_DATA__` <script> tag. We fetch the public
search page per-bairro, parse that JSON, and upsert each listing as a
daily snapshot.

Caveats
-------
* Their HTML shape changes ~yearly. If `_extract_listings` stops returning
  rows, open the page in a browser, inspect `window.__NEXT_DATA__`, and
  update the path below.
* Respect rate limits. Defaults to 2s between requests.
* Their anti-bot is forgiving at low volume; if you see 403s, rotate
  User-Agent and/or slow down further.
"""
from __future__ import annotations

import json
import time
from datetime import date
from typing import Iterable, Iterator

import httpx
from selectolax.parser import HTMLParser
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import REQUEST_DELAY_SEC, TIER1_BAIRROS, USER_AGENT
from ..db import connect, upsert_listing_snapshot


BASE = "https://www.vivareal.com.br"


def _search_url(bairro: str, page: int = 1) -> str:
    # Vivareal URL pattern for sales in Fortaleza bairros
    return f"{BASE}/venda/ceara/fortaleza/{bairro}/?pagina={page}"


@retry(wait=wait_exponential(multiplier=2, min=2, max=30), stop=stop_after_attempt(4))
def _fetch(client: httpx.Client, url: str) -> str:
    r = client.get(url, follow_redirects=True, timeout=30.0)
    r.raise_for_status()
    return r.text


def _extract_next_data(html: str) -> dict | None:
    tree = HTMLParser(html)
    node = tree.css_first("script#__NEXT_DATA__")
    if not node:
        return None
    try:
        return json.loads(node.text())
    except json.JSONDecodeError:
        return None


def _extract_listings(next_data: dict) -> list[dict]:
    """Walk __NEXT_DATA__ and return the listing array.

    VivaReal nests listings under several possible paths depending on
    the page type. We try known paths in order.
    """
    candidates = [
        ("props", "pageProps", "glueResult", "search", "result", "listings"),
        ("props", "pageProps", "initialProps", "search", "result", "listings"),
        ("props", "pageProps", "listings"),
    ]
    for path in candidates:
        cur: object = next_data
        ok = True
        for key in path:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                ok = False
                break
        if ok and isinstance(cur, list) and cur:
            return cur
    return []


def _to_row(raw: dict, bairro_slug: str, snapshot_date: date) -> dict | None:
    """Normalize VivaReal's listing shape into our schema."""
    listing = raw.get("listing") or raw  # shape varies
    if not isinstance(listing, dict):
        return None

    ext_id = listing.get("id") or listing.get("legacyId")
    if not ext_id:
        return None

    # Price: prefer sale price
    price = None
    for pi in listing.get("pricingInfos") or []:
        if (pi.get("businessType") or "").upper() == "SALE":
            price = _safe_float(pi.get("price"))
            condo_fee = _safe_float(pi.get("monthlyCondoFee"))
            iptu = _safe_float(pi.get("yearlyIptu"))
            break
    else:
        condo_fee = None
        iptu = None

    addr = listing.get("address") or {}
    bedrooms = _first_int(listing.get("bedrooms"))
    suites = _first_int(listing.get("suites"))
    bathrooms = _first_int(listing.get("bathrooms"))
    parking = _first_int(listing.get("parkingSpaces"))
    area = _first_float(listing.get("usableAreas")) or _first_float(listing.get("totalAreas"))

    lat = _safe_float((addr.get("point") or {}).get("lat"))
    lon = _safe_float((addr.get("point") or {}).get("lon"))

    amen_list = listing.get("amenities") or []
    amenities = "|".join(str(a).lower() for a in amen_list) if amen_list else None

    url_path = (raw.get("link") or {}).get("href") or listing.get("link") or ""
    url = f"{BASE}{url_path}" if url_path.startswith("/") else url_path

    return {
        "source": "vivareal",
        "external_id": str(ext_id),
        "snapshot_date": snapshot_date,
        "url": url or None,
        "title": listing.get("title"),
        "bairro": bairro_slug,
        "address": ", ".join(
            p for p in [addr.get("street"), addr.get("neighborhood"), addr.get("city")] if p
        ) or None,
        "latitude": lat,
        "longitude": lon,
        "product_type": _product_type(listing.get("unitTypes") or []),
        "bedrooms": bedrooms,
        "suites": suites,
        "bathrooms": bathrooms,
        "parking_spots": parking,
        "area_m2": area,
        "price_brl": price,
        "condo_fee_brl": condo_fee,
        "iptu_brl_year": iptu,
        "year_built": None,
        "amenities": amenities,
        "description": listing.get("description"),
        "raw_payload": json.dumps(raw, ensure_ascii=False),
    }


def _product_type(unit_types: list) -> str:
    vals = [str(u).upper() for u in unit_types]
    if any("HOUSE" in v or "CASA" in v for v in vals):
        return "house"
    return "residential_apt"


def _safe_float(v) -> float | None:
    try:
        return float(v) if v not in (None, "", "null") else None
    except (TypeError, ValueError):
        return None


def _first_int(v) -> int | None:
    if isinstance(v, list):
        v = v[0] if v else None
    try:
        return int(v) if v not in (None, "", "null") else None
    except (TypeError, ValueError):
        return None


def _first_float(v) -> float | None:
    if isinstance(v, list):
        v = v[0] if v else None
    return _safe_float(v)


def scrape_bairro(
    client: httpx.Client,
    bairro: str,
    max_pages: int = 5,
) -> Iterator[dict]:
    """Yield normalized listing rows for a bairro, one per listing."""
    snapshot = date.today()
    for page in range(1, max_pages + 1):
        html = _fetch(client, _search_url(bairro, page))
        nd = _extract_next_data(html)
        if not nd:
            return
        listings = _extract_listings(nd)
        if not listings:
            return
        for raw in listings:
            row = _to_row(raw, bairro, snapshot)
            if row:
                yield row
        time.sleep(REQUEST_DELAY_SEC)


def run(bairros: Iterable[str] = tuple(TIER1_BAIRROS), max_pages: int = 5) -> int:
    """Scrape all target bairros and upsert into DuckDB."""
    con = connect()
    total = 0
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"}
    with httpx.Client(headers=headers, http2=True) as client:
        for bairro in bairros:
            count = 0
            for row in scrape_bairro(client, bairro, max_pages=max_pages):
                upsert_listing_snapshot(con, row)
                count += 1
            print(f"  {bairro}: {count} listings")
            total += count
    con.close()
    return total


if __name__ == "__main__":
    n = run()
    print(f"Scraped {n} listings total")
