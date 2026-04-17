"""Compute nearest-beach distance for a listing.

We model each beach as a LineString in (lon, lat) space. Distance is
computed in a simple equirectangular projection centered on Fortaleza —
good enough at this scale (errors < ~1%).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import yaml
from shapely.geometry import LineString, Point

from ..config import SEEDS_DIR


# Center projection on Fortaleza
_LAT0 = -3.73
_M_PER_DEG_LAT = 110_574.0
_M_PER_DEG_LON = 111_320.0 * math.cos(math.radians(_LAT0))


class BeachHit(NamedTuple):
    beach_slug: str
    beach_name: str
    premium: float
    distance_m: float


@dataclass
class _Beach:
    slug: str
    name: str
    premium: float
    line: LineString


def _project(lat: float, lon: float) -> tuple[float, float]:
    return (lon * _M_PER_DEG_LON, lat * _M_PER_DEG_LAT)


def _load_beaches(path: Path = SEEDS_DIR / "beaches.yaml") -> list[_Beach]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    beaches: list[_Beach] = []
    for b in data.get("beaches", []):
        pts = [_project(lat, lon) for (lat, lon) in b["points"]]
        beaches.append(
            _Beach(slug=b["slug"], name=b["name"], premium=b.get("premium", 0.5), line=LineString(pts))
        )
    return beaches


_BEACHES = _load_beaches()


def nearest_beach(lat: float | None, lon: float | None) -> BeachHit | None:
    if lat is None or lon is None:
        return None
    p = Point(*_project(lat, lon))
    best: BeachHit | None = None
    for b in _BEACHES:
        d = p.distance(b.line)  # meters because we projected to meters
        if best is None or d < best.distance_m:
            best = BeachHit(b.slug, b.name, b.premium, d)
    return best
