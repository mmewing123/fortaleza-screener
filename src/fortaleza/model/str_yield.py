"""Estimate STR revenue (ADR + occupancy) by bairro.

v0/v1 uses market-level proxies keyed to the bairro's str_demand_score
and proximity to the premium beach segments. v2 will replace this with
live Airbnb/Booking comps.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class STREstimate:
    est_adr_brl: float
    est_occupancy: float
    est_annual_gross_brl: float
    notes: str


# Baseline ADR anchors (BRL/night) for a 1-br ~35m² unit in each tier.
# Scales up with bedrooms and area.
_TIER_ADR_1BR = {1: 380, 2: 280, 3: 180}
_TIER_OCC = {1: 0.62, 2: 0.52, 3: 0.35}


def estimate(
    tier: int,
    str_demand_score: float,
    bedrooms: int | None,
    area_m2: float | None,
    beach_dist_m: float | None,
    beach_premium: float | None,
) -> STREstimate:
    base_adr = _TIER_ADR_1BR.get(tier, 200)
    base_occ = _TIER_OCC.get(tier, 0.4)

    # Bedroom/area scaling
    br = bedrooms or 1
    br_mult = 1.0 + 0.35 * (br - 1)          # +35% per extra bedroom
    a = area_m2 or 35
    area_mult = (a / 35.0) ** 0.4             # sub-linear area scaling

    # Beach proximity: within 200m = full premium, falloff to 0 at 1500m
    if beach_dist_m is None:
        beach_mult = 0.85
    else:
        d = max(0.0, beach_dist_m - 200.0)
        decay = max(0.0, 1.0 - d / 1300.0)
        premium_factor = (beach_premium or 0.5) * decay
        beach_mult = 0.85 + 0.35 * premium_factor   # 0.85..1.20

    demand_mult = 0.7 + 0.6 * str_demand_score       # 0.7..1.3

    adr = base_adr * br_mult * area_mult * beach_mult * demand_mult
    occ = max(0.25, min(0.80, base_occ * (0.6 + 0.8 * str_demand_score)))
    annual = adr * occ * 365

    notes = (
        f"tier={tier} br_mult={br_mult:.2f} area_mult={area_mult:.2f} "
        f"beach_mult={beach_mult:.2f} demand_mult={demand_mult:.2f}"
    )
    return STREstimate(
        est_adr_brl=round(adr, 2),
        est_occupancy=round(occ, 3),
        est_annual_gross_brl=round(annual, 2),
        notes=notes,
    )
