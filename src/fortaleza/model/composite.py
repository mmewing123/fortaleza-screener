"""Composite scoring: turn a listing into a ranked investment score.

Pipeline per listing:
  1. STR legality classification (hard knockout if red)
  2. STR revenue estimation (ADR × occ × 365)
  3. OpEx stack (mgmt, cleaning, condo, IPTU, maintenance, insurance, vacancy)
  4. Acquisition cost stack (ITBI, registro, cartório)
  5. 10y cashflows in BRL → translated to USD under base/bear/bull FX drift
  6. IRR computation
  7. Weighted composite score vs bairro medians

Everything is a data class so it's trivial to unit-test.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Optional

import duckdb

from ..config import SCORE_WEIGHTS
from ..db import connect
from ..enrich.beach_distance import nearest_beach
from ..enrich.str_legality import classify as classify_str
from .fx import FXScenarios
from .str_yield import estimate as estimate_str_yield


def _num(v, default: float = 0.0) -> float:
    """Coerce DuckDB/pandas nullish values (None, NaN) to a usable float."""
    if v is None:
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if math.isnan(f):
        return default
    return f


def _opt(v) -> float | None:
    """Return None for nullish values, else a clean float."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


# --- Assumption constants (override via config later) ----------------------

ACQUISITION_COST_PCT = 0.05          # ITBI + registro + cartório + despachante
CLEANING_PER_TURNOVER_BRL = 120
AVG_STAY_NIGHTS = 4
PLATFORM_FEE_PCT = 0.15              # blended Airbnb+Booking host fee
DEFAULT_MGMT_PCT = 0.22              # Fortaleza local mgmt co-op range
MAINT_PCT_OF_VALUE = 0.015           # annual maintenance as % of property value
INSURANCE_BRL_PER_YEAR_PCT = 0.003   # annual insurance
VACANCY_BUFFER = 0.0                 # already captured in est_occupancy
APPRECIATION_PCT = 0.04              # BRL-nominal annual appreciation (tune)
RENT_GROWTH_PCT = 0.05               # BRL-nominal annual ADR growth (~IGP-M proxy)
TAX_RATE_EFFECTIVE = 0.15            # simplified PF/PJ blended; real answer needs contador
REMITTANCE_FX_HAIRCUT = 0.02         # 2% per USD conversion event
HOLD_YEARS = 10
EXIT_SELLING_COSTS_PCT = 0.06        # broker + cap-gains drag approximation


@dataclass
class ScoreRow:
    source: str
    external_id: str
    str_legality_flag: str
    beach_dist_m: Optional[float]
    beach_nearest: Optional[str]
    est_adr_brl: Optional[float]
    est_occupancy: Optional[float]
    gross_revpar_brl: Optional[float]
    net_annual_brl: Optional[float]
    acquisition_cost_brl: Optional[float]
    usd_irr_10y_base: Optional[float]
    usd_irr_10y_bear: Optional[float]
    usd_irr_10y_bull: Optional[float]
    composite_score: Optional[float]
    notes: str


def _irr(cashflows: list[float], guess: float = 0.08) -> Optional[float]:
    """Newton-Raphson IRR. Returns None if it fails to converge."""
    if not cashflows or cashflows[0] >= 0:
        return None
    r = guess
    for _ in range(100):
        npv = sum(cf / (1 + r) ** t for t, cf in enumerate(cashflows))
        d_npv = sum(-t * cf / (1 + r) ** (t + 1) for t, cf in enumerate(cashflows))
        if abs(d_npv) < 1e-12:
            return None
        r_new = r - npv / d_npv
        if not (-0.99 < r_new < 10):
            return None
        if abs(r_new - r) < 1e-7:
            return r_new
        r = r_new
    return None


def _compute_cashflows_usd(
    price_brl: float,
    net_annual_brl: float,
    fx_spot: float,
    annual_fx_drift: float,
    appreciation: float = APPRECIATION_PCT,
    rent_growth: float = RENT_GROWTH_PCT,
    hold_years: int = HOLD_YEARS,
) -> list[float]:
    """Build USD cashflows: year 0 = -acquisition, years 1..N = net income + year N exit."""
    acq = price_brl * (1 + ACQUISITION_COST_PCT)
    # Year 0: full BRL acquisition translated at spot, with one haircut for the inbound FX conversion
    cashflows_usd = [-(acq / fx_spot) * (1 + REMITTANCE_FX_HAIRCUT)]

    for yr in range(1, hold_years + 1):
        # BRL operating income grows with rent_growth
        income_brl = net_annual_brl * (1 + rent_growth) ** (yr - 1)
        fx_t = fx_spot * (1 + annual_fx_drift) ** yr
        income_usd = (income_brl / fx_t) * (1 - REMITTANCE_FX_HAIRCUT)

        if yr == hold_years:
            # Exit: property appreciates at nominal BRL rate, pay selling costs, FX-convert
            sale_brl = price_brl * (1 + appreciation) ** hold_years * (1 - EXIT_SELLING_COSTS_PCT)
            income_usd += (sale_brl / fx_t) * (1 - REMITTANCE_FX_HAIRCUT)
        cashflows_usd.append(income_usd)
    return cashflows_usd


def _score_one(
    listing: dict,
    bairro_row: dict,
    median_price_per_m2: float,
    median_revpar: float,
    fx_spot: float,
    fx: FXScenarios,
) -> ScoreRow:
    source = listing["source"]
    ext_id = listing["external_id"]
    price = _opt(listing.get("price_brl"))
    area = _opt(listing.get("area_m2"))
    condo_fee_m = _num(listing.get("condo_fee_brl"))
    iptu = _num(listing.get("iptu_brl_year"))
    bedrooms = listing.get("bedrooms")
    if bedrooms is not None:
        try:
            bedrooms = int(bedrooms) if not math.isnan(float(bedrooms)) else None
        except (TypeError, ValueError):
            bedrooms = None

    # --- STR legality ------------------------------------------------------
    legality = classify_str(
        listing.get("product_type"), listing.get("description"), listing.get("amenities")
    )

    # --- Beach distance ----------------------------------------------------
    bh = nearest_beach(_opt(listing.get("latitude")), _opt(listing.get("longitude")))
    beach_dist_m = bh.distance_m if bh else None
    beach_name = bh.beach_name if bh else None
    beach_premium = bh.premium if bh else None

    # --- STR revenue estimation -------------------------------------------
    yield_est = estimate_str_yield(
        tier=bairro_row.get("tier") or 3,
        str_demand_score=bairro_row.get("str_demand_score") or 0.3,
        bedrooms=bedrooms,
        area_m2=area,
        beach_dist_m=beach_dist_m,
        beach_premium=beach_premium,
    )

    # --- Opex stack --------------------------------------------------------
    gross = yield_est.est_annual_gross_brl
    occupied_nights = 365 * yield_est.est_occupancy
    turnovers = occupied_nights / AVG_STAY_NIGHTS
    cleaning_annual = turnovers * CLEANING_PER_TURNOVER_BRL

    platform_fees = gross * PLATFORM_FEE_PCT
    mgmt = gross * DEFAULT_MGMT_PCT
    condo_annual = condo_fee_m * 12
    price_for_opex = price or 0.0
    maint = price_for_opex * MAINT_PCT_OF_VALUE
    insurance = price_for_opex * INSURANCE_BRL_PER_YEAR_PCT

    opex = platform_fees + mgmt + cleaning_annual + condo_annual + iptu + maint + insurance
    pretax = gross - opex
    net_annual_brl = pretax * (1 - TAX_RATE_EFFECTIVE)

    # --- IRR scenarios -----------------------------------------------------
    usd_irr_base = usd_irr_bear = usd_irr_bull = None
    if price and price > 0 and net_annual_brl > 0:
        for attr, drift in (("base", fx.base), ("bear", fx.bear), ("bull", fx.bull)):
            cfs = _compute_cashflows_usd(price, net_annual_brl, fx_spot, drift)
            irr = _irr(cfs)
            if attr == "base":
                usd_irr_base = irr
            elif attr == "bear":
                usd_irr_bear = irr
            else:
                usd_irr_bull = irr

    # --- Composite score ---------------------------------------------------
    # Normalize each sub-score to [0, 1]. Missing inputs => 0.5 (neutral).
    def _clip(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
        return max(lo, min(hi, x))

    # USD IRR: 0% -> 0, 15% -> 1
    irr_sub = _clip((usd_irr_base or 0) / 0.15) if usd_irr_base is not None else 0.5

    # RevPAR vs median: 1.0 at median, up to 1.3x -> 1.0
    revpar = gross / 365 if gross else 0
    revpar_sub = _clip(((revpar / median_revpar) - 0.7) / 0.6) if median_revpar else 0.5

    # Price/m² vs median — LOWER is better. 0.7x median -> 1.0, 1.3x -> 0
    if price and area and median_price_per_m2:
        ppm = price / area
        ratio = ppm / median_price_per_m2
        ppm_sub = _clip((1.3 - ratio) / 0.6)
    else:
        ppm_sub = 0.5

    # Condo burden: condo fees as % of projected gross revenue. 0% -> 1, 30%+ -> 0
    burden = condo_annual / gross if gross else 1.0
    condo_sub = _clip((0.30 - burden) / 0.30)

    # Building quality proxy: amenities count + "pool" presence
    amen = (listing.get("amenities") or "").lower()
    has_pool = 1.0 if ("pool" in amen or "piscina" in amen) else 0.0
    amen_count = len([a for a in amen.split("|") if a])
    quality_sub = _clip(0.5 * has_pool + 0.05 * amen_count)

    score = (
        SCORE_WEIGHTS["usd_irr_10y"] * irr_sub
        + SCORE_WEIGHTS["revpar_vs_median"] * revpar_sub
        + SCORE_WEIGHTS["price_per_m2_vs_median"] * ppm_sub
        + SCORE_WEIGHTS["condo_burden"] * condo_sub
        + SCORE_WEIGHTS["building_quality"] * quality_sub
    )

    # Hard-apply STR legality modifier
    if legality == "red":
        score = 0.0
    elif legality == "yellow":
        score *= 0.80

    notes = (
        f"legality={legality} irr_sub={irr_sub:.2f} revpar_sub={revpar_sub:.2f} "
        f"ppm_sub={ppm_sub:.2f} condo_sub={condo_sub:.2f} quality_sub={quality_sub:.2f} "
        f"| {yield_est.notes}"
    )

    return ScoreRow(
        source=source,
        external_id=ext_id,
        str_legality_flag=legality,
        beach_dist_m=beach_dist_m,
        beach_nearest=beach_name,
        est_adr_brl=yield_est.est_adr_brl,
        est_occupancy=yield_est.est_occupancy,
        gross_revpar_brl=round(revpar, 2) if revpar else None,
        net_annual_brl=round(net_annual_brl, 2),
        acquisition_cost_brl=round(price * ACQUISITION_COST_PCT, 2) if price else None,
        usd_irr_10y_base=usd_irr_base,
        usd_irr_10y_bear=usd_irr_bear,
        usd_irr_10y_bull=usd_irr_bull,
        composite_score=round(score, 4),
        notes=notes,
    )


def score_all(fx_spot: float, fx: FXScenarios) -> int:
    """Score every row in listings_latest and upsert into `scores`."""
    con = connect()

    # Load bairros into dict
    bairros_rows = con.execute(
        "SELECT slug, name, tier, fipezap_price_per_m2, str_demand_score FROM bairros"
    ).fetchall()
    bairros = {
        r[0]: {"slug": r[0], "name": r[1], "tier": r[2], "fipezap_price_per_m2": r[3],
               "str_demand_score": r[4]}
        for r in bairros_rows
    }

    # Compute per-bairro medians from current listings
    med = con.execute(
        """
        SELECT bairro,
               median(price_brl / NULLIF(area_m2, 0)) AS median_ppm,
               median(price_brl) AS median_price
        FROM listings_latest
        WHERE price_brl IS NOT NULL AND area_m2 IS NOT NULL
        GROUP BY bairro
        """
    ).fetchall()
    medians = {r[0]: {"ppm": r[1], "price": r[2]} for r in med}

    # Pull listings
    listings = con.execute("SELECT * FROM listings_latest").fetch_df().to_dict("records")

    run_date = date.today()
    written = 0
    for row in listings:
        bairro_slug = row.get("bairro")
        b = bairros.get(bairro_slug) or {"tier": 3, "str_demand_score": 0.3,
                                          "fipezap_price_per_m2": 7000}
        # Use FipeZap figure as fallback if not enough local comps
        m = medians.get(bairro_slug, {})
        median_ppm = m.get("ppm") or b.get("fipezap_price_per_m2") or 7000

        # Local RevPAR median: derive from a dry-run estimate at the bairro mean
        dry_run = estimate_str_yield(
            tier=b.get("tier") or 3,
            str_demand_score=b.get("str_demand_score") or 0.3,
            bedrooms=2,
            area_m2=55,
            beach_dist_m=600,
            beach_premium=0.9,
        )
        median_revpar = dry_run.est_annual_gross_brl / 365

        score = _score_one(row, b, median_ppm, median_revpar, fx_spot, fx)
        con.execute(
            """
            INSERT OR REPLACE INTO scores VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                score.source, score.external_id, run_date,
                score.str_legality_flag, score.beach_dist_m, score.beach_nearest,
                score.est_adr_brl, score.est_occupancy, score.gross_revpar_brl,
                score.net_annual_brl, score.acquisition_cost_brl,
                score.usd_irr_10y_base, score.usd_irr_10y_bear, score.usd_irr_10y_bull,
                score.composite_score, score.notes,
            ],
        )
        written += 1
    con.close()
    return written
