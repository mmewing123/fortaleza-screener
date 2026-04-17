"""DuckDB schema + connection helpers."""
from __future__ import annotations

import duckdb
from pathlib import Path

from .config import DB_PATH


SCHEMA = """
-- Raw listings: one row per (source, external_id, snapshot_date)
CREATE TABLE IF NOT EXISTS listing_snapshots (
    source              VARCHAR,          -- 'vivareal' | 'zap' | 'condohotel'
    external_id         VARCHAR,
    snapshot_date       DATE,
    url                 VARCHAR,
    title               VARCHAR,
    bairro              VARCHAR,
    address             VARCHAR,
    latitude            DOUBLE,
    longitude           DOUBLE,
    product_type        VARCHAR,          -- 'condohotel' | 'residential_apt' | 'house'
    bedrooms            INTEGER,
    suites              INTEGER,
    bathrooms           INTEGER,
    parking_spots       INTEGER,
    area_m2             DOUBLE,
    price_brl           DOUBLE,
    condo_fee_brl       DOUBLE,
    iptu_brl_year       DOUBLE,
    year_built          INTEGER,
    amenities           VARCHAR,          -- pipe-separated tags
    description         VARCHAR,
    raw_payload         JSON,
    PRIMARY KEY (source, external_id, snapshot_date)
);

-- Latest-view: derived from most recent snapshot per listing
CREATE OR REPLACE VIEW listings_latest AS
SELECT *
FROM listing_snapshots
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source, external_id
    ORDER BY snapshot_date DESC
) = 1;

-- Bairro reference data (tiers, FipeZap medians)
CREATE TABLE IF NOT EXISTS bairros (
    slug                VARCHAR PRIMARY KEY,
    name                VARCHAR,
    tier                INTEGER,
    fipezap_price_per_m2 DOUBLE,
    fipezap_rent_per_m2 DOUBLE,
    str_demand_score    DOUBLE,           -- 0..1, manual estimate
    notes               VARCHAR,
    as_of_date          DATE
);

-- Condohotel buildings (v0 universe)
CREATE TABLE IF NOT EXISTS condohotels (
    slug                VARCHAR PRIMARY KEY,
    name                VARCHAR,
    bairro              VARCHAR,
    address             VARCHAR,
    latitude            DOUBLE,
    longitude           DOUBLE,
    pool_letting        BOOLEAN,
    str_legal           BOOLEAN,
    reported_occupancy  DOUBLE,           -- 0..1 last published
    reported_adr_brl    DOUBLE,
    mgmt_fee_pct        DOUBLE,
    notes               VARCHAR,
    verify              BOOLEAN
);

-- Scoring output: one row per listing per scoring run
CREATE TABLE IF NOT EXISTS scores (
    source              VARCHAR,
    external_id         VARCHAR,
    run_date            DATE,
    str_legality_flag   VARCHAR,          -- 'green' | 'yellow' | 'red'
    beach_dist_m        DOUBLE,
    beach_nearest       VARCHAR,
    est_adr_brl         DOUBLE,
    est_occupancy       DOUBLE,
    gross_revpar_brl    DOUBLE,
    net_annual_brl      DOUBLE,
    acquisition_cost_brl DOUBLE,
    usd_irr_10y_base    DOUBLE,
    usd_irr_10y_bear    DOUBLE,
    usd_irr_10y_bull    DOUBLE,
    composite_score     DOUBLE,
    notes               VARCHAR,
    PRIMARY KEY (source, external_id, run_date)
);

-- FX history (from BCB-SGS series 1)
CREATE TABLE IF NOT EXISTS fx_usd_brl (
    dt                  DATE PRIMARY KEY,
    rate                DOUBLE
);
"""


def connect(path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(SCHEMA)
    return con


def upsert_listing_snapshot(con, row: dict) -> None:
    """Insert or replace a snapshot row. Primary key collision => replace."""
    con.execute(
        """
        INSERT OR REPLACE INTO listing_snapshots VALUES (
            $source, $external_id, $snapshot_date, $url, $title, $bairro,
            $address, $latitude, $longitude, $product_type, $bedrooms, $suites,
            $bathrooms, $parking_spots, $area_m2, $price_brl, $condo_fee_brl,
            $iptu_brl_year, $year_built, $amenities, $description, $raw_payload
        )
        """,
        row,
    )
