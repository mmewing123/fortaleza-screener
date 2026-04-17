"""USD/BRL FX data and scenario projection.

Data source: Banco Central do Brasil SGS, series 1 (PTAX USD sell, daily).
Endpoint: https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados
No API key required.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta

import httpx
import pandas as pd

from ..db import connect


SGS_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados?formato=json"


@dataclass
class FXScenarios:
    """Annual USD/BRL depreciation rate under each scenario (BRL decline = +)."""
    base: float   # e.g. 0.05  = BRL weakens 5%/yr
    bear: float   # BRL weakens more aggressively
    bull: float   # BRL strengthens


def fetch_history(start_years_back: int = 10) -> pd.DataFrame:
    start = (date.today() - timedelta(days=365 * start_years_back)).strftime("%d/%m/%Y")
    end = date.today().strftime("%d/%m/%Y")
    url = f"{SGS_URL}&dataInicial={start}&dataFinal={end}"
    r = httpx.get(url, timeout=30.0)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    df["dt"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date
    df["rate"] = df["valor"].astype(float)
    return df[["dt", "rate"]]


def save_history(df: pd.DataFrame) -> int:
    con = connect()
    con.execute("DELETE FROM fx_usd_brl")
    con.executemany("INSERT INTO fx_usd_brl VALUES (?, ?)", df.values.tolist())
    con.close()
    return len(df)


def latest_rate() -> float | None:
    con = connect()
    row = con.execute("SELECT rate FROM fx_usd_brl ORDER BY dt DESC LIMIT 1").fetchone()
    con.close()
    return row[0] if row else None


def derive_scenarios(df: pd.DataFrame) -> FXScenarios:
    """Derive simple annualized drift scenarios from history.

    Base: trailing 10y annualized % change in USD/BRL.
    Bear: base + 1 stdev of annual log-returns.
    Bull: base - 1 stdev.
    """
    d = df.copy()
    d["dt"] = pd.to_datetime(d["dt"])
    d = d.set_index("dt").sort_index()

    annual = d["rate"].resample("YE").last().dropna()
    if len(annual) < 3:
        return FXScenarios(base=0.05, bear=0.12, bull=-0.02)

    logret = (annual / annual.shift(1)).apply(math.log).dropna()
    mu = logret.mean()
    sigma = logret.std()

    base = math.exp(mu) - 1
    bear = math.exp(mu + sigma) - 1
    bull = math.exp(mu - sigma) - 1
    return FXScenarios(base=base, bear=bear, bull=bull)


def refresh_and_scenarios() -> tuple[float, FXScenarios]:
    df = fetch_history()
    save_history(df)
    scenarios = derive_scenarios(df)
    return float(df["rate"].iloc[-1]), scenarios


if __name__ == "__main__":
    rate, sc = refresh_and_scenarios()
    print(f"Latest USD/BRL: {rate:.4f}")
    print(f"Annual drift — base: {sc.base:+.2%}  bear: {sc.bear:+.2%}  bull: {sc.bull:+.2%}")
