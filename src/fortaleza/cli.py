"""CLI for the Fortaleza screener.

Usage:
    fortaleza init            # load seed data (bairros + condohotels)
    fortaleza fx              # refresh USD/BRL FX history
    fortaleza scrape [--pages N]   # scrape VivaReal tier-1 bairros
    fortaleza enrich          # backfill geocodes
    fortaleza score           # run composite scoring
    fortaleza pipeline        # init + fx + scrape + enrich + score
    fortaleza top [--n N]     # print top-ranked listings
"""
from __future__ import annotations

import argparse
import sys

from rich.console import Console
from rich.table import Table

from .db import connect
from .ingest.condohotels import load_condohotels
from .ingest.fipezap import load_bairros
from .ingest.vivareal import run as scrape_vivareal
from .enrich.geocode import backfill_missing_coords
from .model.fx import latest_rate, refresh_and_scenarios, derive_scenarios, fetch_history
from .model.composite import score_all
from .build_html import render as render_html

console = Console()


def cmd_init(args: argparse.Namespace) -> int:
    n_b = load_bairros()
    n_h = load_condohotels()
    console.print(f"[green]Loaded[/green] {n_b} bairros and {n_h} condohotels")
    return 0


def cmd_fx(args: argparse.Namespace) -> int:
    rate, sc = refresh_and_scenarios()
    console.print(f"Latest USD/BRL: [bold]{rate:.4f}[/bold]")
    console.print(
        f"Annual drift — base: {sc.base:+.2%}  bear: {sc.bear:+.2%}  bull: {sc.bull:+.2%}"
    )
    return 0


def cmd_scrape(args: argparse.Namespace) -> int:
    n = scrape_vivareal(max_pages=args.pages)
    console.print(f"[green]Scraped[/green] {n} listings")
    return 0


def cmd_enrich(args: argparse.Namespace) -> int:
    n = backfill_missing_coords(limit=args.limit)
    console.print(f"[green]Geocoded[/green] {n} listings")
    return 0


def cmd_score(args: argparse.Namespace) -> int:
    rate = latest_rate()
    if rate is None:
        console.print("[yellow]No FX history — fetching now[/yellow]")
        rate, sc = refresh_and_scenarios()
    else:
        df = fetch_history(start_years_back=10)
        sc = derive_scenarios(df)
    n = score_all(fx_spot=rate, fx=sc)
    console.print(f"[green]Scored[/green] {n} listings at spot {rate:.4f}")
    return 0


def cmd_pipeline(args: argparse.Namespace) -> int:
    for step_fn, label in [
        (lambda: load_bairros(), "bairros seed"),
        (lambda: load_condohotels(), "condohotels seed"),
        (lambda: refresh_and_scenarios(), "FX refresh"),
        (lambda: scrape_vivareal(max_pages=args.pages), "scrape"),
        (lambda: backfill_missing_coords(limit=500), "geocode"),
    ]:
        console.print(f"[bold cyan]→ {label}[/bold cyan]")
        try:
            step_fn()
        except Exception as e:
            console.print(f"[red]  failed: {e}[/red]")
    return cmd_score(args)


def cmd_top(args: argparse.Namespace) -> int:
    con = connect()
    rows = con.execute(
        """
        SELECT  l.bairro, l.title, l.price_brl, l.area_m2,
                ROUND(l.price_brl / NULLIF(l.area_m2,0), 0) AS ppm,
                s.str_legality_flag,
                ROUND(s.est_adr_brl, 0) AS adr,
                ROUND(s.est_occupancy * 100, 1) AS occ_pct,
                ROUND(s.usd_irr_10y_base * 100, 2) AS irr_base,
                ROUND(s.composite_score, 3) AS score,
                l.url
        FROM scores s
        JOIN listings_latest l USING (source, external_id)
        WHERE s.composite_score IS NOT NULL
        ORDER BY s.composite_score DESC
        LIMIT ?
        """,
        [args.n],
    ).fetchall()
    con.close()

    table = Table(title=f"Top {args.n} listings by composite score")
    for col in ("bairro", "title", "price BRL", "m²", "R$/m²", "legal",
                "ADR", "occ %", "USD IRR %", "score", "url"):
        table.add_column(col, overflow="fold")
    for r in rows:
        table.add_row(*[("" if v is None else str(v)) for v in r])
    console.print(table)
    return 0


def cmd_build_html(args: argparse.Namespace) -> int:
    path = render_html()
    console.print(f"[green]Wrote[/green] {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="fortaleza")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Load seed data")
    sub.add_parser("fx", help="Refresh FX history")

    sp_scrape = sub.add_parser("scrape", help="Scrape VivaReal tier-1 bairros")
    sp_scrape.add_argument("--pages", type=int, default=5)

    sp_enrich = sub.add_parser("enrich", help="Backfill geocodes")
    sp_enrich.add_argument("--limit", type=int, default=200)

    sub.add_parser("score", help="Run composite scoring")

    sp_pipe = sub.add_parser("pipeline", help="Run full pipeline end-to-end")
    sp_pipe.add_argument("--pages", type=int, default=3)

    sp_top = sub.add_parser("top", help="Print top-ranked listings")
    sp_top.add_argument("--n", type=int, default=20)

    sub.add_parser("build-html", help="Render docs/index.html for GitHub Pages")

    args = p.parse_args(argv)
    return {
        "init": cmd_init,
        "fx": cmd_fx,
        "scrape": cmd_scrape,
        "enrich": cmd_enrich,
        "score": cmd_score,
        "pipeline": cmd_pipeline,
        "top": cmd_top,
        "build-html": cmd_build_html,
    }[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
