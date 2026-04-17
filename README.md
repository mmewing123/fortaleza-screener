# Fortaleza STR Screener

Buy-to-STR real estate screening tool for Fortaleza, Ceará, Brazil — built for a
**non-resident investor** who cares about **USD-denominated returns**.

Ranks listings by a composite score that combines a 10-year USD IRR (with FX
scenarios), STR yield vs bairro medians, price/m² vs FipeZap medians, condomínio
fee burden as a share of projected revenue, and building-quality signals.

## What's in here

```
fortaleza-screener/
├── pyproject.toml
├── src/fortaleza/
│   ├── config.py              # paths, tier-1 bairros, scoring weights
│   ├── db.py                  # DuckDB schema + connection
│   ├── cli.py                 # `fortaleza` CLI entrypoint
│   ├── ingest/
│   │   ├── condohotels.py     # v0 universe loader
│   │   ├── fipezap.py         # bairro reference loader
│   │   └── vivareal.py        # v1 VivaReal scraper
│   ├── enrich/
│   │   ├── geocode.py         # Nominatim backfill
│   │   ├── beach_distance.py  # Shapely nearest-beach
│   │   └── str_legality.py    # green/yellow/red classifier
│   └── model/
│       ├── fx.py              # BCB-SGS USD/BRL + scenarios
│       ├── str_yield.py       # ADR/occupancy proxy
│       └── composite.py       # IRR + composite scorer
├── data/seeds/
│   ├── bairros.yaml           # bairro reference data
│   ├── condohotels.yaml       # v0 pool-letting buildings
│   └── beaches.yaml           # beach polylines
├── dashboard/app.py           # Streamlit UI
└── .github/workflows/daily.yml
```

## Install

```bash
git clone <repo> fortaleza-screener
cd fortaleza-screener
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dashboard]'
```

## Run (v0+v1 quickstart)

```bash
# One-shot: loads seeds, refreshes FX, scrapes, geocodes, scores
fortaleza pipeline --pages 3

# Inspect top ranks in the terminal
fortaleza top --n 25

# Fire up the dashboard
streamlit run dashboard/app.py
```

## Individual steps (if you want finer control)

```bash
fortaleza init              # load bairros + condohotels into DuckDB
fortaleza fx                # pull USD/BRL from BCB-SGS, compute scenarios
fortaleza scrape --pages 5  # scrape VivaReal for tier-1 bairros
fortaleza enrich            # geocode rows missing lat/lon
fortaleza score             # run composite scoring
```

Data lands in `data/fortaleza.duckdb`. Inspect directly with:

```bash
duckdb data/fortaleza.duckdb
```

## What the score means

Per listing, the composite is a weighted sum:

| Component                 | Weight | Direction |
|---------------------------|--------|-----------|
| 10y USD IRR (base case)   | 40%    | higher is better |
| RevPAR vs bairro median   | 20%    | higher is better |
| Price/m² vs bairro median | 15%    | lower is better  |
| Condo fees / gross revenue| 15%    | lower is better  |
| Building quality proxy    | 10%    | higher is better |

**STR legality** is applied on top as a modifier: `red` → score forced to 0,
`yellow` → score × 0.8, `green` → unchanged.

## Important caveats (read before trusting any output)

1. **Condohotel seed data is a starting point.** Every row in
   `data/seeds/condohotels.yaml` is marked `verify: true`. Occupancy and ADR
   figures are placeholders — confirm each with the building operator or
   Secovi-CE reports before acting on a score.
2. **Bairro FipeZap figures are proxies.** The `fipezap_price_per_m2` and
   `fipezap_rent_per_m2` values in `data/seeds/bairros.yaml` need to be
   refreshed monthly from the actual [FipeZap bulletin](https://www.fipezap.com.br/).
3. **STR legality classifier is a triage tool.** It reads listing descriptions
   for obvious signals but the only authoritative answer is the condomínio's
   convenção.
4. **Tax assumption is simplified.** `TAX_RATE_EFFECTIVE = 0.15` in
   `composite.py` is a blended PF/PJ placeholder. Real answer requires a CE
   contador and depends on whether STR income gets classified as PJ activity.
5. **VivaReal scraper is fragile.** Their `__NEXT_DATA__` shape changes
   periodically. If `fortaleza scrape` starts returning 0 listings, open a
   search page in the browser, inspect the script tag, and update the JSON
   path in `vivareal.py::_extract_listings`.

## Roadmap

- **v2 — Airbnb/Booking comps.** Replace the ADR/occupancy proxy in
  `str_yield.py` with real per-bairro comps (AirDNA, or a polite scrape with
  seasonal index construction). Highest-impact next change.
- **v2.5 — seasonality + event overlay.** Month-by-month ADR index, Carnaval /
  Fortal / Castelão event premia.
- **v3 — ML scoring.** Once you have 100+ labeled decisions (saw → passed,
  saw → offered), blend rule-based with a gradient-boosted ranker.
- **v3.5 — condo convenção ingestion.** Scrape/parse convenção PDFs where
  available to flip the STR legality flag from yellow to green/red definitively.

## License / scraping ethics

Obey `robots.txt` and rate limits. Default `REQUEST_DELAY_SEC = 2.0`. If you
scale this up, switch to an authorized data partner.
