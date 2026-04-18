"""Generate a static HTML dashboard from the scored DuckDB database.

Writes to docs/index.html so GitHub Pages can serve it.

Single self-contained file: inlines Leaflet (map) and a sortable table.
No build step, no npm, no runtime server required.
"""
from __future__ import annotations

import html
import json
from datetime import date
from pathlib import Path

import duckdb

from .config import DB_PATH


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "docs"
OUT_FILE = OUT_DIR / "index.html"


def _fetch_rows() -> tuple[list[dict], dict]:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT  l.source, l.external_id, l.bairro, l.title, l.address,
                    l.latitude, l.longitude, l.product_type,
                    l.bedrooms, l.area_m2, l.price_brl, l.condo_fee_brl, l.url,
                    s.str_legality_flag,
                    s.beach_nearest, s.beach_dist_m,
                    s.est_adr_brl, s.est_occupancy,
                    s.net_annual_brl,
                    s.usd_irr_10y_base, s.usd_irr_10y_bear, s.usd_irr_10y_bull,
                    s.composite_score
            FROM listings_latest l
            LEFT JOIN scores s USING (source, external_id)
            WHERE s.composite_score IS NOT NULL
              AND s.run_date = (SELECT MAX(run_date) FROM scores)
            ORDER BY s.composite_score DESC
            """
        ).fetch_df().to_dict("records")

        fx_row = con.execute(
            "SELECT dt, rate FROM fx_usd_brl ORDER BY dt DESC LIMIT 1"
        ).fetchone()
    finally:
        con.close()

    fx = {"date": str(fx_row[0]), "rate": float(fx_row[1])} if fx_row else None
    return rows, fx


def _score_color(score: float) -> str:
    if score >= 0.55:
        return "#1b9e4b"
    if score >= 0.45:
        return "#f39c12"
    return "#c0392b"


def _fmt_num(v, decimals: int = 0, pct: bool = False, prefix: str = "") -> str:
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "—"
    if f != f:  # NaN
        return "—"
    if pct:
        return f"{f * 100:.{decimals}f}%"
    if decimals == 0:
        return f"{prefix}{f:,.0f}"
    return f"{prefix}{f:,.{decimals}f}"


def render() -> Path:
    rows, fx = _fetch_rows()
    run_date = date.today().isoformat()

    # JSON payload for the Leaflet map and client-side sort
    map_payload = [
        {
            "id": r["external_id"],
            "title": r["title"] or r["external_id"],
            "bairro": r["bairro"],
            "lat": r["latitude"],
            "lon": r["longitude"],
            "score": r["composite_score"] or 0,
            "price": r["price_brl"],
            "adr": r["est_adr_brl"],
            "occ": r["est_occupancy"],
            "irr": r["usd_irr_10y_base"],
            "legality": r["str_legality_flag"],
            "url": r["url"],
        }
        for r in rows
        if r.get("latitude") and r.get("longitude")
    ]

    # Build HTML table rows
    table_rows_html = []
    for i, r in enumerate(rows, 1):
        color = _score_color(r["composite_score"] or 0)
        url_cell = (
            f'<a href="{html.escape(r["url"])}" target="_blank" rel="noopener">open</a>'
            if r.get("url")
            else "—"
        )
        table_rows_html.append(
            f"""
            <tr>
              <td>{i}</td>
              <td>{html.escape(r['bairro'] or '')}</td>
              <td>{html.escape(r['title'] or r['external_id'])}</td>
              <td>{html.escape(r['product_type'] or '')}</td>
              <td class="num">{_fmt_num(r['area_m2'])}</td>
              <td class="num">{_fmt_num(r['price_brl'], prefix='R$')}</td>
              <td>
                <span class="legality legality-{html.escape(r['str_legality_flag'] or 'yellow')}">
                  {html.escape(r['str_legality_flag'] or '—')}
                </span>
              </td>
              <td class="num">{_fmt_num(r['est_adr_brl'], prefix='R$')}</td>
              <td class="num">{_fmt_num(r['est_occupancy'], decimals=1, pct=True)}</td>
              <td class="num">{_fmt_num(r['usd_irr_10y_base'], decimals=2, pct=True)}</td>
              <td class="num">
                <div class="score-cell">
                  <div class="score-bar" style="width:{(r['composite_score'] or 0)*100:.0f}%; background:{color};"></div>
                  <span>{(r['composite_score'] or 0):.3f}</span>
                </div>
              </td>
              <td>{url_cell}</td>
            </tr>
            """
        )

    fx_html = (
        f"USD/BRL spot: <b>{fx['rate']:.4f}</b> (as of {fx['date']})"
        if fx
        else "FX not loaded"
    )

    tmpl = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Fortaleza STR Screener</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  :root {{
    --fg: #1b1f23; --muted: #666; --bg: #fafbfc; --card: #fff;
    --border: #e1e4e8; --accent: #0366d6;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    margin: 0; padding: 0; background: var(--bg); color: var(--fg);
  }}
  header {{
    background: var(--card); border-bottom: 1px solid var(--border);
    padding: 18px 28px;
  }}
  header h1 {{ margin: 0 0 4px 0; font-size: 22px; }}
  header .meta {{ color: var(--muted); font-size: 13px; }}
  main {{ padding: 20px 28px; max-width: 1400px; margin: 0 auto; }}
  .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 22px; }}
  .kpi {{ background: var(--card); border: 1px solid var(--border); border-radius: 6px; padding: 14px 16px; }}
  .kpi .label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }}
  .kpi .value {{ font-size: 22px; font-weight: 600; margin-top: 4px; }}
  .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 6px; padding: 16px; margin-bottom: 22px; }}
  .card h2 {{ margin: 0 0 12px 0; font-size: 16px; }}
  #map {{ height: 460px; border-radius: 4px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); }}
  th {{ background: #f6f8fa; font-weight: 600; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.03em; }}
  td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  tr:hover td {{ background: #f6f8fa; }}
  .legality {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; text-transform: uppercase; }}
  .legality-green {{ background: #d4edda; color: #155724; }}
  .legality-yellow {{ background: #fff3cd; color: #856404; }}
  .legality-red {{ background: #f8d7da; color: #721c24; }}
  .score-cell {{ position: relative; height: 20px; width: 110px; background: #eef; border-radius: 3px; overflow: hidden; margin-left: auto; }}
  .score-cell .score-bar {{ position: absolute; top: 0; left: 0; bottom: 0; opacity: 0.45; }}
  .score-cell span {{ position: relative; padding-left: 8px; line-height: 20px; font-weight: 600; }}
  a {{ color: var(--accent); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .caveat {{ background: #fff8e1; border-left: 3px solid #f39c12; padding: 10px 14px; font-size: 13px; color: #6d5300; margin-bottom: 18px; border-radius: 3px; }}
  footer {{ color: var(--muted); font-size: 12px; text-align: center; padding: 24px; }}
</style>
</head>
<body>
<header>
  <h1>🏖️ Fortaleza STR Screener</h1>
  <div class="meta">Buy-to-STR screening for non-resident investors · {fx_html} · last run {run_date}</div>
</header>
<main>

  <div class="caveat">
    <b>Work in progress.</b> Condohotel ADR and occupancy figures are placeholders pending verification.
    FipeZap bairro medians are proxies. Use for directional ranking only — not investment advice.
  </div>

  <div class="kpis">
    <div class="kpi"><div class="label">Listings scored</div><div class="value">{len(rows)}</div></div>
    <div class="kpi"><div class="label">Top score</div><div class="value">{(rows[0]['composite_score'] if rows else 0):.3f}</div></div>
    <div class="kpi"><div class="label">Top USD IRR (base)</div><div class="value">{_fmt_num(rows[0]['usd_irr_10y_base'] if rows else None, decimals=2, pct=True)}</div></div>
    <div class="kpi"><div class="label">Median USD IRR</div><div class="value">{_fmt_num(sorted([r['usd_irr_10y_base'] for r in rows if r['usd_irr_10y_base']])[len(rows)//2] if rows else None, decimals=2, pct=True)}</div></div>
  </div>

  <div class="card">
    <h2>Map</h2>
    <div id="map"></div>
  </div>

  <div class="card">
    <h2>Ranked listings</h2>
    <div style="overflow-x: auto;">
      <table>
        <thead>
          <tr>
            <th>#</th><th>Bairro</th><th>Title</th><th>Type</th>
            <th class="num">m²</th><th class="num">Price</th>
            <th>Legality</th>
            <th class="num">Est. ADR</th><th class="num">Occ</th>
            <th class="num">USD IRR (base)</th>
            <th class="num">Score</th>
            <th>Link</th>
          </tr>
        </thead>
        <tbody>
          {"".join(table_rows_html)}
        </tbody>
      </table>
    </div>
  </div>
</main>

<footer>
  Generated from <a href="https://github.com/mmewing123/fortaleza-screener">mmewing123/fortaleza-screener</a> ·
  Updated daily via GitHub Actions.
</footer>

<script>
  const listings = {json.dumps(map_payload)};
  const map = L.map('map').setView([-3.728, -38.495], 13);
  L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
    attribution: '&copy; OpenStreetMap &copy; CARTO', maxZoom: 19
  }}).addTo(map);

  function colorFor(score) {{
    if (score >= 0.55) return '#1b9e4b';
    if (score >= 0.45) return '#f39c12';
    return '#c0392b';
  }}

  listings.forEach(l => {{
    if (l.lat == null || l.lon == null) return;
    const m = L.circleMarker([l.lat, l.lon], {{
      radius: 6 + 10 * l.score,
      color: colorFor(l.score), fillColor: colorFor(l.score),
      fillOpacity: 0.75, weight: 1
    }}).addTo(map);
    const priceBRL = l.price ? 'R$' + l.price.toLocaleString('en-US', {{maximumFractionDigits:0}}) : '—';
    const adr = l.adr ? 'R$' + l.adr.toFixed(0) : '—';
    const occ = l.occ ? (l.occ * 100).toFixed(0) + '%' : '—';
    const irr = l.irr != null ? (l.irr * 100).toFixed(2) + '%' : '—';
    const link = l.url ? `<a href="${{l.url}}" target="_blank">open listing</a>` : '';
    m.bindPopup(`
      <b>${{l.title}}</b><br>
      ${{l.bairro}} · legality: ${{l.legality}}<br>
      Price: ${{priceBRL}}<br>
      ADR: ${{adr}} × Occ: ${{occ}}<br>
      USD IRR (base): <b>${{irr}}</b><br>
      Composite: <b>${{l.score.toFixed(3)}}</b><br>
      ${{link}}
    `);
  }});
</script>
</body>
</html>
"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(tmpl, encoding="utf-8")
    return OUT_FILE


if __name__ == "__main__":
    p = render()
    print(f"Wrote {p}")
