"""Streamlit dashboard for the Fortaleza STR screener.

Run with:
    streamlit run dashboard/app.py

Tabs:
  Overview   — top-ranked listings + KPIs
  Map        — Folium map, color-coded by composite score
  Detail     — drill-down on one listing, with IRR sensitivity
  Assumptions — edit global model constants and re-score live
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make the src/ package importable when running via `streamlit run`
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import duckdb  # noqa: E402
import folium  # noqa: E402
import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402
from streamlit_folium import st_folium  # noqa: E402

from fortaleza.config import DB_PATH  # noqa: E402
from fortaleza.model.fx import latest_rate  # noqa: E402


st.set_page_config(page_title="Fortaleza STR Screener", layout="wide")


# ------------------------------------------------------------------ data ---

@st.cache_data(ttl=300)
def load_joined() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        df = con.execute(
            """
            SELECT  l.source, l.external_id, l.bairro, l.title, l.address,
                    l.latitude, l.longitude, l.product_type,
                    l.bedrooms, l.suites, l.bathrooms, l.parking_spots,
                    l.area_m2, l.price_brl, l.condo_fee_brl, l.iptu_brl_year,
                    l.url, l.amenities,
                    s.str_legality_flag, s.beach_dist_m, s.beach_nearest,
                    s.est_adr_brl, s.est_occupancy, s.gross_revpar_brl,
                    s.net_annual_brl, s.acquisition_cost_brl,
                    s.usd_irr_10y_base, s.usd_irr_10y_bear, s.usd_irr_10y_bull,
                    s.composite_score, s.notes
            FROM listings_latest l
            LEFT JOIN scores s USING (source, external_id)
            """
        ).fetch_df()
    finally:
        con.close()
    if not df.empty:
        df["price_per_m2"] = df["price_brl"] / df["area_m2"]
    return df


# ------------------------------------------------------------------ UI -----

df = load_joined()
fx = latest_rate()

st.title("🏖️ Fortaleza STR Screener")
st.caption(
    "Buy-to-STR screening for non-resident investors. "
    f"{'USD/BRL spot: **' + f'{fx:.4f}' + '**' if fx else 'FX not yet loaded.'}"
)

if df.empty:
    st.warning(
        "No data yet. Run `fortaleza pipeline` from the CLI first, "
        "or at minimum `fortaleza init && fortaleza fx && fortaleza score`."
    )
    st.stop()

# ------------------------------------------------------------------ sidebar

with st.sidebar:
    st.header("Filters")
    bairros = sorted(df["bairro"].dropna().unique().tolist())
    pick_bairros = st.multiselect("Bairros", bairros, default=bairros)

    product_types = sorted(df["product_type"].dropna().unique().tolist())
    pick_products = st.multiselect("Product type", product_types, default=product_types)

    legality_opts = ["green", "yellow", "red"]
    pick_legality = st.multiselect("STR legality", legality_opts, default=["green", "yellow"])

    if df["price_brl"].notna().any():
        price_lo, price_hi = int(df["price_brl"].min()), int(df["price_brl"].max())
        price_range = st.slider(
            "Price (BRL)",
            min_value=price_lo, max_value=price_hi,
            value=(price_lo, min(price_hi, 2_000_000)),
            step=25_000, format="R$%d",
        )
    else:
        price_range = (0, 10_000_000)

    min_bedrooms = st.number_input("Min bedrooms", 0, 6, 1)
    min_score = st.slider("Min composite score", 0.0, 1.0, 0.0, 0.05)

f = df.copy()
f = f[f["bairro"].isin(pick_bairros)]
f = f[f["product_type"].isin(pick_products)]
f = f[f["str_legality_flag"].fillna("yellow").isin(pick_legality)]
f = f[(f["price_brl"].between(*price_range)) | f["price_brl"].isna()]
f = f[(f["bedrooms"].fillna(0) >= min_bedrooms)]
f = f[(f["composite_score"].fillna(0) >= min_score)]

# ------------------------------------------------------------------ tabs

tab_overview, tab_map, tab_detail, tab_assumptions = st.tabs(
    ["Overview", "Map", "Detail", "Assumptions"]
)

with tab_overview:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Listings", len(f))
    c2.metric("Median R$/m²", f"R${f['price_per_m2'].median():,.0f}" if len(f) else "—")
    c3.metric(
        "Median USD IRR (base)",
        f"{f['usd_irr_10y_base'].median() * 100:.2f}%" if f["usd_irr_10y_base"].notna().any() else "—",
    )
    c4.metric(
        "Top composite score",
        f"{f['composite_score'].max():.3f}" if f["composite_score"].notna().any() else "—",
    )

    st.subheader("Top 25 listings")
    top = f.sort_values("composite_score", ascending=False).head(25)
    show_cols = [
        "bairro", "title", "product_type", "bedrooms", "area_m2",
        "price_brl", "price_per_m2", "str_legality_flag",
        "est_adr_brl", "est_occupancy", "usd_irr_10y_base",
        "composite_score", "url",
    ]
    st.dataframe(
        top[show_cols],
        column_config={
            "price_brl": st.column_config.NumberColumn("Price (BRL)", format="R$%d"),
            "price_per_m2": st.column_config.NumberColumn("R$/m²", format="R$%d"),
            "est_adr_brl": st.column_config.NumberColumn("Est. ADR", format="R$%d"),
            "est_occupancy": st.column_config.NumberColumn("Occ", format="%.1f%%"),
            "usd_irr_10y_base": st.column_config.NumberColumn("USD IRR", format="%.2f%%"),
            "composite_score": st.column_config.ProgressColumn(
                "Score", min_value=0.0, max_value=1.0, format="%.3f"
            ),
            "url": st.column_config.LinkColumn("Link"),
        },
        hide_index=True,
        use_container_width=True,
    )

    st.subheader("Price/m² vs composite score")
    if f["price_per_m2"].notna().any():
        fig = px.scatter(
            f.dropna(subset=["price_per_m2", "composite_score"]),
            x="price_per_m2", y="composite_score",
            color="bairro", size="area_m2", hover_data=["title", "bedrooms"],
            labels={"price_per_m2": "R$/m²", "composite_score": "Composite score"},
        )
        st.plotly_chart(fig, use_container_width=True)

with tab_map:
    map_df = f.dropna(subset=["latitude", "longitude"]).copy()
    st.write(f"{len(map_df)} listings with coordinates.")
    if not map_df.empty:
        m = folium.Map(location=[-3.728, -38.495], zoom_start=13, tiles="cartodbpositron")
        for _, r in map_df.iterrows():
            score = r.get("composite_score") or 0
            # Green > 0.6, amber 0.4–0.6, red below
            color = "#1b9e4b" if score >= 0.6 else "#f39c12" if score >= 0.4 else "#c0392b"
            popup = folium.Popup(
                html=(
                    f"<b>{r['title'] or r['external_id']}</b><br>"
                    f"{r['bairro']} · {int(r['bedrooms']) if pd.notna(r['bedrooms']) else '?'} br<br>"
                    f"R${r['price_brl']:,.0f} · {r['area_m2']:.0f}m²<br>"
                    f"Score: <b>{score:.3f}</b> · Legality: {r['str_legality_flag']}<br>"
                    f"USD IRR base: {(r['usd_irr_10y_base'] or 0) * 100:.2f}%<br>"
                    f"<a href='{r['url'] or '#'}' target='_blank'>Open listing</a>"
                ),
                max_width=320,
            )
            folium.CircleMarker(
                location=[r["latitude"], r["longitude"]],
                radius=5 + 8 * score,
                color=color, fill=True, fill_opacity=0.75,
                popup=popup,
            ).add_to(m)
        st_folium(m, height=600, use_container_width=True)

with tab_detail:
    ids = f["external_id"].dropna().tolist()
    if not ids:
        st.info("No listings match filters.")
    else:
        pick = st.selectbox(
            "Choose a listing",
            ids,
            format_func=lambda eid: (
                f"{eid} — {f[f['external_id'] == eid]['title'].iloc[0] or ''}"
            ),
        )
        row = f[f["external_id"] == pick].iloc[0].to_dict()
        c1, c2 = st.columns([2, 1])
        with c1:
            st.subheader(row.get("title") or row.get("external_id"))
            st.write(f"**Bairro:** {row.get('bairro')}  ·  **Type:** {row.get('product_type')}")
            st.write(f"**Address:** {row.get('address') or '—'}")
            if row.get("url"):
                st.markdown(f"[Open on source]({row['url']})")
            st.write(row.get("notes") or "")
        with c2:
            st.metric("Price", f"R${row['price_brl']:,.0f}" if pd.notna(row.get("price_brl")) else "—")
            st.metric("Area", f"{row['area_m2']:.0f} m²" if pd.notna(row.get("area_m2")) else "—")
            st.metric(
                "Est. ADR × Occ",
                f"R${row['est_adr_brl']:,.0f} × {(row.get('est_occupancy') or 0) * 100:.1f}%",
            )
            st.metric(
                "Composite",
                f"{row['composite_score']:.3f}" if pd.notna(row.get("composite_score")) else "—",
            )

        st.subheader("USD IRR scenarios (10y hold)")
        scen = pd.DataFrame(
            {
                "Scenario": ["Bear (BRL weak)", "Base", "Bull (BRL strong)"],
                "IRR": [
                    row.get("usd_irr_10y_bear") or 0,
                    row.get("usd_irr_10y_base") or 0,
                    row.get("usd_irr_10y_bull") or 0,
                ],
            }
        )
        scen["IRR_pct"] = scen["IRR"] * 100
        st.bar_chart(scen.set_index("Scenario")["IRR_pct"])

with tab_assumptions:
    st.markdown(
        """
        **Current model constants** live in `src/fortaleza/model/composite.py`.
        Edit them there and re-run `fortaleza score` to update the scores table.

        Key levers to tune first:
        - `ACQUISITION_COST_PCT` (ITBI + registro)
        - `PLATFORM_FEE_PCT`, `DEFAULT_MGMT_PCT`
        - `APPRECIATION_PCT`, `RENT_GROWTH_PCT`
        - `TAX_RATE_EFFECTIVE` — needs a CE contador's input before relying on it
        - `REMITTANCE_FX_HAIRCUT` — 2% for banks, ~0.5% if using Wise/Remessa Online
        - Bairro-level `str_demand_score` in `data/seeds/bairros.yaml`
        """
    )
    st.code(
        """
        # Example: bump management fee and re-score
        # src/fortaleza/model/composite.py
        DEFAULT_MGMT_PCT = 0.25    # was 0.22

        # Then:
        $ fortaleza score
        """,
        language="python",
    )
