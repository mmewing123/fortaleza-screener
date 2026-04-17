"""Load bairro reference data (FipeZap proxies) from YAML into DuckDB."""
from __future__ import annotations

from pathlib import Path
import yaml

from ..config import SEEDS_DIR
from ..db import connect


def load_bairros(seed_path: Path = SEEDS_DIR / "bairros.yaml") -> int:
    data = yaml.safe_load(seed_path.read_text(encoding="utf-8"))
    as_of = data.get("as_of_date")
    bairros = data.get("bairros", [])

    con = connect()
    for b in bairros:
        b["as_of_date"] = as_of
        con.execute(
            """
            INSERT OR REPLACE INTO bairros VALUES (
                $slug, $name, $tier, $fipezap_price_per_m2, $fipezap_rent_per_m2,
                $str_demand_score, $notes, $as_of_date
            )
            """,
            b,
        )
    con.close()
    return len(bairros)


if __name__ == "__main__":
    n = load_bairros()
    print(f"Loaded {n} bairros")
