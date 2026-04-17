"""Classify STR legality risk as green/yellow/red.

Heuristic only — real answers come from reading the condomínio's
convenção. This classifier gives a first-pass signal to triage listings.
"""
from __future__ import annotations

import re

# Keywords suggesting STR is explicitly welcomed
_GREEN_RE = re.compile(
    r"\b(temporada|airbnb|booking|short[\s-]?stay|locação por temporada|"
    r"flat|apart[\s-]?hotel|apart[\s-]?hotel|pool\s*letting)\b",
    re.IGNORECASE,
)

# Keywords suggesting STR is disallowed or non-viable
_RED_RE = re.compile(
    r"(não\s*aceita\s*temporada|apenas\s*(moradia|família|fam[ií]lia)|"
    r"sem\s*airbnb|proibid[oa].{0,20}temporada|somente\s*residencial|"
    r"veda[çc][ãa]o.{0,30}temporada)",
    re.IGNORECASE,
)


def classify(product_type: str | None, description: str | None, amenities: str | None) -> str:
    """Return 'green', 'yellow', or 'red'."""
    # Condohotel / pool-letting product type is green by definition
    if product_type == "condohotel":
        return "green"

    text = " ".join(x for x in (description or "", amenities or "") if x)

    if _RED_RE.search(text):
        return "red"

    if _GREEN_RE.search(text):
        return "green"

    return "yellow"
