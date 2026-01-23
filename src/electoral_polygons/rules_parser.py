from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# -----------------------------
# Public API
# -----------------------------

def parse_sv(xlsx_path: str, judet: str = "B", sv: int = 1, sheet_name: int | str = 0) -> Dict[str, Any]:
    """
    Parse one SV block from the RSV XLSX into a structured dict.

    This function is intentionally robust to:
      - Full national XLSX vs a filtered Bucharest-only XLSX
      - "SV header rows" where Arteră is empty but polling place fields exist
      - Rule strings containing # separators and mixed parity/ranges

    Returns a dict with:
      - judet, uat, sv
      - polling_place (sediu/adresa/etc)
      - rules: list of {cod_artera, street, specs, raw}
    """
    df = load_rsv_xlsx(xlsx_path=xlsx_path, sheet_name=sheet_name)

    # Always filter in code (even if dev file is already filtered)
    df = df[df["JUDET"].astype(str).str.strip() == str(judet)].copy()

    # Preserve original order
    df = df.reset_index(drop=False).rename(columns={"index": "_row"})

    # Forward-fill SV to assign rows to SV blocks
    df["sv_ffill"] = pd.to_numeric(df["Nr SV"], errors="coerce").ffill()

    target = df[df["sv_ffill"] == float(sv)].copy()
    if target.empty:
        raise ValueError(f"SV {sv} not found for JUDET={judet} in {xlsx_path}")

    # Identify "header row" (usually the row where Nr SV is explicitly set)
    # In your file: that row often has Arteră empty and Număr imobil like 'nr. -#'
    header_candidates = target[pd.to_numeric(target["Nr SV"], errors="coerce") == float(sv)]
    header_row = header_candidates.iloc[0] if not header_candidates.empty else target.iloc[0]

    # Rule rows: Arteră must be non-empty
    rules_df = target[target["Arteră"].notna() & (target["Arteră"].astype(str).str.strip() != "")].copy()

    # Build output
    out: Dict[str, Any] = {
        "judet": str(judet),
        "uat": _safe_str(header_row.get("UAT")),
        "sv": int(sv),
        "polling_place": {
            "sediu_sv": _safe_str(header_row.get("Sediu SV")),
            "adresa_sv": _safe_str(header_row.get("Adresa SV")),
            "adresa_sv_descriptiva": _safe_str(header_row.get("Adresa SV descriptivă")),
            "localitate_sat": _safe_str(header_row.get("Localitate componentă/ Sat aparținător")),
        },
        "rules": [],
        "source": {
            "xlsx_path": xlsx_path,
            "sheet_name": sheet_name,
        },
    }

    for _, row in rules_df.sort_values("_row").iterrows():
        cod_artera = row.get("Cod Arteră")
        street = _safe_str(row.get("Arteră"))
        low_street = (street or "").lower()
        if low_street.startswith("domiciliul"):
            out.setdefault("notes", []).append({"street": street, "raw": rule_text})
            continue

        rule_text = _safe_str(row.get("Număr imobil / Alfabetic"))

        # Some rows are not actually streets (e.g., "Domiciliul pe raza sec. 1 (D)")
        # We still keep them, but they will parse as integral or empty specs.
        specs = parse_rule_cell(rule_text)

        out["rules"].append(
            {
                "cod_artera": _safe_int_or_none(cod_artera),
                "street": street,
                "specs": specs,
                "raw": rule_text,
            }
        )

    return out


def load_rsv_xlsx(xlsx_path: str, sheet_name: int | str = 0) -> pd.DataFrame:
    """
    Load RSV XLSX using column names (not positions).
    """
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=object)

    # Normalize column names (strip whitespace)
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        "JUDET",
        "UAT",
        "Nr SV",
        "Sediu SV",
        "Adresa SV",
        "Adresa SV descriptivă",
        "Localitate componentă/ Sat aparținător",
        "Cod Arteră",
        "Arteră",
        "Număr imobil / Alfabetic",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in XLSX: {missing}")

    return df


# -----------------------------
# Rule parsing
# -----------------------------

_RANGE_RE = re.compile(
    r"nr\.\s*([0-9]+(?:\s*[A-Za-z])?(?:\s*BIS)?)\s*-\s*([0-9]+(?:\s*[A-Za-z])?(?:\s*BIS)?)",
    flags=re.IGNORECASE,
)
_SINGLE_RE = re.compile(
    r"nr\.\s*([0-9]+(?:\s*[A-Za-z])?(?:\s*BIS)?)",
    flags=re.IGNORECASE,
)


def parse_rule_cell(text: str) -> List[Dict[str, Any]]:
    """
    Parse the 'Număr imobil / Alfabetic' cell into a list of specs.

    Input examples:
      - "integral#"
      - "numere impare nr. 75 -109#"
      - "numere impare nr. 111 -119# numere pare nr. 64 -72#"
      - "numere impare nr. 21 -299 BIS# nr. 301 -319# nr. 319 B#"

    Output spec examples:
      {"kind":"integral"}
      {"parity":"odd","ranges":[("75","109")],"singles":[],"raw":"..."}
    """
    t = (text or "").strip()
    if not t:
        return []

    # Split by '#' separators (your file uses these heavily)
    parts = [p.strip() for p in t.split("#") if p.strip()]
    specs: List[Dict[str, Any]] = []

    for part in parts:
        p = part.strip()
        low = p.lower()

        # Ignore the typical header cell "nr. -"
        if "nr." in low and "-" in low and re.search(r"nr\.\s*-\s*$", low):
            continue
        if low in {"nr. -", "nr.-"}:
            continue

        if "integral" in low:
            specs.append({"kind": "integral"})
            continue

        parity: Optional[str] = None
        if "numere impare" in low:
            parity = "odd"
        elif "numere pare" in low:
            parity = "even"

        ranges = [(a.strip(), b.strip()) for a, b in _RANGE_RE.findall(p)]

        # Singles: collect all nr. X that are not part of a range
        singles_all = [s.strip() for s in _SINGLE_RE.findall(p)]
        singles = _subtract_range_endpoints(singles_all, ranges)

        spec: Dict[str, Any] = {
            "kind": "numbers",
            "parity": parity,            # can be None if unspecified
            "ranges": ranges,            # list of (start,end) strings
            "singles": singles,          # list of strings
            "raw": p,
        }

        # If it contained neither range nor single, keep raw so we can debug later
        # (some lines can be special notes)
        if not ranges and not singles and parity is None:
            spec["kind"] = "note"

        specs.append(spec)

    return specs


def _subtract_range_endpoints(singles: List[str], ranges: List[Tuple[str, str]]) -> List[str]:
    """
    Remove occurrences of range endpoints from the singles list if they appear.
    E.g. "nr. 75 -109" will produce singles ["75","109"] via regex, and we don't want duplicates.
    """
    if not ranges:
        return singles

    endpoints = set()
    for a, b in ranges:
        endpoints.add(_norm_num_token(a))
        endpoints.add(_norm_num_token(b))

    out: List[str] = []
    for s in singles:
        if _norm_num_token(s) in endpoints:
            continue
        out.append(s)
    return out


def _norm_num_token(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().upper())


# -----------------------------
# Small helpers
# -----------------------------

def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, float) and pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None


def _safe_int_or_none(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, float) and pd.isna(x):
        return None
    try:
        return int(str(x).strip())
    except Exception:
        return None
