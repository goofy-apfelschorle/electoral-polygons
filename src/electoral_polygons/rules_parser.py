from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# -----------------------------
# Public API
# -----------------------------

_PARENS_RE = re.compile(r"\((.*?)\)")
_SPLIT_ALIASES_RE = re.compile(r"[;,/]|(?:\s+si\s+)|(?:\s+și\s+)", flags=re.IGNORECASE)


def expand_street_aliases(raw: Any) -> List[str]:
    """
    Expand an 'Arteră' cell into multiple street-name candidates.

    Handles patterns like:
      "Strada X (Strada Y, Strada Z)"
      "Strada X (Strada Y)"
      "Strada X, Strada Y"
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []

    s = str(raw).strip()
    if not s:
        return []

    out: List[str] = []

    # 1) outside parentheses
    outside = re.sub(r"\s*\(.*?\)\s*", " ", s).strip()
    if outside:
        out.append(outside)

    # 2) inside parentheses, split by comma/; / "si/și"
    for grp in _PARENS_RE.findall(s):
        for part in _SPLIT_ALIASES_RE.split(grp):
            p = part.strip()
            if p:
                out.append(p)

    # 3) also split whole string (covers no-parentheses variants)
    for part in _SPLIT_ALIASES_RE.split(s):
        p = part.strip()
        if p:
            out.append(p)

    # de-dupe preserve order
    seen = set()
    uniq: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)

    return uniq


def parse_sv(
    xlsx_path: str,
    judet: str = "B",
    sv: int = 1,
    sheet_name: int | str = 0,
) -> Dict[str, Any]:
    """
    Parse one SV block from the RSV XLSX into a structured dict.

    Returns a dict with:
      - judet, uat, sv
      - polling_place (sediu/adresa/etc)
      - rules: list of {cod_artera, street, street_aliases, specs, raw}
    """
    df = load_rsv_xlsx(xlsx_path=xlsx_path, sheet_name=sheet_name)

    df = df[df["JUDET"].astype(str).str.strip() == str(judet)].copy()
    df = df.reset_index(drop=False).rename(columns={"index": "_row"})

    df["sv_ffill"] = pd.to_numeric(df["Nr SV"], errors="coerce").ffill()

    target = df[df["sv_ffill"] == float(sv)].copy()
    if target.empty:
        raise ValueError(f"SV {sv} not found for JUDET={judet} in {xlsx_path}")

    header_candidates = target[pd.to_numeric(target["Nr SV"], errors="coerce") == float(sv)]
    header_row = header_candidates.iloc[0] if not header_candidates.empty else target.iloc[0]

    rules_df = target[target["Arteră"].notna() & (target["Arteră"].astype(str).str.strip() != "")].copy()

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
        street_raw = row.get("Arteră")
        street = _safe_str(street_raw)
        rule_text = _safe_str(row.get("Număr imobil / Alfabetic"))

        low_street = (street or "").lower()
        if low_street.startswith("domiciliul"):
            out.setdefault("notes", []).append({"street": street, "raw": rule_text})
            continue

        specs = parse_rule_cell(rule_text)

        street_aliases = expand_street_aliases(street_raw)
        # Keep street field as the “primary” human-readable one
        # but always include aliases for matching
        if street and street not in street_aliases:
            street_aliases.insert(0, street)

        out["rules"].append(
            {
                "cod_artera": _safe_int_or_none(cod_artera),
                "street": street,
                "street_aliases": street_aliases,
                "specs": specs,
                "raw": rule_text,
            }
        )

    return out


def load_rsv_xlsx(xlsx_path: str, sheet_name: int | str = 0) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=object)
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
    t = (text or "").strip()
    if not t:
        return []

    parts = [p.strip() for p in t.split("#") if p.strip()]
    specs: List[Dict[str, Any]] = []

    for part in parts:
        p = part.strip()
        low = p.lower()

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

        singles_all = [s.strip() for s in _SINGLE_RE.findall(p)]
        singles = _subtract_range_endpoints(singles_all, ranges)

        spec: Dict[str, Any] = {
            "kind": "numbers",
            "parity": parity,
            "ranges": ranges,
            "singles": singles,
            "raw": p,
        }

        if not ranges and not singles and parity is None:
            spec["kind"] = "note"

        specs.append(spec)

    return specs


def _subtract_range_endpoints(singles: List[str], ranges: List[Tuple[str, str]]) -> List[str]:
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
