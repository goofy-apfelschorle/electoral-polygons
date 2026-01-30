from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import unicodedata
import re


# ----------------------------
# Street normalization (STRICT)
# Keeps street-type tokens (Aleea/Strada/etc.)
# Only changes:
# - strips diacritics
# - expands a few abbreviations (Lt., Gen., Ing., Mr., Dr., Av.)
# - normalizes punctuation/whitespace
# ----------------------------

_ABBR_MAP = {
    "lt": "locotenent",
    "lt.": "locotenent",
    "gen": "general",
    "gen.": "general",
    "ing": "inginer",
    "ing.": "inginer",
    "mr": "maior",
    "mr.": "maior",
    "dr": "doctor",
    "dr.": "doctor",
    "av": "aviator",
    "av.": "aviator",
}


def tokens_in_order(needle: str, haystack: str) -> bool:
    n = needle.split()
    h = haystack.split()
    if not n:
        return False
    j = 0
    for tok in h:
        if tok == n[j]:
            j += 1
            if j == len(n):
                return True
    return False


def _strip_diacritics(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def canon_street_strict(name) -> str:
    """
    Strict canonical form:
      - keeps street type words (Strada/Aleea/etc.) to avoid collisions
      - strips diacritics
      - expands specific abbreviations (Lt/Gen/Ing/Mr/Dr/Av)
      - normalizes punctuation/whitespace
      - removes parenthetical aliases: "X (Y)" -> "X"
    """
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = str(name).strip()

    # remove parenthetical aliases
    s = re.sub(r"\s*\(.*?\)", "", s)

    s = _strip_diacritics(s).lower()

    # punctuation -> spaces
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()

    toks: List[str] = []
    for t in s.split(" "):
        if not t:
            continue
        toks.append(_ABBR_MAP.get(t, t))

    return " ".join(toks)


# ----------------------------
# House number parsing
# ----------------------------

_HN_RANGE_RE = re.compile(r"^\s*(\d+)\s*[-–]\s*(\d+)\s*$")


def housenumber_info(hn) -> Tuple[set[int], Optional[str], bool]:
    """
    Returns (covered_numbers_set, inferred_parity, is_range)

    inferred_parity:
      - "odd"  if a-b and both endpoints odd  (treat range as odd-only)
      - "even" if a-b and both endpoints even (treat range as even-only)
      - None   otherwise (treat as all numbers)
    """
    if hn is None or (isinstance(hn, float) and pd.isna(hn)):
        return set(), None, False

    s = str(hn).strip()

    # pure int
    if re.fullmatch(r"\d+", s):
        n = int(s)
        return {n}, None, False

    # range "75-79"
    m = _HN_RANGE_RE.match(s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        lo, hi = (a, b) if a <= b else (b, a)

        inferred_parity: Optional[str] = None
        if (lo % 2 == 0) and (hi % 2 == 0):
            inferred_parity = "even"
            covered = set(range(lo, hi + 1, 2))
        elif (lo % 2 == 1) and (hi % 2 == 1):
            inferred_parity = "odd"
            covered = set(range(lo, hi + 1, 2))
        else:
            covered = set(range(lo, hi + 1))

        return covered, inferred_parity, True

    # messy stuff like 109A, 109/1, etc -> keep leading digits
    m = re.match(r"(\d+)", s)
    if m:
        n = int(m.group(1))
        return {n}, None, False

    return set(), None, False


# ----------------------------
# Dedupe (keep best geom)
# ----------------------------

def dedupe_by_address(
    gdf: gpd.GeoDataFrame,
    street_col: str = "_street_norm",
    hn_int_col: str = "addr_housenumber_int",
    id_col: str = "addr_id",
) -> gpd.GeoDataFrame:
    """
    Keep exactly 1 feature per (street_norm, housenumber_int).
    Prefer building geometries (ways) over relations over nodes when addr_id is like 'way:...' / 'node:...'.
    """
    if gdf.empty:
        return gdf

    # ensure housenumber_int exists; if not, best-effort extract from addr_housenumber
    if hn_int_col not in gdf.columns:
        gdf[hn_int_col] = (
            gdf["addr_housenumber"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
        )
        gdf[hn_int_col] = pd.to_numeric(gdf[hn_int_col], errors="coerce")

    def pref_score(x: str) -> int:
        s = "" if x is None else str(x)
        if s.startswith("way:"):
            return 0
        if s.startswith("relation:"):
            return 1
        if s.startswith("node:"):
            return 2
        return 3

    g = gdf.copy()

    if id_col in g.columns:
        g["_pref"] = g[id_col].apply(pref_score)
        g = g.sort_values([street_col, hn_int_col, "_pref", id_col], kind="mergesort")
    else:
        g = g.sort_values([street_col, hn_int_col], kind="mergesort")

    g = g.drop_duplicates(subset=[street_col, hn_int_col], keep="first")

    if "_pref" in g.columns:
        g = g.drop(columns=["_pref"])

    return g


# ----------------------------
# Main matcher
# ----------------------------

def match_sv_addresses(
    addresses_gpkg: str,
    sv_parsed: Dict[str, Any],
    layer: str = "addresses",
) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(addresses_gpkg, layer=layer).copy()

    # IMPORTANT: use strict canonical street normalization (with abbr expansion)
    gdf["_street_norm"] = gdf["addr_street"].apply(canon_street_strict)

    hn_infos = gdf["addr_housenumber"].apply(housenumber_info)
    gdf["_hn_set"] = hn_infos.apply(lambda t: t[0])
    gdf["_hn_parity_inferred"] = hn_infos.apply(lambda t: t[1])
    gdf["_hn_is_range"] = hn_infos.apply(lambda t: t[2])

    matched_frames: List[gpd.GeoDataFrame] = []

    for rule in sv_parsed.get("rules", []):
        street = rule.get("street")
        specs = rule.get("specs", [])
        if not street:
            continue

        street_norm = canon_street_strict(street)

        # 1) strict equality match
        street_gdf = gdf[gdf["_street_norm"] == street_norm].copy()

        # 2) fallback: token-in-order match (handles extra middle tokens like "aviator")
        if street_gdf.empty and street_norm:
            mask = gdf["_street_norm"].apply(lambda s: tokens_in_order(street_norm, s))
            street_gdf = gdf[mask].copy()

        if street_gdf.empty:
            continue

        print("RULE", rule.get("street"), "SPECS", rule.get("specs"))


        for spec in specs:
            kind = spec.get("kind")

            # integral: keep all addresses on this street
            if kind == "integral":
                matched_frames.append(street_gdf)
                continue

            if kind != "numbers":
                continue

            parity = spec.get("parity")
            ranges = spec.get("ranges", [])
            singles = spec.get("singles", [])

            singles_int: set[int] = set()
            for x in singles:
                try:
                    singles_int.add(int(x))
                except Exception:
                    pass

            def row_ok(row) -> bool:
                hn_set: set[int] = row["_hn_set"]
                if not hn_set:
                    return False

                # If the housenumber looks like an even-only or odd-only range,
                # enforce that before checking SV parity.
                inferred = row["_hn_parity_inferred"]
                if inferred == "even" and parity == "odd":
                    return False
                if inferred == "odd" and parity == "even":
                    return False

                # SV parity filter
                if parity == "odd" and not any(n % 2 == 1 for n in hn_set):
                    return False
                if parity == "even" and not any(n % 2 == 0 for n in hn_set):
                    return False

                # range overlap
                if ranges:
                    ok = False
                    for start, end in ranges:
                        try:
                            s, e = int(start), int(end)
                        except Exception:
                            continue
                        lo, hi = (s, e) if s <= e else (e, s)
                        if any(lo <= n <= hi for n in hn_set):
                            ok = True
                            break
                    if not ok:
                        return False

                # singles
                if singles_int and hn_set.isdisjoint(singles_int):
                    return False

                return True

            nums = street_gdf[street_gdf.apply(row_ok, axis=1)].copy()
            if not nums.empty:
                matched_frames.append(nums)

    if not matched_frames:
        out = gdf.iloc[0:0].copy()
    else:
        out = pd.concat(matched_frames, ignore_index=True)

    # Collapse duplicates: one row per (street, housenumber_int)
    out = dedupe_by_address(out)

    out["sv"] = sv_parsed.get("sv")
    return out
