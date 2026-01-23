from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
import geopandas as gpd
import pandas as pd
import unicodedata
import re


def normalize_street(name) -> str:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    name = str(name)

    # remove parenthetical aliases
    name = re.sub(r"\s*\(.*?\)", "", name)

    name = name.lower()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r"\s+", " ", name).strip()
    return name


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

def dedupe_by_address(
    gdf: gpd.GeoDataFrame,
    street_col: str = "_street_norm",
    hn_int_col: str = "addr_housenumber_int",
    id_col: str = "addr_id",
) -> gpd.GeoDataFrame:
    """
    Keep exactly 1 feature per (street_norm, housenumber_int).
    Prefer building geometries (ways) over nodes when addr_id is like 'way:...' / 'node:...'.
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

    # preference score: 0 = best
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

    # drop duplicates keeping the preferred one
    g = g.drop_duplicates(subset=[street_col, hn_int_col], keep="first")

    # clean helper col
    if "_pref" in g.columns:
        g = g.drop(columns=["_pref"])

    return g

def dedupe_by_address(
    gdf: gpd.GeoDataFrame,
    street_col: str = "_street_norm",
    hn_int_col: str = "addr_housenumber_int",
    id_col: str = "addr_id",
) -> gpd.GeoDataFrame:
    """
    Keep exactly 1 feature per (street_norm, housenumber_int).
    Prefer building geometries (ways) over nodes when addr_id is like 'way:...' / 'node:...'.
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

    # preference score: 0 = best
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

    # drop duplicates keeping the preferred one
    g = g.drop_duplicates(subset=[street_col, hn_int_col], keep="first")

    # clean helper col
    if "_pref" in g.columns:
        g = g.drop(columns=["_pref"])

    return g

def dedupe_by_address(
    gdf: gpd.GeoDataFrame,
    street_col: str = "_street_norm",
    hn_int_col: str = "addr_housenumber_int",
    id_col: str = "addr_id",
) -> gpd.GeoDataFrame:
    """
    Keep exactly 1 feature per (street_norm, housenumber_int).
    Prefer building geometries (ways) over nodes when addr_id is like 'way:...' / 'node:...'.
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

    # preference score: 0 = best
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

    # drop duplicates keeping the preferred one
    g = g.drop_duplicates(subset=[street_col, hn_int_col], keep="first")

    # clean helper col
    if "_pref" in g.columns:
        g = g.drop(columns=["_pref"])

    return g


def match_sv_addresses(
    addresses_gpkg: str,
    sv_parsed: Dict[str, Any],
    layer: str = "addresses",
) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(addresses_gpkg, layer=layer)

    # Precompute once
    gdf["_street_norm"] = gdf["addr_street"].apply(normalize_street)

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

        street_norm = normalize_street(street)

        # 1) strict equality match
        street_gdf = gdf[gdf["_street_norm"] == street_norm].copy()

        # 2) fallback: contains match (helps when OSM has extra tokens)
        if street_gdf.empty and street_norm:
            street_gdf = gdf[gdf["_street_norm"].str.contains(street_norm, na=False)].copy()

        if street_gdf.empty:
            continue

        for spec in specs:
            kind = spec.get("kind")

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

                # IMPORTANT: if the housenumber looks like an even-only or odd-only range,
                # enforce that before checking SV parity.
                inferred = row["_hn_parity_inferred"]
                if inferred == "even" and parity == "odd":
                    return False
                if inferred == "odd" and parity == "even":
                    return False

                # parity filter (SV parity)
                if parity == "odd" and not any(n % 2 == 1 for n in hn_set):
                    return False
                if parity == "even" and not any(n % 2 == 0 for n in hn_set):
                    return False

                # range filter (overlap is fine; inferred parity already handled above)
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

                # singles filter
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
