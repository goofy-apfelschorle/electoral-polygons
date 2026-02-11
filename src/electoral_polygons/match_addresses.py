# src/electoral_polygons/match_addresses.py
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import unicodedata
import re


# ----------------------------
# Street normalization (LOOSE but safe)
# ----------------------------

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


def tokens_subset(needle: str, haystack: str) -> bool:
    """Order-insensitive fallback: all needle tokens must be present in haystack."""
    n = set(needle.split())
    h = set(haystack.split())
    return bool(n) and n.issubset(h)


def _strip_diacritics(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


_REPL: List[Tuple[str, str]] = [
    # road types
    (r"\bcal-\b|\bcal\.\b|\bcal\b", "calea"),
    (r"\bstr-\b|\bstr\.\b|\bstr\b", "strada"),
    (r"\baleea\b|\balee\b|\bal\.\b|\bal\b", "aleea"),
    (r"\bsoseaua\b|\bsosea\b|\bsos\.\b|\bsos\b", "soseaua"),
    (r"\bbulevardul\b|\bbulevard\b|\bblvd\b|\bblv\b|\bbd\.\b|\bbd\b", "bulevardul"),
    (r"\bpiata\b|\bp-ta\b|\bp\.\b", "piata"),

    # ranks/titles
    # NOTE: handle "G-ral" AFTER hyphen-splitting -> it becomes "g ral"
    (r"\bg\s*ral\b|\bg-ral\b|\bgeneral\b|\bgen\.\b|\bgen\b", "general"),
    (r"\bing\.\b|\bing\b|\binginer\b", "inginer"),
    (r"\bdr\.\b|\bdr\b|\bdoctor\b", "doctor"),
    (r"\bprof\.\b|\bprof\b|\bprofesor\b", "profesor"),
    (r"\bcol\.\b|\bcol\b|\bcolonel\b", "colonel"),
    (r"\blt\.\b|\blt\b|\blocot\b|\blocotenent\b", "locotenent"),
    (r"\bmr\.\b|\bmr\b|\bmaior\b", "maior"),
    (r"\bcap\.\b|\bcap\b|\bcapitan\b", "capitan"),

    # “roles”
    (r"\berou\b", "erou"),
    # NOTE: handle "Serg." -> "serg" and "Sg." -> "sg" after punctuation cleanup
    (r"\bserg\.\b|\bserg\b|\bsg\.\b|\bsg\b|\bsgt\.\b|\bsergent\b", "sergent"),
    (r"\bpoet\b", "poet"),
]

_RANK_TOKENS = {
    "general",
    "colonel",
    "locotenent",
    "maior",
    "capitan",
    "doctor",
    "profesor",
    "inginer",
    "sergent",
    "poet",
    "erou",
}


def drop_rank_tokens(s: str) -> str:
    toks = [t for t in (s or "").split() if t and t not in _RANK_TOKENS]
    return " ".join(toks)


def canon_street_loose(name: Any) -> str:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = str(name).strip()
    if not s:
        return ""

    # remove parenthetical aliases entirely here (parser already extracted them)
    s = re.sub(r"\s*\(.*?\)", "", s)

    s = _strip_diacritics(s).lower()

    # normalize separators & punctuation to spaces
    s = s.replace("’", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"[-_/]", " ", s)
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()

    for pat, rep in _REPL:
        s = re.sub(pat, rep, s)

    s = re.sub(r"\s+", " ", s).strip()
    return s


# ----------------------------
# Diagnostics helpers
# ----------------------------

def _token_set(s: str) -> set[str]:
    return set((s or "").split())


def _best_street_candidates(
    osm_norm_streets: List[str],
    needle_norms: List[str],
    topk: int = 8,
) -> List[Tuple[str, float]]:
    needles = [_token_set(n) for n in needle_norms if n]
    if not needles:
        return []

    scored: List[Tuple[str, float]] = []
    for st in osm_norm_streets:
        ts = _token_set(st)
        if not ts:
            continue
        best = 0.0
        for nt in needles:
            inter = len(nt & ts)
            union = len(nt | ts)
            if union:
                best = max(best, inter / union)
        if best > 0:
            scored.append((st, best))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:topk]


# ----------------------------
# House number parsing
# ----------------------------

# Accept ranges where endpoints may have letters (2B-30, 111A–131B, etc.)
_HN_RANGE_RE = re.compile(r"^\s*(\d+)\s*[A-Za-z]?\s*[-–]\s*(\d+)\s*[A-Za-z]?\s*$")


def housenumber_info(hn) -> Tuple[set[int], Optional[str], bool]:
    """
    Returns (covered_numbers_set, inferred_parity, is_range)

    - "2B" -> {2}
    - "111-131" -> {111,112,...131} or step 2 if inferred parity is clear
    """
    if hn is None or (isinstance(hn, float) and pd.isna(hn)):
        return set(), None, False

    s = str(hn).strip()
    if not s:
        return set(), None, False

    # pure int
    if re.fullmatch(r"\d+", s):
        n = int(s)
        return {n}, None, False

    # range like "111-131" / "111–131" / "111A-131B"
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

    # messy stuff like 109A, 109/1 -> keep leading digits
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

    IMPORTANT FIX:
    - If duplicates exist, prefer RANGE features (111-131) over single-number points,
      because range features carry more coverage for SV rules.
    - Still prefer way > relation > node (geometry quality).
    """
    if gdf.empty:
        return gdf

    g = gdf.copy()

    # ensure housenumber_int exists (digits only)
    if hn_int_col not in g.columns:
        g[hn_int_col] = (
            g["addr_housenumber"].astype(str).str.extract(r"(\d+)", expand=False)
        )
        g[hn_int_col] = pd.to_numeric(g[hn_int_col], errors="coerce")

    def pref_score(x: str) -> int:
        s = "" if x is None else str(x)
        if s.startswith("way:"):
            return 0
        if s.startswith("relation:"):
            return 1
        if s.startswith("node:"):
            return 2
        return 3

    # prefer range features if available
    if "_hn_is_range" in g.columns:
        g["_range_pref"] = g["_hn_is_range"].astype(int)  # 1 better than 0
    else:
        g["_range_pref"] = 0

    if id_col in g.columns:
        g["_geom_pref"] = g[id_col].apply(pref_score)
        # sort: street, hn, range-first (desc), geom-best (asc)
        g = g.sort_values(
            [street_col, hn_int_col, "_range_pref", "_geom_pref", id_col],
            ascending=[True, True, False, True, True],
            kind="mergesort",
        )
    else:
        g = g.sort_values(
            [street_col, hn_int_col, "_range_pref"],
            ascending=[True, True, False],
            kind="mergesort",
        )

    g = g.drop_duplicates(subset=[street_col, hn_int_col], keep="first")

    g = g.drop(columns=[c for c in ["_range_pref", "_geom_pref"] if c in g.columns])
    return g


# ----------------------------
# Main matcher
# ----------------------------

def match_sv_addresses(
    addresses_gpkg: str,
    sv_parsed: Dict[str, Any],
    layer: str = "addresses",
    debug: bool = False,
) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(addresses_gpkg, layer=layer).copy()

    # canonicalize OSM street names
    gdf["_street_norm"] = gdf["addr_street"].apply(canon_street_loose)
    osm_streets_unique = sorted(set(gdf["_street_norm"].dropna().astype(str).tolist()))

    hn_infos = gdf["addr_housenumber"].apply(housenumber_info)
    gdf["_hn_set"] = hn_infos.apply(lambda t: t[0])
    gdf["_hn_parity_inferred"] = hn_infos.apply(lambda t: t[1])
    gdf["_hn_is_range"] = hn_infos.apply(lambda t: t[2])

    matched_frames: List[gpd.GeoDataFrame] = []
    sv_id = sv_parsed.get("sv")

    for rule in sv_parsed.get("rules", []):
        aliases: List[str] = rule.get("street_aliases") or []
        if not aliases:
            street = rule.get("street")
            if street:
                aliases = [street]

        specs = rule.get("specs", [])
        if not specs:
            continue

        # Normalize aliases and also add rank-stripped variants
        alias_norms = [canon_street_loose(a) for a in aliases if a]
        alias_norms = [a for a in alias_norms if a]
        if not alias_norms:
            continue

        alias_norms2 = alias_norms + [drop_rank_tokens(a) for a in alias_norms if a]
        seen = set()
        alias_norms = []
        for a in alias_norms2:
            if a and a not in seen:
                seen.add(a)
                alias_norms.append(a)

        # 1) equality
        alias_set = set(alias_norms)
        street_gdf = gdf[gdf["_street_norm"].isin(alias_set)].copy()

        # 2) token-in-order
        if street_gdf.empty:
            for needle in alias_norms:
                if not needle:
                    continue
                mask = gdf["_street_norm"].apply(lambda s: tokens_in_order(needle, s))
                cand = gdf[mask]
                if not cand.empty:
                    street_gdf = cand.copy()
                    break

        # 3) token-subset
        if street_gdf.empty:
            for needle in alias_norms:
                if not needle:
                    continue
                mask = gdf["_street_norm"].apply(lambda s: tokens_subset(needle, s))
                cand = gdf[mask]
                if not cand.empty:
                    street_gdf = cand.copy()
                    break

        if street_gdf.empty:
            if debug:
                sugg = _best_street_candidates(osm_streets_unique, alias_norms, topk=8)
                print(f"[MISS street] SV{sv_id} | rule={rule.get('street')}")
                print(f"  aliases={aliases}")
                print(f"  norm_aliases={alias_norms}")
                print(f"  suggestions={sugg}")
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

                # Parity checks: keep as-is for normal points.
                # For interval features, parity is still respected, but will not be
                # used to *reject* if range overlap is clearly true.
                inferred = row["_hn_parity_inferred"]
                is_range = bool(row.get("_hn_is_range"))

                # range overlap check first (important for OSM intervals)
                if ranges:
                    overlaps = False
                    for start, end in ranges:
                        try:
                            s, e = int(start), int(end)
                        except Exception:
                            continue
                        lo, hi = (s, e) if s <= e else (e, s)
                        if any(lo <= n <= hi for n in hn_set):
                            overlaps = True
                            break
                    if not overlaps:
                        return False
                else:
                    # if no ranges, we may still match by parity/singles
                    overlaps = True

                # if it’s an OSM interval and it overlaps, don’t over-reject on parity
                if not is_range:
                    if inferred == "even" and parity == "odd":
                        return False
                    if inferred == "odd" and parity == "even":
                        return False

                    if parity == "odd" and not any(n % 2 == 1 for n in hn_set):
                        return False
                    if parity == "even" and not any(n % 2 == 0 for n in hn_set):
                        return False
                else:
                    # interval: parity filter only if it would *definitely* contradict
                    if inferred == "even" and parity == "odd":
                        return False
                    if inferred == "odd" and parity == "even":
                        return False
                    # otherwise let overlap drive the match

                # singles constraint
                if singles_int and hn_set.isdisjoint(singles_int):
                    return False

                return True

            nums = street_gdf[street_gdf.apply(row_ok, axis=1)].copy()

            if nums.empty:
                if debug:
                    print(f"[MISS numbers] SV{sv_id} | rule={rule.get('street')} | aliases={aliases}")
                    print(f"  spec={spec}")
                    try:
                        sample = street_gdf["addr_housenumber"].astype(str).head(25).tolist()
                        print(f"  sample_osm_hn={sample}")
                    except Exception:
                        pass
                continue

            matched_frames.append(nums)

    if not matched_frames:
        out = gdf.iloc[0:0].copy()
    else:
        out = pd.concat(matched_frames, ignore_index=True)

    # IMPORTANT: dedupe AFTER matching, and prefer interval features
    out = dedupe_by_address(out)
    out["sv"] = sv_id
    return out
