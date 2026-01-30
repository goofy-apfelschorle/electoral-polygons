from __future__ import annotations

from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import LineString
from shapely.ops import unary_union

from electoral_polygons.rules_parser import parse_sv
from electoral_polygons.match_addresses import match_sv_addresses


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _rep_points(gdf: gpd.GeoDataFrame) -> np.ndarray:
    pts = gdf.geometry.representative_point()
    return np.array([(p.x, p.y) for p in pts], dtype=float)


def _pairwise_distances(coords: np.ndarray) -> np.ndarray:
    diffs = coords[:, None, :] - coords[None, :, :]
    d2 = (diffs**2).sum(axis=2)
    return np.sqrt(d2)


def _nearest_neighbor_stats(coords: np.ndarray) -> tuple[float, float]:
    n = coords.shape[0]
    if n < 2:
        return 0.0, 0.0
    D = _pairwise_distances(coords)
    np.fill_diagonal(D, float("inf"))
    nn = D.min(axis=1)
    return float(np.median(nn)), float(np.quantile(nn, 0.90))


def _pca_ratio(coords: np.ndarray) -> float:
    n = coords.shape[0]
    if n < 2:
        return 1.0
    X = coords - coords.mean(axis=0, keepdims=True)
    cov = np.cov(X.T)
    eigvals = np.linalg.eigvalsh(cov)
    lam1 = float(eigvals[-1])
    lam2 = float(eigvals[-2]) if len(eigvals) >= 2 else 0.0
    return lam1 / (lam2 + 1e-9)


def _count_components(coords: np.ndarray, radius: float) -> int:
    n = coords.shape[0]
    if n == 0:
        return 0
    if n == 1:
        return 1
    if radius <= 0:
        return n

    D = _pairwise_distances(coords)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(n):
        js = np.where(D[i, i + 1 :] <= radius)[0]
        for j_off in js:
            union(i, i + 1 + int(j_off))

    return len({find(i) for i in range(n)})


def _mst_edges(coords: np.ndarray) -> list[tuple[int, int]]:
    n = coords.shape[0]
    if n <= 1:
        return []

    D = _pairwise_distances(coords)
    np.fill_diagonal(D, float("inf"))

    in_tree = np.zeros(n, dtype=bool)
    in_tree[0] = True
    best_dist = D[0].copy()
    best_parent = np.zeros(n, dtype=int)

    edges: list[tuple[int, int]] = []
    for _ in range(n - 1):
        cand = np.where(~in_tree)[0]
        j = int(cand[np.argmin(best_dist[cand])])
        p = int(best_parent[j])
        edges.append((p, j))
        in_tree[j] = True

        for k in np.where(~in_tree)[0]:
            if D[j, k] < best_dist[k]:
                best_dist[k] = D[j, k]
                best_parent[k] = j

    return edges


def build_auto_polygon(buildings: gpd.GeoDataFrame) -> tuple[object, dict]:
    coords = _rep_points(buildings)
    d50, d90 = _nearest_neighbor_stats(coords)
    R = _pca_ratio(coords)
    C = _count_components(coords, 1.5 * d50 if d50 > 0 else 0.0)

    if R >= 6 and C <= 2:
        mode = "corridor"
        w = clamp(0.5 * d50, 10.0, 35.0)
    elif R <= 3 and C == 1:
        mode = "blob"
        w = clamp(0.35 * d50, 6.0, 25.0)
    else:
        mode = "multi"
        w = clamp(0.8 * d50, 15.0, 60.0)

    b = clamp(0.15 * d50, 3.0, 12.0) if d50 > 0 else 6.0
    s = clamp(0.08 * d50, 1.0, 6.0) if d50 > 0 else 2.0

    buildings_union = unary_union(buildings.geometry)

    if mode == "blob":
        poly = buildings_union.buffer(w)
    else:
        edges = _mst_edges(coords)
        if not edges:
            poly = buildings_union.buffer(b)
        else:
            lines = [LineString([tuple(coords[u]), tuple(coords[v])]) for u, v in edges]
            skeleton = unary_union(lines)
            corridor = skeleton.buffer(w, cap_style=1, join_style=1)
            poly = corridor.union(buildings_union.buffer(b))

    poly = poly.simplify(s, preserve_topology=True)

    meta = {"mode": mode, "d50": d50, "d90": d90, "R": R, "C": C, "w": w, "b": b, "s": s}
    return poly, meta


def list_sv_ids_from_excel(xlsx_path: str, judet: str) -> list[int]:
    """
    Extract all SV ids for a county code (judet) from the raw XLSX.
    Tries to auto-detect the SV-id column.
    """
    df = pd.read_excel(xlsx_path, sheet_name=0, dtype=object)

    # Try to find a judet column by name
    judet_cols = [
        c
        for c in df.columns
        if isinstance(c, str)
        and c.strip().lower() in ("judet", "județ", "cod judet", "cod județ")
    ]
    if judet_cols:
        df_b = df[df[judet_cols[0]].astype(str).str.strip().str.upper() == judet.upper()].copy()
    else:
        # fallback: any column equals judet
        mask = False
        for c in df.columns:
            s = df[c].astype(str).str.strip().str.upper()
            mask = mask | (s == judet.upper())
        df_b = df[mask].copy()

    if df_b.empty:
        raise ValueError(f"No rows found for judet={judet} in {xlsx_path}")

    def to_num(series):
        return pd.to_numeric(series, errors="coerce")

    candidates = []
    for c in df_b.columns:
        nums = to_num(df_b[c])
        if nums.notna().sum() < 10:
            continue
        maxv = float(nums.max())
        nunique = int(nums.dropna().astype(int).nunique())
        if maxv >= 50 and nunique >= 10:
            name = str(c).lower()
            bonus = 0
            if "sv" in name:
                bonus += 5
            if "sect" in name:
                bonus += 4
            if "nr" in name or "num" in name:
                bonus += 2
            candidates.append((bonus, maxv, nunique, c))

    if not candidates:
        raise ValueError("Could not identify an SV id column automatically.")

    candidates.sort(reverse=True)
    sv_col = candidates[0][3]

    sv_ids = (
        pd.to_numeric(df_b[sv_col], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    sv_ids = sorted(set(sv_ids))
    print(f"[debug] Using SV column: {sv_col!r} -> {len(sv_ids)} SV ids (min={min(sv_ids)}, max={max(sv_ids)})")
    return sv_ids


def main() -> None:
    xlsx = "src/data/raw/RSV-17.11.2025-----Extras-partiale-BUC.xlsx"
    assets_gpkg = "src/src/electoral_polygons/assets/bucharest_osm_assets.gpkg"

    links_gpkg = "src/data/scratch/buildings_with_addresses.gpkg"
    links_layer = "building_address_links"
    buildings_layer = "buildings_enriched"

    out_gpkg = "src/data/scratch/all_sv_polygons_auto.gpkg"
    Path("src/data/scratch").mkdir(parents=True, exist_ok=True)

    # Load once
    links = gpd.read_file(links_gpkg, layer=links_layer).copy()
    b = gpd.read_file(links_gpkg, layer=buildings_layer).copy()

    # Build join key (keep it simple for now, same as you’ve used in selectors)
    links["addr_street"] = links["addr_street"].fillna("").astype(str).str.strip()
    links["addr_housenumber"] = links["addr_housenumber"].fillna("").astype(str).str.strip()
    links["key"] = links["addr_street"] + "|" + links["addr_housenumber"]

    b["_id_str"] = b["osm_id"].astype(str)

    polygons: list[dict] = []
    stats_rows: list[dict] = []

    sv_ids = list_sv_ids_from_excel(xlsx, judet="B")

    for sv in sv_ids:
        try:
            sv_parsed = parse_sv(xlsx_path=xlsx, judet="B", sv=sv)
        except Exception as e:
            stats_rows.append({"sv": sv, "status": "parse_failed", "error": str(e)})
            continue

        matched_addr = match_sv_addresses(
            addresses_gpkg=assets_gpkg,
            sv_parsed=sv_parsed,
            layer="addresses",
        )

        if matched_addr.empty:
            stats_rows.append({"sv": sv, "status": "no_addresses"})
            continue

        matched_addr = matched_addr.copy()
        matched_addr["addr_street"] = matched_addr["addr_street"].fillna("").astype(str).str.strip()
        matched_addr["addr_housenumber"] = matched_addr["addr_housenumber"].fillna("").astype(str).str.strip()
        matched_addr["key"] = matched_addr["addr_street"] + "|" + matched_addr["addr_housenumber"]

        sv_keys = set(matched_addr["key"].tolist())
        sv_links = links[links["key"].isin(sv_keys)].copy()

        building_ids = sv_links["building_id"].astype(str).dropna().unique().tolist()
        sv_buildings = b[b["_id_str"].isin(building_ids)].copy()
        if "_id_str" in sv_buildings.columns:
            sv_buildings = sv_buildings.drop(columns=["_id_str"])

        if sv_buildings.empty:
            stats_rows.append(
                {
                    "sv": sv,
                    "status": "no_buildings",
                    "addr_n": int(len(matched_addr)),
                    "link_n": int(len(sv_links)),
                }
            )
            continue

        poly, meta = build_auto_polygon(sv_buildings)

        polygons.append(
            {
                "sv": int(sv),
                "mode": meta["mode"],
                "addr_n": int(len(matched_addr)),
                "link_n": int(len(sv_links)),
                "bldg_n": int(len(sv_buildings)),
                "d50": float(meta["d50"]),
                "d90": float(meta["d90"]),
                "R": float(meta["R"]),
                "C": int(meta["C"]),
                "w": float(meta["w"]),
                "b": float(meta["b"]),
                "simplify": float(meta["s"]),
                "geometry": poly,
            }
        )

        stats_rows.append(
            {
                "sv": int(sv),
                "status": "ok",
                "mode": meta["mode"],
                "addr_n": int(len(matched_addr)),
                "link_n": int(len(sv_links)),
                "bldg_n": int(len(sv_buildings)),
                **meta,
            }
        )

        print(
            f"[SV{sv}] ok: addr={len(matched_addr)} links={len(sv_links)} "
            f"bldg={len(sv_buildings)} mode={meta['mode']}"
        )

    if polygons:
        out = gpd.GeoDataFrame(polygons, geometry="geometry", crs=b.crs)
        out.to_file(out_gpkg, layer="sv_polygons_auto", driver="GPKG")

    stats = pd.DataFrame(stats_rows)
    stats.to_csv("src/data/scratch/all_sv_polygons_auto_stats.csv", index=False)

    print("Wrote:", out_gpkg, "layer=sv_polygons_auto")
    print("Wrote: src/data/scratch/all_sv_polygons_auto_stats.csv")


if __name__ == "__main__":
    main()
