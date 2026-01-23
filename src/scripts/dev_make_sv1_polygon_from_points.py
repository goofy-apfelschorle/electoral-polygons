from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString
from shapely.ops import unary_union


def _empty_gdf(work_crs: str, cols: list[str]) -> gpd.GeoDataFrame:
    """Create an empty GeoDataFrame with a geometry column."""
    base = {c: [] for c in cols}
    gdf = gpd.GeoDataFrame(base, geometry=gpd.GeoSeries([], crs=work_crs), crs=work_crs)
    return gdf


def main() -> None:
    assets_gpkg = Path("src/src/electoral_polygons/assets/bucharest_osm_assets.gpkg")
    buildings_layer = "main_buildings"
    boundary_layer = "boundary"
    streets_layer = "streets"

    sv1_points_gpkg = Path("src/data/scratch/sv1_addresses.gpkg")
    sv1_points_layer = "sv1_addresses"

    out_gpkg = assets_gpkg
    out_layer = "sv1_polygon_v3"  # new name to avoid ArcMap caching

    # ---- Load ----
    buildings = gpd.read_file(assets_gpkg, layer=buildings_layer)
    boundary = gpd.read_file(assets_gpkg, layer=boundary_layer)
    streets = gpd.read_file(assets_gpkg, layer=streets_layer)
    pts = gpd.read_file(sv1_points_gpkg, layer=sv1_points_layer)

    if pts.empty:
        raise ValueError("SV1 points empty. Run dev_match_sv1.py first.")
    if buildings.empty:
        raise ValueError("Buildings layer empty (unexpected).")
    if boundary.empty:
        raise ValueError("Boundary layer empty (unexpected).")
    if streets.empty:
        raise ValueError("Streets layer empty (unexpected).")

    # Work CRS for metric distances
    work_crs = "EPSG:3857"
    buildings_w = buildings.to_crs(work_crs)
    boundary_w = boundary.to_crs(work_crs)
    streets_w = streets.to_crs(work_crs)

    pts_w = pts.to_crs(work_crs).copy()
    pts_w["pt_id"] = range(len(pts_w))

    pts_union = unary_union(pts_w.geometry)

    # ---- Find Ion Mihalache line locally ----
    name_field = None
    for cand in ["name", "addr_street", "street"]:
        if cand in streets_w.columns:
            name_field = cand
            break
    if name_field is None:
        raise ValueError(f"Streets layer has no name field. Columns: {list(streets_w.columns)}")

    streets_w["_norm"] = streets_w[name_field].fillna("").astype(str).str.strip().str.lower()
    target = "bulevardul ion mihalache"

    mih = streets_w[streets_w["_norm"] == target].copy()
    if mih.empty:
        mih = streets_w[streets_w["_norm"].str.contains(target, na=False)].copy()
    if mih.empty:
        raise ValueError("Could not find Ion Mihalache in streets layer.")

    # Keep only local segments near SV points (prevents snapping to a far Mihalache piece)
    local = pts_union.buffer(500.0)
    mih_local = mih[mih.intersects(local)].copy()
    if mih_local.empty:
        raise ValueError("Found Ion Mihalache, but none of its segments intersect SV1 neighborhood.")
    mih_line = unary_union(mih_local.geometry)

    # ---- Build street segment spanning SV points ----
    dists = [mih_line.project(p) for p in pts_w.geometry]
    a, b = min(dists), max(dists)

    pad_along = 10.0
    a = max(0.0, a - pad_along)
    b = min(mih_line.length, b + pad_along)

    n = 300
    ds = [a + (b - a) * i / (n - 1) for i in range(n)]
    seg = LineString([mih_line.interpolate(d).coords[0] for d in ds])

    # ---- Corridor around segment ----
    frontage_dist = 35.0
    corridor = seg.buffer(frontage_dist)

    cand = buildings_w[buildings_w.intersects(corridor)].copy()
    print(f"[debug] candidate buildings in corridor: {len(cand)}")
    if cand.empty:
        raise ValueError("No buildings in corridor. Increase frontage_dist (e.g., 90–140).")

    # ---- Seed buildings: nearest building to each point (then require corridor intersection) ----
    all_sidx = buildings_w.sindex
    max_seed_dist = 350.0

    seed_rows = []
    failed_rows = []

    for _, r in pts_w.iterrows():
        p = r.geometry
        pid = int(r["pt_id"])

        hits = list(all_sidx.intersection(p.buffer(max_seed_dist).bounds))
        if not hits:
            failed_rows.append({"pt_id": pid, "reason": "no_hits_in_bbox", "geometry": p})
            continue

        sub = buildings_w.iloc[hits].copy()
        sub["d"] = sub.geometry.distance(p)
        sub = sub[sub["d"] <= max_seed_dist]
        if sub.empty:
            failed_rows.append({"pt_id": pid, "reason": "no_building_within_max_seed_dist", "geometry": p})
            continue

        best_i = sub["d"].idxmin()
        best_geom = buildings_w.loc[best_i].geometry
        best_d = float(sub.loc[best_i, "d"])

        # Require it to intersect corridor (avoid snapping behind blocks)
        if not best_geom.intersects(corridor):
            failed_rows.append({"pt_id": pid, "reason": f"nearest_not_in_corridor (d={best_d:.1f}m)", "geometry": p})
            continue

        seed_rows.append({"pt_id": pid, "best_d": best_d, "geometry": best_geom})

    # --- SAFE construction of GeoDataFrames (works even if list is empty) ---
    if seed_rows:
        seeds = gpd.GeoDataFrame(seed_rows, geometry="geometry", crs=work_crs)
    else:
        seeds = _empty_gdf(work_crs, cols=["pt_id", "best_d"])

    if failed_rows:
        failed = gpd.GeoDataFrame(failed_rows, geometry="geometry", crs=work_crs)
    else:
        failed = _empty_gdf(work_crs, cols=["pt_id", "reason"])

    print(f"[debug] seeds found: {len(seeds)} / {len(pts_w)} points")
    if len(failed) > 0:
        print("[debug] failed point ids:", failed["pt_id"].tolist())
        print(failed[["pt_id", "reason"]])

    # Write debug layers (ArcMap)
    seeds.to_crs(buildings.crs).to_file(out_gpkg, layer="sv1_seeds_debug", driver="GPKG")
    failed.to_crs(buildings.crs).to_file(out_gpkg, layer="sv1_failed_points_debug", driver="GPKG")
    print("Wrote debug layers: sv1_seeds_debug, sv1_failed_points_debug")

    if seeds.empty:
        raise ValueError("No seed buildings. Increase frontage_dist or max_seed_dist, or OSM buildings missing here.")

    # ---- Grow selection to catch adjacent frontage buildings ----
    grow_m = 18
    seed_union = unary_union(seeds.geometry).buffer(grow_m)
    grown = cand[cand.intersects(seed_union)].copy()
    print(f"[debug] grown buildings: {len(grown)}")

    # ---- Determine correct side using seed centroid ----
    divider = seg.buffer(22.0)
    neigh = pts_union.buffer(350.0)
    two_sides = neigh.difference(divider)

    if two_sides.geom_type == "Polygon":
        side_poly = two_sides
    else:
        polys = list(getattr(two_sides, "geoms", []))
        seed_centroid = unary_union(seeds.geometry).centroid
        dists = [seed_centroid.distance(p) for p in polys]
        side_poly = polys[int(pd.Series(dists).idxmin())]

    grown = grown[grown.intersects(side_poly)].copy()

    # ---- Filled polygon from buildings ----
    fill_m = 14.0
    poly_buildings = unary_union(grown.geometry).buffer(fill_m).buffer(0)

    # ---- One-side strip to fill missing building footprints ----
    strip_depth = 25.0
    strip = seg.buffer(strip_depth).intersection(side_poly).buffer(0)

    # HARD CLAMP: never let the polygon expand beyond a local neighborhood of the SV points
    clamp = pts_union.buffer(140.0)  # << main safety valve (meters)
    poly_buildings = poly_buildings.intersection(clamp).buffer(0)
    strip = strip.intersection(clamp).buffer(0)

    poly = unary_union([poly_buildings, strip]).buffer(0)

    # clip to Bucharest boundary
    bnd = unary_union(boundary_w.geometry)
    poly = poly.intersection(bnd).buffer(0)
    if poly.is_empty:
        raise ValueError("SV1 polygon became empty after clipping.")

    # Keep largest part if multipolygon
    if poly.geom_type == "MultiPolygon":
        parts = list(poly.geoms)
        parts.sort(key=lambda g: g.area, reverse=True)
        poly = parts[0]

    out = gpd.GeoDataFrame(
        [{
            "sv": 1,
            "street": "Bulevardul Ion Mihalache",
            "method": "seed_buildings_plus_strip",
            "frontage_dist": frontage_dist,
            "max_seed_dist": max_seed_dist,
            "grow_m": grow_m,
            "fill_m": fill_m,
            "strip_depth": strip_depth,
        }],
        geometry=[poly],
        crs=work_crs,
    ).to_crs(buildings.crs)

    out.to_file(out_gpkg, layer=out_layer, driver="GPKG")
    print(f"Wrote layer '{out_layer}' into: {out_gpkg}")


if __name__ == "__main__":
    main()
