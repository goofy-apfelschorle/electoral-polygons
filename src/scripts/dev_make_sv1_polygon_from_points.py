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

    from shapely.affinity import rotate, translate
    import numpy as np
    import math

    def _line_angle_deg(line: LineString) -> float:
        coords = np.asarray(line.coords)
        dx = float(coords[-1, 0] - coords[0, 0])
        dy = float(coords[-1, 1] - coords[0, 1])
        return math.degrees(math.atan2(dy, dx))

    # ---- Determine correct side using SEEDS in a rotated street frame ----
    origin = seg.centroid
    ang = _line_angle_deg(seg)
    rot = -ang

    seg_r = rotate(seg, rot, origin=origin, use_radians=False)
    ry = float(np.mean([c[1] for c in seg_r.coords]))

    seeds_r = seeds.copy()
    seeds_r["geometry"] = seeds_r.geometry.map(lambda g: rotate(g, rot, origin=origin, use_radians=False))
    seeds_r["geometry"] = seeds_r.geometry.map(lambda g: translate(g, yoff=-ry))

    # side is decided by seed centroid y sign after rotation (street ~ horizontal, street ~ y=0)
    seed_y = float(seeds_r.geometry.centroid.y.mean())
    keep_pos = seed_y >= 0
    print(f"[debug] side_by_seeds keep_pos={keep_pos} (seed_y_mean={seed_y:.2f})")

    def _filter_side(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if gdf.empty:
            return gdf
        gr = gdf.copy()
        gr["geometry"] = gr.geometry.map(lambda g: rotate(g, rot, origin=origin, use_radians=False))
        gr["geometry"] = gr.geometry.map(lambda g: translate(g, yoff=-ry))
        cy = gr.geometry.centroid.y
        mask = (cy >= 0) if keep_pos else (cy <= 0)
        return gdf.loc[mask.values].copy()

    # Filter candidate corridor buildings to the correct street side
    cand_side = _filter_side(cand)

    # ---- Restrict to frontage band (avoid grabbing buildings deep behind the street) ----
    # Use seeds as truth: how far from the street do "correct" buildings sit?
    seed_d = seeds.geometry.distance(seg)
    # robust max (avoid one weird outlier)
    seed_band = float(seed_d.quantile(0.90)) + 12.0  # meters; tweak margin if needed
    print(f"[debug] seed_band_from_street={seed_band:.1f}m")

    # Measure candidate distance to street
    cand_side = cand_side.copy()
    cand_side["d_street"] = cand_side.geometry.distance(seg)

    # Keep only buildings close enough to the street (frontage)
    cand_front = cand_side[cand_side["d_street"] <= seed_band].copy()
    print(f"[debug] cand_front buildings: {len(cand_front)} (from {len(cand_side)})")
    cand_front.to_crs(buildings.crs).to_file(out_gpkg, layer="sv1_cand_front_debug", driver="GPKG")
    cand_side.to_crs(buildings.crs).to_file(out_gpkg, layer="sv1_cand_side_debug", driver="GPKG")
    print("[debug] wrote sv1_cand_front_debug and sv1_cand_side_debug")
    print(f"[debug] cand_side buildings: {len(cand_side)}")
    print(f"[debug] cand_front buildings: {len(cand_front)}")



    # Safety fallback if we filtered too hard
    if cand_front.empty:
        print("[debug] cand_front empty; falling back to cand_side")
        cand_front = cand_side

    grown_side = _filter_side(grown)

    print(f"[debug] cand_side buildings: {len(cand_side)}")
    print(f"[debug] grown_side buildings: {len(grown_side)}")


    print(f"[debug] cand_side buildings: {len(cand_side)}")

    from electoral_polygons.polygonize import (
    OrthogonalFrontageParams,
    build_orthogonal_frontage_polygon,
    )

    # ---- Build orthogonal, one-sided frontage polygon ----
    # Clamp remains your safety valve
    clamp = pts_union.buffer(350.0)
    anchor = unary_union(seeds.geometry)  # or pts_union; seeds is usually best


    params = OrthogonalFrontageParams(
        pad_along=10.0,
        pad_x=12.0,
        pad_y=15.0,
        building_buffer=7.0,
        close_m=22.0,
        simplify_tol=1.2,
    )
    
    # debug: what geometry are we actually using to build the polygon?
    cand_front.to_crs(buildings.crs).to_file(out_gpkg, layer="sv1_used_buildings_debug", driver="GPKG")

    seg_gdf = gpd.GeoDataFrame([{"name": "sv1_seg"}], geometry=[seg], crs=work_crs).to_crs(buildings.crs)
    seg_gdf.to_file(out_gpkg, layer="sv1_seg_debug", driver="GPKG")
    print("[debug] wrote sv1_used_buildings_debug and sv1_seg_debug")

    anchor=pts_union #for trimming ends
    poly = build_orthogonal_frontage_polygon(
        street_seg=seg,
        buildings=cand_front,
        anchor_geom=anchor,
        clamp_poly=clamp,
        params=params,
    )


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
            "method": "seed_buildings_orthogonal_corridor",
            "frontage_dist": frontage_dist,
            "max_seed_dist": max_seed_dist,
            "grow_m": grow_m,
            "pad_x": params.pad_x,
            "pad_y": params.pad_y,
            "building_buffer": params.building_buffer,
            "simplify_tol": params.simplify_tol,
        }],
        geometry=[poly],
        crs=work_crs,
    ).to_crs(buildings.crs)

    out.to_file(out_gpkg, layer=out_layer, driver="GPKG")
    print(f"Wrote layer '{out_layer}' into: {out_gpkg}")


if __name__ == "__main__":
    main()
