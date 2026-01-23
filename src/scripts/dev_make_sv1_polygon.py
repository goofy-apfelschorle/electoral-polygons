from __future__ import annotations

from pathlib import Path
import re
import unicodedata

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import unary_union


def normalize_street(name) -> str:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    name = str(name)
    name = re.sub(r"\s*\(.*?\)", "", name)
    name = name.lower()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r"\s+", " ", name).strip()
    return name


def longest_linestring(geom):
    if geom is None or geom.is_empty:
        return None
    if isinstance(geom, LineString):
        return geom
    if isinstance(geom, MultiLineString):
        parts = list(geom.geoms)
        return max(parts, key=lambda g: g.length) if parts else None
    if hasattr(geom, "geoms"):
        lines = [g for g in geom.geoms if isinstance(g, LineString)]
        if lines:
            return max(lines, key=lambda g: g.length)
    return None


def pick_street_name_field(streets: gpd.GeoDataFrame) -> str:
    # Try common fields
    for cand in ["name", "addr_street", "street", "str_name", "road_name"]:
        if cand in streets.columns:
            return cand
    # If nothing obvious, show columns
    raise ValueError(f"Could not find a street-name column. Available columns: {list(streets.columns)}")


def main() -> None:
    assets_gpkg = Path("src/src/electoral_polygons/assets/bucharest_osm_assets.gpkg")
    buildings_layer = "main_buildings"   # <- change if your good layer has a different name
    streets_layer = "streets"
    boundary_layer = "boundary"

    # SV1 points output created earlier by dev_match_sv1.py
    sv1_points_gpkg = Path("src/data/scratch/sv1_addresses.gpkg")
    sv1_points_layer = "sv1_addresses"

    out_gpkg = Path("src/data/scratch/sv1_polygon.gpkg")
    out_layer = "sv1_polygon"

    # --- Load layers ---
    buildings = gpd.read_file(assets_gpkg, layer=buildings_layer)
    streets = gpd.read_file(assets_gpkg, layer=streets_layer)
    boundary = gpd.read_file(assets_gpkg, layer=boundary_layer)
    pts = gpd.read_file(sv1_points_gpkg, layer=sv1_points_layer)

    if pts.empty:
        raise ValueError("SV1 points layer is empty. Run dev_match_sv1.py first.")
    if buildings.empty:
        raise ValueError("Buildings layer is empty (unexpected).")
    if streets.empty:
        raise ValueError("Streets layer is empty (unexpected).")

    # --- Choose a metric CRS for buffering ---
    # If your assets are already projected, we’ll keep them; if not, we use EPSG:3857
    work_crs = None
    if streets.crs is not None and hasattr(streets.crs, "is_geographic") and not streets.crs.is_geographic:
        work_crs = streets.crs
    else:
        work_crs = "EPSG:3857"

    # Reproject everything to work CRS
    streets_w = streets.to_crs(work_crs)
    buildings_w = buildings.to_crs(work_crs)
    boundary_w = boundary.to_crs(work_crs)
    pts_w = pts.to_crs(work_crs)

    # --- Find Ion Mihalache street geometry ---
    name_field = pick_street_name_field(streets_w)
    streets_w["_norm"] = streets_w[name_field].apply(normalize_street)

    target_norm = normalize_street("Bulevardul Ion Mihalache")
    mih = streets_w[streets_w["_norm"] == target_norm].copy()
    if mih.empty:
        mih = streets_w[streets_w["_norm"].str.contains(target_norm, na=False)].copy()
    if mih.empty:
        # debug help
        sample = streets_w[name_field].dropna().astype(str).head(20).tolist()
        raise ValueError(f"Could not find Ion Mihalache in streets layer. Sample names: {sample}")

    mih_line = unary_union(mih.geometry)
    mih_line = longest_linestring(mih_line)
    if mih_line is None:
        raise ValueError("Failed to build a usable LineString for Ion Mihalache.")

    # --- Get the relevant segment using a corridor around points ---
    pts_union = unary_union(pts_w.geometry)

    # 250m corridor around points to cut only the relevant street segment
    seg_geom = mih_line.intersection(pts_union.buffer(250))
    seg = longest_linestring(seg_geom)
    if seg is None or seg.length < 20:
        seg = mih_line  # fallback

    print(f"[debug] work_crs={work_crs}")
    print(f"[debug] SV1 points={len(pts_w)}  buildings={len(buildings_w)}  mih_line_len={mih_line.length:.1f}  seg_len={seg.length:.1f}")

    # --- Build a one-side corridor and pick the correct side based on point containment ---
    big_buf = seg.buffer(180)      # wide enough to reach buildings on both sides
    divider = seg.buffer(15)       # thickness to split sides (road width-ish)
    sides = big_buf.difference(divider)

    # Choose the side that contains most SV1 points
    if sides.geom_type == "Polygon":
        chosen_side = sides
    else:
        side_polys = list(getattr(sides, "geoms", []))
        if not side_polys:
            raise ValueError("Could not split corridor into sides.")
        counts = [int(pts_w.within(poly).sum()) for poly in side_polys]
        chosen_side = side_polys[int(pd.Series(counts).idxmax())]
        print(f"[debug] side point counts={counts}")

    # --- Select buildings intersecting chosen corridor ---
    b_sel = buildings_w[buildings_w.intersects(chosen_side)].copy()
    print(f"[debug] buildings intersect chosen_side = {len(b_sel)}")

    print(f"[debug] buildings selected (corridor only) = {len(b_sel)}")


    if b_sel.empty:
        raise ValueError(
            "No buildings selected for SV1. "
            "Likely CRS/streets mismatch or buffers too small. "
            "Try increasing big_buf/divider or distance threshold."
        )

    # --- Dissolve buildings into polygon and clip to boundary ---
    poly = unary_union(b_sel.geometry)
    poly = poly.intersection(unary_union(boundary_w.geometry))

    out = gpd.GeoDataFrame(
        [{"sv": 1, "street": "Bulevardul Ion Mihalache"}],
        geometry=[poly],
        crs=work_crs,
    )

    # Write in same CRS as work_crs; ArcMap will still load it fine.
    out_gpkg.parent.mkdir(parents=True, exist_ok=True)
    out.to_file(out_gpkg, layer=out_layer, driver="GPKG")

    print(f"Selected buildings: {len(b_sel)}")
    print(f"Wrote SV1 polygon to: {out_gpkg} (layer={out_layer})")


if __name__ == "__main__":
    main()
