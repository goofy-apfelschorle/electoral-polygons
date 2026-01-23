from __future__ import annotations

from pathlib import Path
import geopandas as gpd
import osmnx as ox
import pyogrio
import pandas as pd


def pick_boundary_layer(gpkg_path: Path) -> str:
    layers = pyogrio.list_layers(str(gpkg_path))  # list of (name, geometry_type)
    for name, gtype in layers:
        if "boundary" in name.lower() and gtype.lower() in {"polygon", "multipolygon"}:
            return name
    for name, gtype in layers:
        if gtype.lower() in {"polygon", "multipolygon"}:
            return name
    raise ValueError(f"No polygon boundary-like layer found in {gpkg_path}. Layers: {layers}")


def pick_reference_crs(gpkg_path: Path) -> str:
    layers = [n for n, _ in pyogrio.list_layers(str(gpkg_path))]
    for candidate in ["main_streets", "streets", "main_roads", "roads"]:
        if candidate in layers:
            g = gpd.read_file(gpkg_path, layer=candidate)
            if g.crs:
                return g.crs.to_string()
    bname = pick_boundary_layer(gpkg_path)
    b = gpd.read_file(gpkg_path, layer=bname)
    return b.crs.to_string() if b.crs else "EPSG:4326"


def main() -> None:
    gpkg_path = Path("src/src/electoral_polygons/assets/bucharest_osm_assets.gpkg")
    out_layer = "main_buildings"

    if not gpkg_path.exists():
        raise FileNotFoundError(f"Could not find GPKG at: {gpkg_path}")

    boundary_layer = pick_boundary_layer(gpkg_path)
    print(f"Using boundary layer: {boundary_layer}")

    boundary = gpd.read_file(gpkg_path, layer=boundary_layer)
    if boundary.empty:
        raise ValueError(f"Boundary layer '{boundary_layer}' is empty.")

    boundary_wgs = boundary.to_crs(4326)

    # Shapely warning is fine; keeps compatibility
    geom = boundary_wgs.unary_union

    tags = {"building": True}
    gdf = ox.features_from_polygon(geom, tags=tags)

    gdf = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])].copy()

    keep_cols = [c for c in ["building", "name", "addr:housenumber", "addr:street"] if c in gdf.columns]
    gdf = gdf[keep_cols + ["geometry"]].copy()

    # Robust osm_id creation
    if isinstance(gdf.index, pd.MultiIndex):
        gdf["osm_id"] = gdf.index.map(lambda x: f"{x[0]}:{x[1]}")
    else:
        gdf["osm_id"] = gdf.index.map(str)

    # ---- ArcMap compatibility: remove ':' from field names ----
    rename_map = {c: c.replace(":", "_") for c in gdf.columns if ":" in c}
    if rename_map:
        gdf = gdf.rename(columns=rename_map)
    # ----------------------------------------------------------

    target_crs = pick_reference_crs(gpkg_path)
    gdf = gdf.set_crs(4326).to_crs(target_crs)

    gdf.to_file(gpkg_path, layer=out_layer, driver="GPKG")
    gdf.to_file(gpkg_path, layer=out_layer, driver="GPKG")

    print(f"Fetched buildings: {len(gdf)}")
    print(f"Wrote layer '{out_layer}' to: {gpkg_path}")


if __name__ == "__main__":
    main()
