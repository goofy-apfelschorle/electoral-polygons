from __future__ import annotations

import geopandas as gpd


def make_polygon_from_selected_buildings(
    selected_path: str,
    selected_layer: str,
    out_path: str,
    *,
    buffer_m: float = 18.0,
    simplify_m: float = 2.0,
) -> None:
    b = gpd.read_file(selected_path, layer=selected_layer)
    if len(b) == 0:
        raise ValueError("Selected buildings layer is empty.")

    # dissolve all selected buildings into one geometry
    geom = b.geometry.unary_union

    # buffer outward to include frontage/sidewalk and connect slight gaps
    poly = geom.buffer(buffer_m)

    # simplify to remove jagged edges
    if simplify_m and simplify_m > 0:
        poly = poly.simplify(simplify_m, preserve_topology=True)

    out = gpd.GeoDataFrame({"sv": ["SV1"], "buffer_m": [buffer_m]}, geometry=[poly], crs=b.crs)
    out.to_file(out_path, layer="sv1_polygon_from_selected", driver="GPKG")
    print("Wrote:", out_path, "layer=sv1_polygon_from_selected")


if __name__ == "__main__":
    IN_PATH = "src/data/scratch/sv1_selected_buildings_from_links.gpkg"
    IN_LAYER = "sv1_buildings_selected"
    OUT_PATH = "src/data/scratch/sv1_polygon_from_selected_buildings.gpkg"

    make_polygon_from_selected_buildings(
        selected_path=IN_PATH,
        selected_layer=IN_LAYER,
        out_path=OUT_PATH,
        buffer_m=18.0,
        simplify_m=2.0,
    )
