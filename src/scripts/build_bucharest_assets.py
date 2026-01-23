from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple


import geopandas as gpd
import pandas as pd

try:
    import osmnx as ox
except ImportError as e:
    raise SystemExit(
        "Missing dependency: osmnx. Install with `pip install osmnx` "
        "(or add it to your project dependencies)."
    ) from e


# ----------------------------
# Config
# ----------------------------

@dataclass(frozen=True)
class BuildConfig:
    place_query: str = "Bucharest, Romania"
    crs_epsg: int = 3844  # Romania Stereo 70
    network_type: str = "drive"  # robust default; change to "all" if you want footways etc.
    simplify: bool = True
    retain_all: bool = False  # keep largest component by default


ROOT = Path(__file__).resolve().parents[1]
ASSETS_DIR = ROOT / "src" / "electoral_polygons" / "assets"
ASSETS_DIR.mkdir(parents=True, exist_ok=True)

ASSETS_GPKG = ASSETS_DIR / "bucharest_osm_assets.gpkg"
ASSETS_META = ASSETS_DIR / "assets_metadata.json"


# ----------------------------
# Helpers
# ----------------------------

def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s


def _parse_housenumber_int(hn: Optional[str]) -> Optional[int]:
    """
    Best-effort parse:
    - "14" -> 14
    - "14A" -> 14
    - "14-16" -> 14 (we keep first numeric token)
    - "14/1" -> 14
    """
    if hn is None:
        return None
    if not isinstance(hn, str):
        hn = str(hn)

    m = re.search(r"(\d+)", hn)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _ensure_single_polygon(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Dissolve into one geometry (MultiPolygon ok).
    """
    if len(gdf) == 1:
        return gdf
    dissolved = gdf.dissolve()
    dissolved = dissolved.reset_index(drop=True)
    return dissolved


def _write_layer(gpkg_path: Path, gdf: gpd.GeoDataFrame, layer: str) -> None:
    # overwrite layer if exists
    gdf.to_file(gpkg_path, layer=layer, driver="GPKG")


# ----------------------------
# Build steps
# ----------------------------

def build_boundary(cfg: BuildConfig) -> gpd.GeoDataFrame:
    """
    Boundary from OSM geocode. If you already have a curated boundary,
    you can replace this with a loader and skip geocoding.
    """
    boundary = ox.geocode_to_gdf(cfg.place_query)
    boundary = _ensure_single_polygon(boundary)

    boundary = boundary.to_crs(epsg=cfg.crs_epsg)
    # fix invalid / self-intersecting geometries (OSM boundary issue)
    boundary["geometry"] = boundary["geometry"].buffer(0)
    boundary = boundary[["geometry"]].copy()
    boundary["name"] = "Bucharest"
    boundary["source"] = "osmnx_geocode"
    boundary = boundary[["name", "source", "geometry"]]
    return boundary


def build_street_graph(boundary: gpd.GeoDataFrame, cfg: BuildConfig):
    """
    Download + project the street network inside the boundary polygon.
    Returns projected graph and edges/nodes GeoDataFrames.
    """
    poly = boundary.geometry.iloc[0]

    G = ox.graph_from_place(
    cfg.place_query,
    network_type=cfg.network_type,
    simplify=cfg.simplify,
    retain_all=cfg.retain_all,
    truncate_by_edge=True,
    )


    # Project graph to Stereo 70
    G = ox.project_graph(G, to_crs=f"EPSG:{cfg.crs_epsg}")

    nodes, edges = ox.graph_to_gdfs(G, nodes=True, edges=True, node_geometry=True, fill_edge_geometry=True)

    # Normalize a few fields for stability
    # length is usually present, but keep our own length_m name
    if "length" in edges.columns:
        edges["length_m"] = pd.to_numeric(edges["length"], errors="coerce")
    else:
        edges["length_m"] = edges.geometry.length

    # Stringify osmid (can be list)
    if "osmid" in edges.columns:
        edges["osmid"] = edges["osmid"].apply(lambda x: ",".join(map(str, x)) if isinstance(x, (list, tuple, set)) else (str(x) if pd.notna(x) else None))

    # Ensure expected columns exist
    for col in ["name", "highway", "oneway"]:
        if col not in edges.columns:
            edges[col] = None

    streets_cols = ["u", "v", "key", "osmid", "name", "highway", "oneway", "length_m", "geometry"]
    streets = edges.reset_index()[streets_cols].copy()

    # Nodes
    if "osmid" not in nodes.columns:
        nodes = nodes.reset_index().rename(columns={"index": "osmid"})
    nodes = nodes.reset_index(drop=False)
    if "osmid" not in nodes.columns:
        nodes["osmid"] = nodes["index"]

    # x/y typically exist
    for col in ["x", "y", "street_count"]:
        if col not in nodes.columns:
            nodes[col] = None

    nodes_out = nodes[["osmid", "x", "y", "street_count", "geometry"]].copy()

    return G, streets, nodes_out


def build_addresses(boundary: gpd.GeoDataFrame, cfg: BuildConfig) -> gpd.GeoDataFrame:
    """
    Pull OSM objects that explicitly carry addr:housenumber.
    Keep nodes and buildings with housenumber; convert buildings to points via representative point.
    """
    poly = boundary.geometry.iloc[0]

    tags = {"addr:housenumber": True}
    addr = ox.features_from_place(cfg.place_query, tags)

    # Project to target CRS
    addr = addr.to_crs(epsg=cfg.crs_epsg)

    # Keep only records that actually have a housenumber
    if "addr:housenumber" not in addr.columns:
        # Nothing returned
        return gpd.GeoDataFrame(columns=["addr_id", "addr_street", "addr_housenumber", "addr_housenumber_int", "source", "geometry"], crs=f"EPSG:{cfg.crs_epsg}")

    addr = addr[addr["addr:housenumber"].notna()].copy()

    # Build points for everything:
    # - points stay as is
    # - polygons -> representative point (better than centroid if concave)
    geom_type = addr.geometry.geom_type
    is_poly = geom_type.isin(["Polygon", "MultiPolygon"])
    addr.loc[is_poly, "geometry"] = addr.loc[is_poly, "geometry"].representative_point()

    # Derive source
    # osmnx features_from_polygon returns an index with (element_type, osmid)
    source = []
    addr_id = []
    for idx in addr.index:
        # idx often like ('node', 123) or ('way', 456)
        if isinstance(idx, tuple) and len(idx) == 2:
            etype, oid = idx
            if etype == "node":
                source.append("node")
            elif etype in ("way", "relation"):
                source.append("building")  # not always building, but usually polygon
            else:
                source.append("other")
            addr_id.append(f"{etype}:{oid}")
        else:
            source.append("other")
            addr_id.append(str(idx))

    addr["addr_id"] = addr_id
    addr["source"] = source
    addr["addr_street"] = addr.get("addr:street", None)
    addr["addr_housenumber"] = addr["addr:housenumber"].astype(str)
    addr["addr_housenumber_int"] = addr["addr_housenumber"].apply(_parse_housenumber_int)

    out = addr[["addr_id", "addr_street", "addr_housenumber", "addr_housenumber_int", "source", "geometry"]].copy()
    out = out.reset_index(drop=True)

    # Keep only point geometries now
    out = out[out.geometry.geom_type == "Point"].copy()

    return out


def write_metadata(boundary: gpd.GeoDataFrame, cfg: BuildConfig, gpkg_path: Path) -> None:
    geom = boundary.geometry.iloc[0]
    minx, miny, maxx, maxy = geom.bounds

    meta = {
        "city": "Bucharest",
        "place_query": cfg.place_query,
        "crs_epsg": cfg.crs_epsg,
        "network_type": cfg.network_type,
        "simplify": cfg.simplify,
        "retain_all": cfg.retain_all,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "assets_gpkg": str(gpkg_path.name),
        "layers": ["boundary", "streets", "nodes", "addresses"],
        "bbox": {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy},
    }

    ASSETS_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def main() -> None:
    cfg = BuildConfig()

    print("[1/4] Building boundary…")
    boundary = build_boundary(cfg)
    
    
    boundary["geometry"] = boundary["geometry"].buffer(0)

    print("[2/4] Building street network graph + layers…")
    _, streets, nodes = build_street_graph(boundary, cfg)

    print("[3/4] Building address points…")
    addresses = build_addresses(boundary, cfg)

    print(f"[4/4] Writing GeoPackage → {ASSETS_GPKG}")
    # Write layers (each call overwrites the layer)
    _write_layer(ASSETS_GPKG, boundary, "boundary")
    _write_layer(ASSETS_GPKG, streets, "streets")
    _write_layer(ASSETS_GPKG, nodes, "nodes")
    _write_layer(ASSETS_GPKG, addresses, "addresses")

    write_metadata(boundary, cfg, ASSETS_GPKG)

    print("Done.")
    print(f"- GeoPackage: {ASSETS_GPKG}")
    print(f"- Metadata:   {ASSETS_META}")


if __name__ == "__main__":
    main()
