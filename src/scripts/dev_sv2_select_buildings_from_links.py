 
from __future__ import annotations

import geopandas as gpd
import pandas as pd


def _norm_street(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


def _norm_housenumber(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", "", regex=True)
    )


def sv1_select_buildings(
    sv_addr_path: str,
    sv_addr_layer: str,
    links_path: str,
    links_layer: str,
    buildings_path: str,
    buildings_layer: str,
    out_path: str,
    *,
    sv_street_col: str = "addr_street",
    sv_hn_col: str = "addr_housenumber",
    link_street_col: str = "addr_street",
    link_hn_col: str = "addr_housenumber",
    link_building_id_col: str = "building_id",
    buildings_id_col: str = "osm_id",
) -> None:
    sv = gpd.read_file(sv_addr_path, layer=sv_addr_layer)
    links = gpd.read_file(links_path, layer=links_layer)
    b = gpd.read_file(buildings_path, layer=buildings_layer)

    # Normalize keys on both sides
    sv = sv.copy()
    links = links.copy()
    b = b.copy()

    sv[sv_street_col] = _norm_street(sv[sv_street_col])
    sv[sv_hn_col] = _norm_housenumber(sv[sv_hn_col])

    links[link_street_col] = _norm_street(links[link_street_col])
    links[link_hn_col] = _norm_housenumber(links[link_hn_col])

    # Build match key
    sv["key"] = sv[sv_street_col] + "|" + sv[sv_hn_col]
    links["key"] = links[link_street_col] + "|" + links[link_hn_col]

    # Match SV addresses -> link records
    sv_keys = set(sv["key"].tolist())
    matched_links = links[links["key"].isin(sv_keys)].copy()

    # Unique buildings
    building_ids = matched_links[link_building_id_col].astype(str).dropna().unique().tolist()

    b["_id_str"] = b[buildings_id_col].astype(str)
    selected_buildings = b[b["_id_str"].isin(building_ids)].copy().drop(columns=["_id_str"])

    # SV addresses that didn’t match anything
    matched_keys = set(matched_links["key"].tolist())
    sv_unmatched = sv[~sv["key"].isin(matched_keys)].copy()

    # Write debug outputs
    matched_links.to_file(out_path, layer="sv1_addr_matched_links", driver="GPKG")
    selected_buildings.to_file(out_path, layer="sv1_buildings_selected", driver="GPKG")
    if len(sv_unmatched) > 0:
        sv_unmatched.to_file(out_path, layer="sv1_addr_unmatched", driver="GPKG")

    print("Wrote:", out_path)
    print("Matched link records:", len(matched_links))
    print("Unique buildings selected:", len(selected_buildings))
    print("SV1 addresses unmatched:", len(sv_unmatched))


if __name__ == "__main__":
    SV_ADDR_PATH = "src/data/scratch/sv2_addresses.gpkg"
    SV_ADDR_LAYER = None  # if gpkg has only one layer, geopandas ignores; but fiona needs name.
    # Safer: explicitly set the layer name you know (often "sv1_addresses" or similar).
    # If you’re unsure, run: python -c "import fiona; print(fiona.listlayers('src/data/scratch/sv1_addresses.gpkg'))"

    LINKS_PATH = "src/data/scratch/buildings_with_addresses.gpkg"
    BUILDINGS_PATH = "src/data/scratch/buildings_with_addresses.gpkg"

    OUT_PATH = "src/data/scratch/sv2_selected_buildings_from_links.gpkg"

    # Determine SV layer name automatically
    import fiona
    sv_layers = fiona.listlayers(SV_ADDR_PATH)
    SV_ADDR_LAYER = sv_layers[0]

    sv1_select_buildings(
        sv_addr_path=SV_ADDR_PATH,
        sv_addr_layer=SV_ADDR_LAYER,
        links_path=LINKS_PATH,
        links_layer="building_address_links",
        buildings_path=BUILDINGS_PATH,
        buildings_layer="buildings_enriched",
        out_path=OUT_PATH,
        sv_street_col="addr_street",
        sv_hn_col="addr_housenumber",
        link_street_col="addr_street",
        link_hn_col="addr_housenumber",
        link_building_id_col="building_id",
        buildings_id_col="osm_id",
    )
