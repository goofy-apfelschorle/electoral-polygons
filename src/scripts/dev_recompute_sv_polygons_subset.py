from __future__ import annotations

from pathlib import Path
import geopandas as gpd
import pandas as pd

from electoral_polygons.rules_parser import parse_sv
from electoral_polygons.match_addresses import match_sv_addresses
from electoral_polygons.polygonize import build_auto_polygon


def main() -> None:
    # --- choose SVs here ---
    target_svs = [339, 1212]

    xlsx = "src/data/raw/RSV-17.11.2025-----Extras-partiale-BUC.xlsx"
    assets_gpkg = "src/src/electoral_polygons/assets/bucharest_osm_assets.gpkg"

    # link outputs you already generated
    links_gpkg = "src/data/scratch/buildings_with_addresses.gpkg"
    links_layer = "building_address_links"
    buildings_layer = "buildings_enriched"

    out_path = "src/data/scratch/sv_subset_polygons_fixed.gpkg"
    out_layer = "sv_subset_polygons_fixed"
    stats_csv = "src/data/scratch/sv_subset_polygons_fixed_stats.csv"

    Path("src/data/scratch").mkdir(parents=True, exist_ok=True)

    # load once
    links = gpd.read_file(links_gpkg, layer=links_layer).copy()
    b = gpd.read_file(links_gpkg, layer=buildings_layer).copy()

    links["addr_street"] = links["addr_street"].fillna("").astype(str).str.strip()
    links["addr_housenumber"] = links["addr_housenumber"].fillna("").astype(str).str.strip()
    links["key"] = links["addr_street"] + "|" + links["addr_housenumber"]

    b["_id_str"] = b["osm_id"].astype(str)

    rows = []
    stats = []

    for sv in target_svs:
        sv_parsed = parse_sv(xlsx_path=xlsx, judet="B", sv=sv)

        matched_addr = match_sv_addresses(
            addresses_gpkg=assets_gpkg,
            sv_parsed=sv_parsed,
            layer="addresses",
        )

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
            print(f"[SV{sv}] no buildings (addr={len(matched_addr)} links={len(sv_links)})")
            stats.append({"sv": sv, "status": "no_buildings", "addr_n": len(matched_addr), "link_n": len(sv_links), "bldg_n": 0})
            continue

        poly, meta = build_auto_polygon(sv_buildings)

        rows.append(
            {
                "sv": int(sv),
                "addr_n": int(len(matched_addr)),
                "link_n": int(len(sv_links)),
                "bldg_n": int(len(sv_buildings)),
                "mode": meta["mode"],
                "d50": meta["d50"],
                "d90": meta["d90"],
                "R": meta["R"],
                "w": meta["w"],
                "b": meta["b"],
                "simplify": meta["simplify"],
                "max_edge": meta["max_edge"],
                "close_r": meta["close_r"],
                "edges_kept": meta["n_edges_kept"],
                "geometry": poly,
            }
        )
        stats.append({"sv": sv, "status": "ok", **meta, "addr_n": len(matched_addr), "link_n": len(sv_links), "bldg_n": len(sv_buildings)})
        print(f"[SV{sv}] ok: addr={len(matched_addr)} links={len(sv_links)} bldg={len(sv_buildings)} mode={meta['mode']} edges_kept={meta['n_edges_kept']}")

    if rows:
        out = gpd.GeoDataFrame(rows, geometry="geometry", crs=b.crs)
        out.to_file(out_path, layer=out_layer, driver="GPKG")
        print(f"Wrote {out_path} layer={out_layer}")

    pd.DataFrame(stats).to_csv(stats_csv, index=False)
    print(f"Wrote {stats_csv}")


if __name__ == "__main__":
    main()
