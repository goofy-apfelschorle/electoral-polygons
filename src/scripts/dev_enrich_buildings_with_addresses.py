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


def _has_addr(df: pd.DataFrame, street_col: str, hn_col: str) -> pd.Series:
    street_ok = df[street_col].fillna("").astype(str).str.strip().ne("")
    hn_ok = df[hn_col].fillna("").astype(str).str.strip().ne("")
    return street_ok & hn_ok


def enrich_buildings_with_addresses(
    buildings_path: str,
    buildings_layer: str,
    addresses_path: str,
    addresses_layer: str,
    out_path: str,
    *,
    # columns
    building_id_col: str = "osm_id",
    b_street_col: str = "addr_street",
    b_housenumber_col: str = "addr_housenumber",
    a_id_col: str = "addr_id",
    a_street_col: str = "addr_street",
    a_housenumber_col: str = "addr_housenumber",
    # matching params
    max_nearest_m: float = 30.0,
) -> None:
    # --- Load
    b = gpd.read_file(buildings_path, layer=buildings_layer)
    a = gpd.read_file(addresses_path, layer=addresses_layer)

    if b.crs is None or a.crs is None:
        raise ValueError("Both buildings and addresses must have CRS set.")
    if b.crs != a.crs:
        a = a.to_crs(b.crs)

    b = b.copy()
    a = a.copy()

    # --- IDs
    if building_id_col not in b.columns:
        if "fid" in b.columns:
            building_id_col = "fid"
        else:
            b["building_id"] = range(1, len(b) + 1)
            building_id_col = "building_id"

    if a_id_col not in a.columns:
        if "fid" in a.columns:
            a_id_col = "fid"
        else:
            a["addr_id"] = range(1, len(a) + 1)
            a_id_col = "addr_id"

    # --- Required address fields
    for col in [a_street_col, a_housenumber_col]:
        if col not in a.columns:
            raise ValueError(f"Address column not found in addresses layer: {col}")

    for col in [b_street_col, b_housenumber_col]:
        if col not in b.columns:
            raise ValueError(f"Address column not found in buildings layer: {col}")

    # --- Normalize
    a[a_street_col] = _norm_street(a[a_street_col])
    a[a_housenumber_col] = _norm_housenumber(a[a_housenumber_col])

    b[b_street_col] = _norm_street(b[b_street_col])
    b[b_housenumber_col] = _norm_housenumber(b[b_housenumber_col])

    # --- Split buildings: keep existing addresses, fill only missing
    b["has_addr_orig"] = _has_addr(b, b_street_col, b_housenumber_col)
    b_missing = b[~b["has_addr_orig"]].copy()
    b_existing = b[b["has_addr_orig"]].copy()

    # Representative points for nearest joins (only missing buildings)
    b_missing_rep = gpd.GeoDataFrame(
        {"building_id": b_missing[building_id_col].astype(str)},
        geometry=b_missing.geometry.representative_point(),
        crs=b.crs,
    )

    link_frames = []

    # --- A) Existing building-address as links (so SV selection works uniformly)
    if len(b_existing) > 0:
        existing_links = gpd.GeoDataFrame(
            {
                "building_id": b_existing[building_id_col].astype(str),
                "addr_id": pd.NA,
                "addr_street": b_existing[b_street_col],
                "addr_housenumber": b_existing[b_housenumber_col],
                "match_type": "existing",
                "dist_m": 0.0,
            },
            geometry=b_existing.geometry.representative_point(),
            crs=b.crs,
        )
        link_frames.append(existing_links)

    # --- B) Within join: address points inside missing buildings
    if len(b_missing) > 0:
        within = gpd.sjoin(
            a[[a_id_col, a_street_col, a_housenumber_col, "geometry"]].copy(),
            b_missing[[building_id_col, "geometry"]].rename(columns={building_id_col: "building_id"}),
            how="left",
            predicate="within",
        ).drop(columns=["index_right"])

        within["building_id"] = within["building_id"].astype("string")
        within["match_type"] = "within"
        within["dist_m"] = 0.0

        matched_within = within[within["building_id"].notna()].copy()
        leftover = within[within["building_id"].isna()].copy()

        # --- C) Nearest join for leftovers (still only missing buildings)
        if len(leftover) > 0:
            nearest = gpd.sjoin_nearest(
                leftover.drop(columns=["building_id", "match_type", "dist_m"], errors="ignore"),
                b_missing_rep,
                how="left",
                max_distance=max_nearest_m,
                distance_col="dist_m",
            ).drop(columns=["index_right"])

            nearest["building_id"] = nearest["building_id"].astype("string")
            nearest["match_type"] = "nearest"

            matched_nearest = nearest[nearest["building_id"].notna()].copy()
            unmatched_final = nearest[nearest["building_id"].isna()].copy()
        else:
            matched_nearest = gpd.GeoDataFrame(columns=within.columns, geometry="geometry", crs=b.crs)
            unmatched_final = gpd.GeoDataFrame(columns=within.columns, geometry="geometry", crs=b.crs)

        point_links = pd.concat([matched_within, matched_nearest], ignore_index=True)
        if len(point_links) > 0:
            point_links = point_links.rename(
                columns={
                    a_id_col: "addr_id",
                    a_street_col: "addr_street",
                    a_housenumber_col: "addr_housenumber",
                }
            )
            point_links = gpd.GeoDataFrame(
                point_links[["building_id", "addr_id", "addr_street", "addr_housenumber", "match_type", "dist_m", "geometry"]],
                geometry="geometry",
                crs=b.crs,
            )
            point_links["building_id"] = point_links["building_id"].astype(str)
            link_frames.append(point_links)

        addr_unmatched = unmatched_final.rename(
            columns={
                a_id_col: "addr_id",
                a_street_col: "addr_street",
                a_housenumber_col: "addr_housenumber",
            }
        )
        addr_unmatched = gpd.GeoDataFrame(
            addr_unmatched[["addr_id", "addr_street", "addr_housenumber", "geometry"]],
            geometry="geometry",
            crs=b.crs,
        )
    else:
        addr_unmatched = gpd.GeoDataFrame(
            columns=["addr_id", "addr_street", "addr_housenumber", "geometry"],
            geometry="geometry",
            crs=b.crs,
        )

    # --- Combine links
    if len(link_frames) == 0:
        link = gpd.GeoDataFrame(
            columns=["building_id", "addr_id", "addr_street", "addr_housenumber", "match_type", "dist_m", "geometry"],
            geometry="geometry",
            crs=b.crs,
        )
    else:
        link = pd.concat(link_frames, ignore_index=True)
        link = gpd.GeoDataFrame(link, geometry="geometry", crs=b.crs)

    # --- Enrich buildings with addr_count + fill ONE representative addr into missing buildings (no overwrite)
    b_enriched = b.copy()
    b_enriched["_building_id_str"] = b_enriched[building_id_col].astype(str)

    addr_count = link.groupby("building_id").size().rename("addr_count").reset_index()
    b_enriched = b_enriched.merge(addr_count, how="left", left_on="_building_id_str", right_on="building_id")
    b_enriched["addr_count"] = b_enriched["addr_count"].fillna(0).astype(int)
    b_enriched = b_enriched.drop(columns=["building_id"], errors="ignore")

    # Fill missing addr fields from point links (within/nearest) only
    point_only = link[link["match_type"].isin(["within", "nearest"])].copy()
    if len(point_only) > 0:
        point_only["dist_m"] = pd.to_numeric(point_only["dist_m"], errors="coerce").fillna(0.0)
        point_first = (
            point_only.sort_values(["building_id", "dist_m"])
            .drop_duplicates(subset=["building_id"], keep="first")
            .set_index("building_id")
        )

        missing_mask = ~b_enriched["has_addr_orig"]
        ids_missing = b_enriched.loc[missing_mask, "_building_id_str"]

        b_enriched.loc[missing_mask, b_street_col] = ids_missing.map(point_first["addr_street"]).fillna(
            b_enriched.loc[missing_mask, b_street_col]
        )
        b_enriched.loc[missing_mask, b_housenumber_col] = ids_missing.map(point_first["addr_housenumber"]).fillna(
            b_enriched.loc[missing_mask, b_housenumber_col]
        )

    b_enriched["has_addr_final"] = _has_addr(b_enriched, b_street_col, b_housenumber_col)
    b_enriched = b_enriched.drop(columns=["_building_id_str"], errors="ignore")

    # --- Write
    link.to_file(out_path, layer="building_address_links", driver="GPKG")
    b_enriched.to_file(out_path, layer="buildings_enriched", driver="GPKG")
    if len(addr_unmatched) > 0:
        addr_unmatched.to_file(out_path, layer="addr_unmatched", driver="GPKG")

    print("Wrote:", out_path)
    print("Layers:")
    print(" - building_address_links:", len(link))
    print(" - buildings_enriched:", len(b_enriched))
    print(" - addr_unmatched:", len(addr_unmatched))
    print("Stats:")
    print(" - buildings had addr originally:", int(b["has_addr_orig"].sum()))
    print(" - buildings have addr after:", int(b_enriched["has_addr_final"].sum()))
    print(" - point links added:", int((link["match_type"].isin(["within", "nearest"])).sum()))


if __name__ == "__main__":
    ASSETS_GPKG = r"src/src/electoral_polygons/assets/bucharest_osm_assets.gpkg"

    BUILDINGS_PATH = ASSETS_GPKG
    BUILDINGS_LAYER = "main_buildings"

    ADDR_PATH = ASSETS_GPKG
    ADDR_LAYER = "addresses"

    OUT_PATH = "src/data/scratch/buildings_with_addresses.gpkg"

    enrich_buildings_with_addresses(
        buildings_path=BUILDINGS_PATH,
        buildings_layer=BUILDINGS_LAYER,
        addresses_path=ADDR_PATH,
        addresses_layer=ADDR_LAYER,
        out_path=OUT_PATH,
        building_id_col="osm_id",
        b_street_col="addr_street",
        b_housenumber_col="addr_housenumber",
        a_id_col="addr_id",
        a_street_col="addr_street",
        a_housenumber_col="addr_housenumber",
        max_nearest_m=30.0,
    )
