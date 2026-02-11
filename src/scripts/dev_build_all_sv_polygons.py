# src/scripts/dev_build_all_sv_polygons.py
from __future__ import annotations

from pathlib import Path
import geopandas as gpd
import pandas as pd

from electoral_polygons.rules_parser import parse_sv
from electoral_polygons.match_addresses import match_sv_addresses
from electoral_polygons.polygonize import build_auto_polygon


def list_sv_ids_from_excel(xlsx_path: str, judet: str) -> list[int]:
    """
    Extract all SV ids for a county code (judet) from the raw XLSX.
    Tries to auto-detect the SV-id column.
    """
    df = pd.read_excel(xlsx_path, sheet_name=0, dtype=object)

    judet_cols = [
        c
        for c in df.columns
        if isinstance(c, str)
        and c.strip().lower() in ("judet", "județ", "cod judet", "cod județ", "judet ")
    ]
    if judet_cols:
        df_b = df[df[judet_cols[0]].astype(str).str.strip().str.upper() == judet.upper()].copy()
    else:
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

    sv_ids = pd.to_numeric(df_b[sv_col], errors="coerce").dropna().astype(int).unique().tolist()
    sv_ids = sorted(set(sv_ids))
    print(
        f"[debug] Using SV column: {sv_col!r} -> {len(sv_ids)} SV ids "
        f"(min={min(sv_ids)}, max={max(sv_ids)})"
    )
    return sv_ids


def _resolve_assets_gpkg(root: Path) -> Path:
    """
    Your repo has historically had both:
      - src/electoral_polygons/assets/...
      - src/src/electoral_polygons/assets/...
    Pick the one that exists (and fail loudly if neither).
    """
    candidates = [
        root / "src" / "electoral_polygons" / "assets" / "bucharest_osm_assets.gpkg",
        root / "src" / "src" / "electoral_polygons" / "assets" / "bucharest_osm_assets.gpkg",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        "Could not find bucharest_osm_assets.gpkg. Tried:\n" + "\n".join(str(p) for p in candidates)
    )


def main() -> None:
    # Repo root: .../electoral_polygons
    ROOT = Path(__file__).resolve().parents[2]

    # Inputs
    xlsx = ROOT / "src" / "data" / "raw" / "RSV-17.11.2025-----Extras-partiale-BUC.xlsx"
    assets_gpkg = _resolve_assets_gpkg(ROOT)

    links_gpkg = ROOT / "src" / "data" / "scratch" / "buildings_with_addresses.gpkg"
    links_layer = "building_address_links"
    buildings_layer = "buildings_enriched"

    # Outputs (FULL RUN)
    out_dir = ROOT / "src" / "data" / "scratch"
    out_gpkg = out_dir / "all_sv_polygons_auto.gpkg"
    out_layer = "sv_polygons_auto"
    out_stats_csv = out_dir / "all_sv_polygons_auto_stats.csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Debug: path sanity
    print("[debug] ROOT:", ROOT)
    print("[debug] xlsx:", xlsx, "exists=", xlsx.exists())
    print("[debug] assets_gpkg:", assets_gpkg, "exists=", assets_gpkg.exists())
    print("[debug] links_gpkg:", links_gpkg, "exists=", links_gpkg.exists())

    # Debug: confirm we are importing the UPDATED code
    try:
        import inspect
        import electoral_polygons.match_addresses as ma
        import electoral_polygons.polygonize as pz

        print("[debug] match_addresses loaded from:", inspect.getsourcefile(ma))
        print("[debug] polygonize loaded from:", inspect.getsourcefile(pz))
        print("[debug] build_auto_polygon loaded from:", inspect.getsourcefile(build_auto_polygon))
    except Exception as e:
        print("[debug] inspect import paths failed:", e)

    if not xlsx.exists():
        raise FileNotFoundError(f"Missing XLSX: {xlsx}")
    if not links_gpkg.exists():
        raise FileNotFoundError(f"Missing links/buildings GPKG: {links_gpkg}")

    # Load once
    links = gpd.read_file(str(links_gpkg), layer=links_layer).copy()
    b = gpd.read_file(str(links_gpkg), layer=buildings_layer).copy()

    # Join key must match the one we will build from matched_addr
    links["addr_street"] = links["addr_street"].fillna("").astype(str).str.strip()
    links["addr_housenumber"] = links["addr_housenumber"].fillna("").astype(str).str.strip()
    links["key"] = links["addr_street"] + "|" + links["addr_housenumber"]

    b["_id_str"] = b["osm_id"].astype(str)

    polygons: list[dict] = []
    stats_rows: list[dict] = []

    # FULL SV list
    sv_ids = list_sv_ids_from_excel(str(xlsx), judet="B")

    for sv in sv_ids:
        try:
            sv_parsed = parse_sv(xlsx_path=str(xlsx), judet="B", sv=int(sv))
        except Exception as e:
            stats_rows.append({"sv": int(sv), "status": "parse_failed", "error": str(e)})
            continue

        matched_addr = match_sv_addresses(
            addresses_gpkg=str(assets_gpkg),
            sv_parsed=sv_parsed,
            layer="addresses",
            debug=False,
        )

        if matched_addr.empty:
            stats_rows.append({"sv": int(sv), "status": "no_addresses"})
            continue

        # IMPORTANT: build the SAME join key that links[] uses
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
                    "sv": int(sv),
                    "status": "no_buildings",
                    "addr_n": int(len(matched_addr)),
                    "link_n": int(len(sv_links)),
                }
            )
            continue

        try:
            poly, meta = build_auto_polygon(sv_buildings)
        except Exception as e:
            stats_rows.append(
                {
                    "sv": int(sv),
                    "status": "polygon_failed",
                    "addr_n": int(len(matched_addr)),
                    "link_n": int(len(sv_links)),
                    "bldg_n": int(len(sv_buildings)),
                    "error": str(e),
                }
            )
            continue

        polygons.append(
            {
                "sv": int(sv),
                "mode": meta.get("mode"),
                "addr_n": int(len(matched_addr)),
                "link_n": int(len(sv_links)),
                "bldg_n": int(len(sv_buildings)),
                **{k: v for k, v in meta.items() if k not in ("mode", "ensure_log")},
                "geometry": poly,
            }
        )

        stats_rows.append(
            {
                "sv": int(sv),
                "status": "ok",
                "mode": meta.get("mode"),
                "addr_n": int(len(matched_addr)),
                "link_n": int(len(sv_links)),
                "bldg_n": int(len(sv_buildings)),
                **{k: v for k, v in meta.items() if k != "ensure_log"},
            }
        )

        print(
            f"[SV{sv}] ok: addr={len(matched_addr)} links={len(sv_links)} "
            f"bldg={len(sv_buildings)} mode={meta.get('mode')}"
        )

    # Write outputs
    if polygons:
        out = gpd.GeoDataFrame(polygons, geometry="geometry", crs=b.crs)
        out.to_file(str(out_gpkg), layer=out_layer, driver="GPKG")

    pd.DataFrame(stats_rows).to_csv(str(out_stats_csv), index=False)

    print("Wrote:", out_gpkg, f"layer={out_layer}")
    print("Wrote:", out_stats_csv)


if __name__ == "__main__":
    main()
