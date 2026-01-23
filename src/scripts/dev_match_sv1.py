from __future__ import annotations

from pathlib import Path

from electoral_polygons.rules_parser import parse_sv
from electoral_polygons.match_addresses import match_sv_addresses


def main() -> None:
    xlsx = "src/data/raw/RSV-17.11.2025-----Extras-partiale-BUC.xlsx"
    gpkg = "src/src/electoral_polygons/assets/bucharest_osm_assets.gpkg"

    sv1 = parse_sv(xlsx_path=xlsx, judet="B", sv=1)

    


    matched = match_sv_addresses(
        addresses_gpkg=gpkg,
        sv_parsed=sv1,
        layer="addresses",
    )

    print("After dedupe:", len(matched))
    print(matched[["addr_street", "addr_housenumber", "addr_housenumber_int", "addr_id"]]
        .sort_values(["addr_street","addr_housenumber_int"])
        .to_string(index=False))
    print(f"Matched addresses for SV{sv1['sv']}: {len(matched)}")
    print(matched[["addr_street", "addr_housenumber"]].sort_values(["addr_street","addr_housenumber"]).to_string(index=False))


    out_dir = Path("src/data/scratch")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "sv1_addresses.gpkg"
    matched.to_file(out_path, layer="sv1_addresses", driver="GPKG")

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
