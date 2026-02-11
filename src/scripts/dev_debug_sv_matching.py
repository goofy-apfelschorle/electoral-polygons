# src/scripts/dev_debug_sv_matching.py
from __future__ import annotations

from pathlib import Path

from electoral_polygons.rules_parser import parse_sv
from electoral_polygons.match_addresses import match_sv_addresses


def _resolve_assets_gpkg(root: Path) -> Path:
    candidates = [
        root / "src" / "electoral_polygons" / "assets" / "bucharest_osm_assets.gpkg",
        root / "src" / "src" / "electoral_polygons" / "assets" / "bucharest_osm_assets.gpkg",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("Could not find bucharest_osm_assets.gpkg in expected locations.")


def main() -> None:
    ROOT = Path(__file__).resolve().parents[2]
    xlsx = ROOT / "src" / "data" / "raw" / "RSV-17.11.2025-----Extras-partiale-BUC.xlsx"
    assets_gpkg = _resolve_assets_gpkg(ROOT)

    # include SV100 too (you mentioned Dorobanți big SV seems wrong)
    sv_ids = [68, 100, 101, 167, 337, 338, 339, 340]

    for sv in sv_ids:
        print("\n" + "=" * 100)
        print(f"SV {sv}")
        sv_parsed = parse_sv(xlsx_path=str(xlsx), judet="B", sv=sv)

        matched = match_sv_addresses(
            addresses_gpkg=str(assets_gpkg),
            sv_parsed=sv_parsed,
            layer="addresses",
            debug=True,
        )
        print(f"[RESULT] SV{sv}: matched addresses = {len(matched)}")


if __name__ == "__main__":
    main()
