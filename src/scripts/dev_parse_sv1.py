from __future__ import annotations

from pprint import pprint
from pathlib import Path

from electoral_polygons.rules_parser import parse_sv


def main() -> None:
    # Repo root assumed: running from project root
    # Adjust if you run from elsewhere
    xlsx = "src/data/raw/RSV-17.11.2025-----Extras-partiale-BUC.xlsx"

    sv1 = parse_sv(xlsx_path=xlsx, judet="B", sv=1)
    pprint(sv1)

    # Optional: write a JSON debug artifact into scratch
    try:
        import json
        out_dir = Path("src/data/scratch")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "sv1_parsed.json"
        out_path.write_text(json.dumps(sv1, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nWrote: {out_path}")
    except Exception as e:
        print(f"\n(Info) Could not write JSON debug artifact: {e}")


if __name__ == "__main__":
    main()
