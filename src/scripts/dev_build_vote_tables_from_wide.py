from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


# --- edit if you want consistent political colors in GIS ---
PARTY_COLOR: Dict[str, str] = {
    # Examples (edit to your needs):
    # "UNIUNEA SALVAȚI ROMÂNIA": "#2e7dff",
    # "PARTIDUL SOCIAL DEMOCRAT": "#d50000",
    # "PARTIDUL NAȚIONAL LIBERAL": "#f2c200",
    # "ALIANȚA PENTRU UNIREA ROMÂNILOR": "#f57c00",
    # "INDEPENDENT": "#808080",
}

JOIN_KEY_OUT = "precinct_nr"


def read_csv_robust(path: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "cp1250", "latin1"]
    seps = [",", ";", "\t", "|"]

    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep, dtype=str, low_memory=False)
                if df.shape[1] <= 1:
                    continue
                return df
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(f"Failed reading {path} (last error: {last_err})")


def normalize_text(s: pd.Series) -> pd.Series:
    # minimal normalize for filtering Bucuresti/București
    x = s.astype(str).str.strip()
    x = x.str.replace("ă", "a").str.replace("â", "a").str.replace("î", "i").str.replace("ș", "s").str.replace("ş", "s")
    x = x.str.replace("ț", "t").str.replace("ţ", "t")
    return x.str.lower()


def to_int_df(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        s = out[c].astype(str).str.strip()
        s = s.replace({"": None, "nan": None, "None": None})
        s = s.str.replace(".", "", regex=False).str.replace(",", "", regex=False)
        out[c] = pd.to_numeric(s, errors="coerce").fillna(0).astype(int)
    return out


def find_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cols = [c.lower().strip() for c in df.columns]
    mapping = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in mapping:
            return mapping[cand.lower()]
    # fallback: partial match
    for c in df.columns:
        cl = c.lower()
        for cand in candidates:
            if cand.lower() in cl:
                return c
    raise ValueError(f"Could not find any of columns: {candidates}")


def parse_candidate_col(col: str) -> Tuple[str, str]:
    """
    Column examples:
      'DRULĂ CĂTĂLIN-UNIUNEA SALVAȚI ROMÂNIA-voturi'
      'BĂLUȚĂ DANIEL-PARTIDUL SOCIAL DEMOCRAT-voturi'
      'X Y-INDEPENDENT-voturi'
    """
    base = col
    if base.lower().endswith("-voturi"):
        base = base[:-len("-voturi")]

    parts = [p.strip() for p in base.split("-") if p.strip()]

    if len(parts) >= 2:
        name = parts[0]
        party = "-".join(parts[1:]).strip()
    else:
        name = base.strip()
        party = ""

    if party.lower() in ("independent", "independent̆", "independentă", "independent a") or party == "":
        # keep empty as-is; many datasets use INDEPENDENT explicitly anyway
        pass

    return name, party


def build_tables(input_csv: Path, out_dir: Path, write_xlsx: bool) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    df = read_csv_robust(input_csv)

    # Required fields
    precinct_col = find_col(df, ["precinct_nr", "precinct", "sectie", "nr_sectie", "nrsectie"])
    uat_col = find_col(df, ["uat", "uat_name", "localitate", "comuna", "siruta", "unitate"])

    # Filter to Bucharest (handles București/Bucuresti) + sectors
    uat_norm = normalize_text(df[uat_col])
    is_buc = uat_norm.str.contains("bucuresti", na=False) | uat_norm.str.contains("sector", na=False)
    df = df.loc[is_buc].copy()

    # Vote columns: anything ending in "-voturi" (case-insensitive)
    vote_cols = [c for c in df.columns if str(c).strip().lower().endswith("-voturi")]
    if not vote_cols:
        raise ValueError("No vote columns found ending with '-voturi'.")

    # Keep join key + votes only for the wide join table
    wide = df[[precinct_col] + vote_cols].copy()
    wide = wide.rename(columns={precinct_col: JOIN_KEY_OUT})

    # Ensure ints
    wide = to_int_df(wide, vote_cols)

    # Winner per row (SV)
    votes_matrix = wide[vote_cols]
    winner_idx = votes_matrix.values.argmax(axis=1)
    winner_votes = votes_matrix.max(axis=1)

    winner_col = pd.Series([vote_cols[i] for i in winner_idx], index=wide.index, name="winner_col")
    parsed = winner_col.apply(parse_candidate_col)
    winner_name = parsed.apply(lambda t: t[0]).rename("winner_name")
    winner_party = parsed.apply(lambda t: t[1]).rename("winner_party")

    total_votes = votes_matrix.sum(axis=1).rename("total_votes_sum")
    winner_share = (winner_votes / total_votes.replace(0, pd.NA)).fillna(0).rename("winner_share")

    summary = pd.concat(
        [
            wide[[JOIN_KEY_OUT]],
            winner_col,
            winner_name,
            winner_party,
            winner_votes.rename("winner_votes"),
            total_votes,
            winner_share,
        ],
        axis=1,
    )

    # optional color column
    summary["winner_color"] = summary["winner_party"].map(PARTY_COLOR).fillna("")

    stem = input_csv.stem
    wide_path = out_dir / f"{stem}__sv_votes_wide.csv"
    summ_path = out_dir / f"{stem}__sv_winner_summary.csv"

    wide.to_csv(wide_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summ_path, index=False, encoding="utf-8-sig")

    if write_xlsx:
        xlsx_path = out_dir / f"{stem}__derived_tables.xlsx"
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
            wide.to_excel(xw, sheet_name="sv_votes_wide", index=False)
            summary.to_excel(xw, sheet_name="sv_winner_summary", index=False)

    print(f"[ok] Filtered rows (Bucuresti): {len(df):,}")
    print(f"[ok] Vote columns: {len(vote_cols)}")
    print(f"[ok] wrote: {wide_path}")
    print(f"[ok] wrote: {summ_path}")
    if write_xlsx:
        print(f"[ok] wrote: {out_dir / f'{stem}__derived_tables.xlsx'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default="rezultate_vot/raw/pv_part_cnty_pcj_b.csv",
        help="Path to raw wide results CSV (candidate columns end with -voturi)",
    )
    ap.add_argument("--out-dir", default="rezultate_vot/processed")
    ap.add_argument("--xlsx", action="store_true")
    args = ap.parse_args()

    build_tables(Path(args.input), Path(args.out_dir), write_xlsx=bool(args.xlsx))


if __name__ == "__main__":
    main()
