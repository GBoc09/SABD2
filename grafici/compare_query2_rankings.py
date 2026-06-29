#!/usr/bin/env python3
"""
Confronto Query 2 - verifica se la classifica cambia tra strategie di watermarking.

Input attesi, di default in output/query2:
  - query2_1h_WM15.csv
  - query2_1h_ADAPTIVE.csv
  - query2_6h_WM15.csv
  - query2_6h_ADAPTIVE.csv
  - query2_global_WM15.csv
  - query2_global_ADAPTIVE.csv

Output:
  - query2_watermark_ranking_comparison.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "output" / "query2"
WINDOW_FILES = {
    "1h": ("query2_1h_WM15.csv", "query2_1h_ADAPTIVE.csv"),
    "6h": ("query2_6h_WM15.csv", "query2_6h_ADAPTIVE.csv"),
    "global": ("query2_global_WM15.csv", "query2_global_ADAPTIVE.csv"),
}
REQUIRED_COLUMNS = {"ts", "rank", "origin_airport_id"}
OUTPUT_COLUMNS = [
    "window_type",
    "ts",
    "same_ranked_airports",
    "same_airport_set",
    "num_positions_changed",
    "wm15_top10",
    "adaptive_top10",
    "positions_changed_detail",
    "airports_only_in_wm15",
    "airports_only_in_adaptive",
    "wm15_rank_count",
    "adaptive_rank_count",
]


def parse_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def resolve_input_dir(input_dir: str | None) -> Path:
    return Path(input_dir) if input_dir else DEFAULT_INPUT_DIR


def read_query2(path: Path) -> list[dict[str, str | int | None]]:
    if not path.exists():
        raise FileNotFoundError(f"File non trovato: {path}")

    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name}: colonne mancanti: {sorted(missing)}")

        rows: list[dict[str, str | int | None]] = []
        for row in reader:
            rows.append(
                {
                    "ts": str(row.get("ts", "")),
                    "rank": parse_int(row.get("rank")),
                    "origin_airport_id": parse_int(row.get("origin_airport_id")),
                }
            )
        return rows


def ranking_for_timestamp(rows: list[dict[str, str | int | None]], ts: str, top_n: int) -> list[int]:
    part = [
        row
        for row in rows
        if row["ts"] == ts and row["rank"] is not None and row["origin_airport_id"] is not None
    ]
    part.sort(key=lambda row: (int(row["rank"]), int(row["origin_airport_id"])))
    return [int(row["origin_airport_id"]) for row in part[:top_n]]


def compare_window(
    window_type: str,
    wm15: list[dict[str, str | int | None]],
    adaptive: list[dict[str, str | int | None]],
    top_n: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    all_ts = sorted({str(row["ts"]) for row in wm15} | {str(row["ts"]) for row in adaptive})

    for ts in all_ts:
        wm_rank = ranking_for_timestamp(wm15, ts, top_n)
        ad_rank = ranking_for_timestamp(adaptive, ts, top_n)

        changed_positions = []
        for i in range(max(len(wm_rank), len(ad_rank))):
            wm_airport = wm_rank[i] if i < len(wm_rank) else None
            ad_airport = ad_rank[i] if i < len(ad_rank) else None
            if wm_airport != ad_airport:
                changed_positions.append(f"rank {i + 1}: WM15={wm_airport}, ADAPTIVE={ad_airport}")

        wm_set = set(wm_rank)
        ad_set = set(ad_rank)
        rows.append(
            {
                "window_type": window_type,
                "ts": ts,
                "same_ranked_airports": wm_rank == ad_rank,
                "same_airport_set": wm_set == ad_set,
                "num_positions_changed": len(changed_positions),
                "wm15_top10": "|".join(map(str, wm_rank)),
                "adaptive_top10": "|".join(map(str, ad_rank)),
                "positions_changed_detail": "; ".join(changed_positions),
                "airports_only_in_wm15": "|".join(map(str, sorted(wm_set - ad_set))),
                "airports_only_in_adaptive": "|".join(map(str, sorted(ad_set - wm_set))),
                "wm15_rank_count": len(wm_rank),
                "adaptive_rank_count": len(ad_rank),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows: list[dict[str, object]]) -> None:
    if not rows:
        print("Nessuna finestra da confrontare.")
        return

    print("\nSintesi:")
    print("window_type compared_windows identical_rankings same_airport_sets windows_with_rank_changes")
    for window_type in sorted({str(row["window_type"]) for row in rows}):
        part = [row for row in rows if row["window_type"] == window_type]
        identical = sum(row["same_ranked_airports"] is True for row in part)
        same_sets = sum(row["same_airport_set"] is True for row in part)
        changed = sum(row["same_ranked_airports"] is not True for row in part)
        print(f"{window_type:11} {len(part):16} {identical:18} {same_sets:17} {changed:25}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Confronta le classifiche Query 2 tra WM15 e ADAPTIVE.")
    parser.add_argument("--input-dir", default=None, help="Cartella contenente i CSV di input. Default: output/query2.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Cartella in cui salvare il CSV di output. Default: stessa cartella degli input.",
    )
    parser.add_argument("--top-n", type=int, default=10, help="Numero di posizioni da confrontare per ranking.")
    parser.add_argument("--output", default="query2_watermark_ranking_comparison.csv", help="Nome del CSV prodotto.")
    args = parser.parse_args()

    if args.top_n <= 0:
        raise ValueError("--top-n deve essere maggiore di zero.")

    input_dir = resolve_input_dir(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for window_type, (wm_name, ad_name) in WINDOW_FILES.items():
        wm15 = read_query2(input_dir / wm_name)
        adaptive = read_query2(input_dir / ad_name)
        rows.extend(compare_window(window_type, wm15, adaptive, args.top_n))

    out_path = output_dir / args.output
    write_csv(out_path, rows)
    print(f"Creato: {out_path}")
    print_summary(rows)


if __name__ == "__main__":
    main()
