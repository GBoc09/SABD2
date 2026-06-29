#!/usr/bin/env python3
"""
Confronto Query 1 - medie dei risultati tra strategie di watermarking.

Input attesi, di default in output/query1:
  - query1_WM15.csv
  - query1_WM100.csv
  - query1_ADAPTIVE.csv

Output:
  - query1_watermark_mean_comparison.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "output" / "query1"
DEFAULT_METRICS = [
    "num_flights",
    "completed",
    "cancelled",
    "diverted",
    "dep_delay_mean",
    "cancellation_rate",
    "late_departure_rate",
]
OUTPUT_COLUMNS = [
    "scope",
    "airline",
    "metric",
    "wm15_mean",
    "wm100_mean",
    "adaptive_mean",
    "abs_diff_wm100_minus_wm15",
    "pct_diff_wm100_vs_wm15",
    "abs_diff_adaptive_minus_wm15",
    "pct_diff_adaptive_vs_wm15",
    "wm15_non_null_values",
    "wm100_non_null_values",
    "adaptive_non_null_values",
]


def parse_number(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def mean(values: Iterable[float | None]) -> float | None:
    nums = [value for value in values if value is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def rounded(value: float | None) -> float | None:
    return round(value, 6) if value is not None else None


def pct_diff(value: float | None, baseline: float | None) -> float | None:
    if value is None or baseline is None or baseline == 0:
        return None
    return (value - baseline) / baseline * 100.0


def resolve_input_dir(input_dir: str | None) -> Path:
    return Path(input_dir) if input_dir else DEFAULT_INPUT_DIR


def read_query1(path: Path) -> list[dict[str, str | float | None]]:
    if not path.exists():
        raise FileNotFoundError(f"File non trovato: {path}")

    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        missing = {"airline", *DEFAULT_METRICS} - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name}: colonne mancanti: {sorted(missing)}")

        rows: list[dict[str, str | float | None]] = []
        for row in reader:
            parsed: dict[str, str | float | None] = {"airline": row.get("airline", "")}
            for metric in DEFAULT_METRICS:
                parsed[metric] = parse_number(row.get(metric))
            rows.append(parsed)
        return rows


def metric_values(rows: list[dict[str, str | float | None]], metric: str) -> list[float | None]:
    return [row[metric] if isinstance(row[metric], float) else None for row in rows]


def comparison_rows(
    wm15: list[dict[str, str | float | None]],
    wm100: list[dict[str, str | float | None]],
    adaptive: list[dict[str, str | float | None]],
    metrics: Iterable[str],
    group_col: str | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    if group_col is None:
        groups = [("ALL", None)]
    else:
        values = sorted(
            {
                str(row[group_col])
                for row in [*wm15, *wm100, *adaptive]
                if row.get(group_col) not in (None, "")
            }
        )
        groups = [(value, value) for value in values]

    for label, value in groups:
        wm_part = wm15 if group_col is None else [row for row in wm15 if row[group_col] == value]
        wm100_part = wm100 if group_col is None else [row for row in wm100 if row[group_col] == value]
        ad_part = adaptive if group_col is None else [row for row in adaptive if row[group_col] == value]

        for metric in metrics:
            wm_values = metric_values(wm_part, metric)
            wm100_values = metric_values(wm100_part, metric)
            ad_values = metric_values(ad_part, metric)
            wm_mean = mean(wm_values)
            wm100_mean = mean(wm100_values)
            ad_mean = mean(ad_values)
            wm100_diff = wm100_mean - wm_mean if wm_mean is not None and wm100_mean is not None else None
            wm100_pct = pct_diff(wm100_mean, wm_mean)
            adaptive_diff = ad_mean - wm_mean if wm_mean is not None and ad_mean is not None else None
            adaptive_pct = pct_diff(ad_mean, wm_mean)
            rows.append(
                {
                    "scope": "ALL" if group_col is None else group_col,
                    "airline": "ALL" if group_col is None else label,
                    "metric": metric,
                    "wm15_mean": rounded(wm_mean),
                    "wm100_mean": rounded(wm100_mean),
                    "adaptive_mean": rounded(ad_mean),
                    "abs_diff_wm100_minus_wm15": rounded(wm100_diff),
                    "pct_diff_wm100_vs_wm15": rounded(wm100_pct),
                    "abs_diff_adaptive_minus_wm15": rounded(adaptive_diff),
                    "pct_diff_adaptive_vs_wm15": rounded(adaptive_pct),
                    "wm15_non_null_values": sum(value is not None for value in wm_values),
                    "wm100_non_null_values": sum(value is not None for value in wm100_values),
                    "adaptive_non_null_values": sum(value is not None for value in ad_values),
                }
            )
    return rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Confronta le medie della Query 1 tra WM15, WM100 e ADAPTIVE.")
    parser.add_argument("--input-dir", default=None, help="Cartella contenente i CSV di input. Default: output/query1.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Cartella in cui salvare il CSV di output. Default: stessa cartella degli input.",
    )
    parser.add_argument("--wm15", default="query1_WM15.csv", help="Nome/path del CSV Q1 WM15.")
    parser.add_argument("--wm100", default="query1_WM100.csv", help="Nome/path del CSV Q1 WM100.")
    parser.add_argument("--adaptive", default="query1_ADAPTIVE.csv", help="Nome/path del CSV Q1 ADAPTIVE.")
    parser.add_argument("--output", default="query1_watermark_mean_comparison.csv", help="Nome del CSV prodotto.")
    args = parser.parse_args()

    input_dir = resolve_input_dir(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    wm15_path = Path(args.wm15) if Path(args.wm15).is_absolute() else input_dir / args.wm15
    wm100_path = Path(args.wm100) if Path(args.wm100).is_absolute() else input_dir / args.wm100
    adaptive_path = Path(args.adaptive) if Path(args.adaptive).is_absolute() else input_dir / args.adaptive

    wm15 = read_query1(wm15_path)
    wm100 = read_query1(wm100_path)
    adaptive = read_query1(adaptive_path)

    rows = []
    rows.extend(comparison_rows(wm15, wm100, adaptive, DEFAULT_METRICS))
    rows.extend(comparison_rows(wm15, wm100, adaptive, DEFAULT_METRICS, group_col="airline"))

    out_path = output_dir / args.output
    write_csv(out_path, rows)
    print(f"Creato: {out_path}")


if __name__ == "__main__":
    main()
