#!/usr/bin/env python3
"""
Confronto Query 3 - media di min, max e percentili tra strategie di watermarking.

Input attesi, di default in output/query3:
  - query3_1day_WM15.csv
  - query3_1day_WM100.csv
  - query3_1day_ADAPTIVE.csv
  - query3_7day_WM15.csv
  - query3_7day_WM100.csv
  - query3_7day_ADAPTIVE.csv
  - query3_global_WM15.csv
  - query3_global_WM100.csv
  - query3_global_ADAPTIVE.csv

Output:
  - query3_watermark_mean_comparison.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "output" / "query3"
WINDOW_FILES = {
    "1day": ("query3_1day_WM15.csv", "query3_1day_WM100.csv", "query3_1day_ADAPTIVE.csv"),
    "7day": ("query3_7day_WM15.csv", "query3_7day_WM100.csv", "query3_7day_ADAPTIVE.csv"),
    "global": ("query3_global_WM15.csv", "query3_global_WM100.csv", "query3_global_ADAPTIVE.csv"),
}
METRICS = ["min", "p25", "p50", "p75", "p90", "max"]
REQUIRED_COLUMNS = {"airline", "hour", *METRICS}
OUTPUT_COLUMNS = [
    "window_type",
    "scope",
    "airline",
    "hour",
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


def parse_int(value: str | None) -> int | None:
    number = parse_number(value)
    return int(number) if number is not None else None


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


def read_query3(path: Path) -> list[dict[str, str | int | float | None]]:
    if not path.exists():
        raise FileNotFoundError(f"File non trovato: {path}")

    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name}: colonne mancanti: {sorted(missing)}")

        rows: list[dict[str, str | int | float | None]] = []
        for row in reader:
            parsed: dict[str, str | int | float | None] = {
                "airline": row.get("airline", ""),
                "hour": parse_int(row.get("hour")),
            }
            for metric in METRICS:
                parsed[metric] = parse_number(row.get(metric))
            rows.append(parsed)
        return rows


def metric_values(rows: list[dict[str, str | int | float | None]], metric: str) -> list[float | None]:
    return [row[metric] if isinstance(row[metric], float) else None for row in rows]


def emit_metric_rows(
    rows: list[dict[str, object]],
    window_type: str,
    scope: str,
    label_airline: str,
    label_hour: str | int,
    wm_part: list[dict[str, str | int | float | None]],
    wm100_part: list[dict[str, str | int | float | None]],
    ad_part: list[dict[str, str | int | float | None]],
    metrics: Iterable[str],
) -> None:
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
                "window_type": window_type,
                "scope": scope,
                "airline": label_airline,
                "hour": label_hour,
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


def compare_window(
    window_type: str,
    wm15: list[dict[str, str | int | float | None]],
    wm100: list[dict[str, str | int | float | None]],
    adaptive: list[dict[str, str | int | float | None]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    emit_metric_rows(rows, window_type, "ALL", "ALL", "ALL", wm15, wm100, adaptive, METRICS)

    airlines = sorted(
        {
            str(row["airline"])
            for row in [*wm15, *wm100, *adaptive]
            if row.get("airline") not in (None, "")
        }
    )
    for airline in airlines:
        wm_air = [row for row in wm15 if row["airline"] == airline]
        wm100_air = [row for row in wm100 if row["airline"] == airline]
        ad_air = [row for row in adaptive if row["airline"] == airline]
        emit_metric_rows(rows, window_type, "airline", airline, "ALL", wm_air, wm100_air, ad_air, METRICS)

    keys = sorted(
        {
            (str(row["airline"]), row["hour"])
            for row in [*wm15, *wm100, *adaptive]
            if row.get("airline") not in (None, "") and row.get("hour") is not None
        }
    )
    for airline, hour in keys:
        wm_part = [row for row in wm15 if row["airline"] == airline and row["hour"] == hour]
        wm100_part = [row for row in wm100 if row["airline"] == airline and row["hour"] == hour]
        ad_part = [row for row in adaptive if row["airline"] == airline and row["hour"] == hour]
        emit_metric_rows(rows, window_type, "airline_hour", airline, int(hour), wm_part, wm100_part, ad_part, METRICS)

    return rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Confronta le medie delle statistiche Query 3 tra WM15, WM100 e ADAPTIVE.")
    parser.add_argument("--input-dir", default=None, help="Cartella contenente i CSV di input. Default: output/query3.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Cartella in cui salvare il CSV di output. Default: stessa cartella degli input.",
    )
    parser.add_argument("--output", default="query3_watermark_mean_comparison.csv", help="Nome del CSV prodotto.")
    args = parser.parse_args()

    input_dir = resolve_input_dir(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for window_type, (wm_name, wm100_name, ad_name) in WINDOW_FILES.items():
        wm15 = read_query3(input_dir / wm_name)
        wm100 = read_query3(input_dir / wm100_name)
        adaptive = read_query3(input_dir / ad_name)
        rows.extend(compare_window(window_type, wm15, wm100, adaptive))

    out_path = output_dir / args.output
    write_csv(out_path, rows)
    print(f"Creato: {out_path}")


if __name__ == "__main__":
    main()
