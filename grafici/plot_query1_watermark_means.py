#!/usr/bin/env python3
"""
Grafico Query 1 - bar chart delle medie per strategia di watermarking.

Input:
  - query1_watermark_mean_comparison.csv, prodotto da compare_query1_watermark_means.py

Output:
  - query1_watermark_mean_histogram.png
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "output" / "query1" / "query1_watermark_mean_comparison.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output" / "query1" / "plot"
REQUIRED_COLUMNS = {"scope", "airline", "metric", "wm15_mean", "wm100_mean", "adaptive_mean"}
plt = None


def load_plot_dependencies() -> None:
    global plt
    if plt is not None:
        return
    try:
        os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as pyplot
    except ModuleNotFoundError as exc:
        missing = exc.name or "dipendenza Python"
        raise SystemExit(f"Dipendenza mancante: installa {missing} per usare questo script.") from exc

    plt = pyplot


def parse_number(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def plot_value(value: object) -> float:
    return value if isinstance(value, float) else float("nan")


def resolve_input(input_path: str | None) -> Path:
    return Path(input_path) if input_path else DEFAULT_INPUT


def read_comparison(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        raise FileNotFoundError(
            f"File non trovato: {path}. Esegui prima grafici/compare_query1_watermark_means.py."
        )

    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name}: colonne mancanti: {sorted(missing)}")

        rows: list[dict[str, object]] = []
        for row in reader:
            rows.append(
                {
                    "scope": row.get("scope", ""),
                    "airline": row.get("airline", ""),
                    "metric": row.get("metric", ""),
                    "wm15_mean": parse_number(row.get("wm15_mean")),
                    "wm100_mean": parse_number(row.get("wm100_mean")),
                    "adaptive_mean": parse_number(row.get("adaptive_mean")),
                }
            )
        return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Crea il grafico delle medie Query 1 per strategia.")
    parser.add_argument(
        "--input",
        default=None,
        help="CSV di confronto Q1. Default: output/query1/query1_watermark_mean_comparison.csv.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Cartella in cui salvare il grafico. Default: output/query1/plot.",
    )
    parser.add_argument("--output", default="query1_watermark_mean_histogram.png", help="Nome del PNG prodotto.")
    parser.add_argument(
        "--scope",
        default="ALL",
        choices=["ALL", "airline"],
        help="ALL genera il grafico globale; airline genera il grafico per compagnia.",
    )
    args = parser.parse_args()

    rows = read_comparison(resolve_input(args.input))
    if args.scope == "ALL":
        plot_rows = [row for row in rows if row["scope"] == "ALL"]
        title = "Query 1 - medie globali per strategia di watermarking"
    else:
        plot_rows = [row for row in rows if row["scope"] == "airline"]
        for row in plot_rows:
            row["metric"] = f"{row['airline']} / {row['metric']}"
        title = "Query 1 - medie per compagnia e strategia di watermarking"

    plot_rows = [
        row
        for row in plot_rows
        if row["wm15_mean"] is not None or row["wm100_mean"] is not None or row["adaptive_mean"] is not None
    ]
    if not plot_rows:
        raise ValueError("Nessun dato numerico disponibile per il grafico richiesto.")

    load_plot_dependencies()
    x = list(range(len(plot_rows)))
    width = 0.25
    wm_values = [plot_value(row["wm15_mean"]) for row in plot_rows]
    wm100_values = [plot_value(row["wm100_mean"]) for row in plot_rows]
    ad_values = [plot_value(row["adaptive_mean"]) for row in plot_rows]
    labels = [str(row["metric"]) for row in plot_rows]

    fig, ax = plt.subplots(figsize=(max(10, len(plot_rows) * 0.85), 6))
    ax.bar([i - width for i in x], wm_values, width, label="WM15")
    ax.bar(x, wm100_values, width, label="WM100")
    ax.bar([i + width for i in x], ad_values, width, label="ADAPTIVE")
    ax.set_title(title)
    ax.set_ylabel("Media")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.legend()
    fig.tight_layout()

    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / args.output
    fig.savefig(out_path, dpi=160)
    print(f"Creato: {out_path}")


if __name__ == "__main__":
    main()
