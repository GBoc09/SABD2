#!/usr/bin/env python3
"""
Grafico Query 3 - bar chart delle medie di min, max e percentili.

Input:
  - query3_watermark_mean_comparison.csv, prodotto da compare_query3_delay_stats_means.py

Output:
  - query3_watermark_delay_stats_histogram.png
"""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "output" / "query3" / "query3_watermark_mean_comparison.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output" / "query3" / "plot"
REQUIRED_COLUMNS = {"window_type", "scope", "airline", "hour", "metric", "wm15_mean", "wm100_mean", "adaptive_mean"}
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
            f"File non trovato: {path}. Esegui prima grafici/compare_query3_delay_stats_means.py."
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
                    "window_type": row.get("window_type", ""),
                    "scope": row.get("scope", ""),
                    "airline": row.get("airline", ""),
                    "hour": row.get("hour", ""),
                    "metric": row.get("metric", ""),
                    "wm15_mean": parse_number(row.get("wm15_mean")),
                    "wm100_mean": parse_number(row.get("wm100_mean")),
                    "adaptive_mean": parse_number(row.get("adaptive_mean")),
                }
            )
        return rows


def label_for(row: dict[str, object], scope: str) -> str:
    if scope == "ALL":
        return f"{row['window_type']} / {row['metric']}"
    if scope == "airline":
        return f"{row['window_type']} / {row['airline']} / {row['metric']}"
    return f"{row['window_type']} / {row['airline']} h{row['hour']} / {row['metric']}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Crea il grafico delle medie Query 3 per strategia.")
    parser.add_argument(
        "--input",
        default=None,
        help="CSV di confronto Q3. Default: output/query3/query3_watermark_mean_comparison.csv.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Cartella in cui salvare il grafico. Default: output/query3/plot.",
    )
    parser.add_argument("--output", default="query3_watermark_delay_stats_histogram.png", help="Nome del PNG prodotto.")
    parser.add_argument(
        "--window-type",
        default="global",
        choices=["1day", "7day", "global", "ALL"],
        help="Tipo finestra da graficare. ALL mostra tutte le finestre.",
    )
    parser.add_argument(
        "--scope",
        default="ALL",
        choices=["ALL", "airline", "airline_hour"],
        help="Scope del confronto da graficare. Default: ALL.",
    )
    args = parser.parse_args()

    rows = read_comparison(resolve_input(args.input))
    plot_rows = [row for row in rows if row["scope"] == args.scope]
    if args.window_type != "ALL":
        plot_rows = [row for row in plot_rows if row["window_type"] == args.window_type]

    for row in plot_rows:
        row["label"] = label_for(row, args.scope)

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
    labels = [str(row["label"]) for row in plot_rows]

    fig, ax = plt.subplots(figsize=(max(10, len(plot_rows) * 0.75), 6))
    ax.bar([i - width for i in x], wm_values, width, label="WM15")
    ax.bar(x, wm100_values, width, label="WM100")
    ax.bar([i + width for i in x], ad_values, width, label="ADAPTIVE")
    ax.set_title("Query 3 - medie di min, max e percentili per strategia di watermarking")
    ax.set_ylabel("Media ritardo DEP_DELAY")
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
