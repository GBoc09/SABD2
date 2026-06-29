#!/usr/bin/env python3
"""
Grafici di throughput per le strategie di watermarking.

Input default:
  - performance/throughput_*.csv

Output default:
  - performance/plot/throughput/instant_throughput_comparison.png
  - performance/plot/throughput/average_throughput_comparison.png
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = PROJECT_ROOT / "performance"
DEFAULT_OUTPUT_DIR = DEFAULT_INPUT_DIR / "plot" / "throughput"
REQUIRED_COLUMNS = {
    "label",
    "window_start_ms",
    "window_end_ms",
    "window_events",
}
STRATEGY_COL = "watermark_strategy"
LABEL_COL = "query_label"
COLORS = {
    "ADAPTIVE": "tab:green",
    "WM15": "tab:blue",
    "WM100": "tab:orange",
}

pd = None
plt = None


def load_plot_dependencies() -> None:
    global pd, plt
    if pd is not None and plt is not None:
        return
    try:
        os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as pyplot
        import pandas as pandas_module
    except ModuleNotFoundError as exc:
        missing = exc.name or "dipendenza Python"
        raise SystemExit(f"Dipendenza mancante: installa {missing} per usare questo script.") from exc

    pd = pandas_module
    plt = pyplot


def discover_files(input_dir: Path, pattern: str) -> list[Path]:
    files = sorted(input_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"Nessun CSV trovato con pattern {input_dir / pattern}")
    return files


def strategy_from_file(path: Path) -> str:
    name = path.stem
    prefix = "throughput_"
    if name.startswith(prefix):
        remainder = name[len(prefix) :]
        if "_p" in remainder:
            return remainder.rsplit("_p", 1)[0]
        return remainder
    return name


def normalize_label(raw_label: object, strategy: object) -> str:
    label = str(raw_label)
    suffix = f"-{strategy}"
    if label.endswith(suffix):
        return label[: -len(suffix)]
    return label


def filename_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    return slug or "throughput"


def read_throughput_files(files: list[Path], min_window_events: int) -> "pd.DataFrame":
    frames = []
    for path in files:
        df = pd.read_csv(path)
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"{path.name}: colonne mancanti: {sorted(missing)}")

        df = df.copy()
        if STRATEGY_COL not in df.columns:
            df[STRATEGY_COL] = strategy_from_file(path)

        numeric_columns = (REQUIRED_COLUMNS - {"label"}) | {
            "source_subtask_index",
            "parallelism",
        }
        for col in numeric_columns & set(df.columns):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["window_start_ms", "window_end_ms", "window_events"])
        df = df[df["window_events"] > min_window_events]
        if not df.empty:
            df[LABEL_COL] = [
                normalize_label(label, strategy)
                for label, strategy in zip(df["label"], df[STRATEGY_COL])
            ]
            frames.append(df)

    if not frames:
        raise ValueError("Nessun dato di throughput disponibile dopo il filtro richiesto.")
    return pd.concat(frames, ignore_index=True)


def aggregate_by_strategy_time(
    df: "pd.DataFrame",
    time_bucket_seconds: int,
    smooth_buckets: int,
    group_columns: list[str] | None = None,
) -> "pd.DataFrame":
    if time_bucket_seconds <= 0:
        raise ValueError("--time-bucket-seconds deve essere maggiore di 0.")

    if group_columns is None:
        group_columns = [STRATEGY_COL]

    if "source_subtask_index" in df.columns:
        index_cols = group_columns + ["source_subtask_index"]
    else:
        index_cols = group_columns

    df = df.sort_values(index_cols + ["window_end_ms"]).copy()
    df["time_s"] = df.groupby(group_columns)["window_end_ms"].transform(
        lambda s: (s - s.min()) / 1000.0
    )
    df["time_bin_s"] = (df["time_s"] // time_bucket_seconds) * time_bucket_seconds

    grouped = (
        df.groupby(group_columns + ["time_bin_s"], as_index=False)
        .agg(
            window_events=("window_events", "sum"),
            bucket_start_ms=("window_start_ms", "min"),
            bucket_end_ms=("window_end_ms", "max"),
        )
        .sort_values(group_columns + ["time_bin_s"])
    )
    grouped["time_s"] = grouped["time_bin_s"] + time_bucket_seconds / 2
    grouped["duration_s"] = (grouped["bucket_end_ms"] - grouped["bucket_start_ms"]) / 1000.0
    grouped = grouped[grouped["duration_s"] > 0].copy()
    if grouped.empty:
        raise ValueError("Nessun bucket di throughput con durata positiva.")

    # Calcola i rate dai conteggi grezzi. Sommare colonne gia espresse in eventi/s
    # gonfia il throughput quando nello stesso bucket cadono piu report/subtask.
    grouped["instant_throughput_events_per_second"] = (
        grouped["window_events"] / grouped["duration_s"]
    )
    grouped["first_bucket_start_ms"] = grouped.groupby(group_columns)["bucket_start_ms"].transform(
        "min"
    )
    grouped["cumulative_events"] = grouped.groupby(group_columns)["window_events"].cumsum()
    grouped["elapsed_s"] = (
        grouped["bucket_end_ms"] - grouped["first_bucket_start_ms"]
    ) / 1000.0
    grouped = grouped[grouped["elapsed_s"] > 0].copy()
    grouped["average_throughput_events_per_second"] = (
        grouped["cumulative_events"] / grouped["elapsed_s"]
    )

    if smooth_buckets > 1:
        for metric in (
            "instant_throughput_events_per_second",
            "average_throughput_events_per_second",
        ):
            grouped[f"plot_{metric}"] = grouped.groupby(group_columns)[metric].transform(
                lambda s: s.rolling(window=smooth_buckets, min_periods=1, center=True).mean()
            )
    else:
        grouped["plot_instant_throughput_events_per_second"] = grouped[
            "instant_throughput_events_per_second"
        ]
        grouped["plot_average_throughput_events_per_second"] = grouped[
            "average_throughput_events_per_second"
        ]

    return grouped


def plot_metric(df: "pd.DataFrame", metric: str, title: str, ylabel: str, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    for strategy, group in df.groupby(STRATEGY_COL):
        group = group.sort_values("time_s")
        ax.plot(
            group["time_s"],
            group[f"plot_{metric}"],
            label=str(strategy),
            color=COLORS.get(str(strategy)),
            linewidth=2,
        )

    ax.set_title(title)
    ax.set_xlabel("Tempo relativo (s)")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_metric_by_label(
    df: "pd.DataFrame",
    metric: str,
    title_prefix: str,
    ylabel: str,
    output_dir: Path,
    filename_prefix: str,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written_paths = []

    for label, group in df.groupby(LABEL_COL):
        output_path = output_dir / f"{filename_prefix}_{filename_slug(str(label))}.png"
        plot_metric(
            group,
            metric,
            f"{title_prefix} - {label}",
            ylabel,
            output_path,
        )
        written_paths.append(output_path)

    return written_paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Crea grafici di throughput dalle metriche performance esportate.")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, type=Path, help="Cartella contenente throughput_*.csv.")
    parser.add_argument("--pattern", default="throughput_*.csv", help="Pattern dei CSV di throughput.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, type=Path, help="Cartella dei PNG prodotti.")
    parser.add_argument(
        "--min-window-events",
        default=0,
        type=int,
        help="Esclude finestre con window_events <= valore indicato. Default: 0, usa tutti i punti validi.",
    )
    parser.add_argument(
        "--time-bucket-seconds",
        default=10,
        type=int,
        help="Ampiezza dei bucket temporali usati per aggregare i punti. Default: 10.",
    )
    parser.add_argument(
        "--smooth-buckets",
        default=3,
        type=int,
        help="Numero di bucket su cui applicare la media mobile al plot. Usa 1 per disattivarla.",
    )
    args = parser.parse_args()

    load_plot_dependencies()
    files = discover_files(args.input_dir, args.pattern)
    raw_df = read_throughput_files(files, args.min_window_events)
    overview_df = aggregate_by_strategy_time(
        raw_df,
        args.time_bucket_seconds,
        args.smooth_buckets,
    )
    by_label_df = aggregate_by_strategy_time(
        raw_df,
        args.time_bucket_seconds,
        args.smooth_buckets,
        group_columns=[LABEL_COL, STRATEGY_COL],
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    plot_metric(
        overview_df,
        "instant_throughput_events_per_second",
        "Instant Throughput",
        "Eventi / secondo",
        args.output_dir / "instant_throughput_comparison.png",
    )
    plot_metric(
        overview_df,
        "average_throughput_events_per_second",
        "Average Throughput",
        "Eventi / secondo",
        args.output_dir / "average_throughput_comparison.png",
    )
    instant_label_paths = plot_metric_by_label(
        by_label_df,
        "instant_throughput_events_per_second",
        "Instant Throughput",
        "Eventi / secondo",
        args.output_dir / "by_label" / "instant",
        "instant_throughput",
    )
    average_label_paths = plot_metric_by_label(
        by_label_df,
        "average_throughput_events_per_second",
        "Average Throughput",
        "Eventi / secondo",
        args.output_dir / "by_label" / "average",
        "average_throughput",
    )

    print(f"Grafici globali salvati in: {args.output_dir}")
    print(
        "Grafici per label salvati in: "
        f"{args.output_dir / 'by_label'} ({len(instant_label_paths) + len(average_label_paths)} file)"
    )


if __name__ == "__main__":
    main()
