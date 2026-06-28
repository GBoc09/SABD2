from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

# =====================================================
# CONFIGURAZIONE
# =====================================================

CSV_FILE = "/home/giulia/Documenti/SABD/project2/SABD2/output/query3/query3_global_WM100.csv"

OUTPUT_DIR = Path(
    "/home/giulia/Documenti/SABD/project2/SABD2/output/query3/plot/WM100/global"
)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


df = pd.read_csv(CSV_FILE)

# elimina eventuali duplicati
df = df.drop_duplicates()

# timestamp
df["ts"] = pd.to_datetime(df["ts"])

# ordina
df = df.sort_values(["airline", "hour"])


for airline in sorted(df["airline"].unique()):

    airline_df = df[df["airline"] == airline]

    stats = []

    p90_x = []
    p90_y = []

    for hour in range(24):

        row = airline_df[airline_df["hour"] == hour]

        if row.empty:
            continue

        row = row.iloc[0]

        stats.append(
            {
                "label": str(hour),
                "whislo": row["min"],
                "q1": row["p25"],
                "med": row["p50"],
                "q3": row["p75"],
                "whishi": row["max"],
                "fliers": [],
            }
        )

        p90_x.append(len(stats))
        p90_y.append(row["p90"])


    fig, ax = plt.subplots(figsize=(14, 6))

    ax.bxp(
        stats,
        showfliers=False,
        widths=0.6,
    )

    # P90
    ax.scatter(
        p90_x,
        p90_y,
        marker="D",
        s=40,
        label="P90",
        zorder=3,
    )

    ax.set_title(
        f"Departure Delay Distribution - {airline}",
        fontsize=15,
        fontweight="bold",
    )

    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Departure Delay (minutes)")

    ax.grid(True, axis="y", alpha=0.3)

    ax.legend()

    plt.tight_layout()

    plt.savefig(
        OUTPUT_DIR / f"{airline}_boxplot.png",
        dpi=300,
        bbox_inches="tight",
    )

    plt.close()

print("Grafici salvati in:", OUTPUT_DIR)