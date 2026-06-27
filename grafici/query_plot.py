from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# =====================================================
# Configurazione
# =====================================================

CSV_FILE = "/home/giulia/Documenti/SABD/project2/SABD2/output/query1/query1_WM100.csv"
#"/home/giulia/Documenti/SABD/project2/SABD2/output/query1/query1_WM15.csv"

OUTPUT_DIR = Path("/home/giulia/Documenti/SABD/project2/SABD2/output/query1/plot")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "query1_temporal_trends.png"

# =====================================================
# Lettura dati
# =====================================================

df = pd.read_csv(CSV_FILE)

df["window_start"] = pd.to_datetime(df["window_start"])
df["window_end"] = pd.to_datetime(df["window_end"])

# Ordina temporalmente
df = df.sort_values("window_start")

# =====================================================
# Figura
# =====================================================

fig, axs = plt.subplots(
    2,
    2,
    figsize=(16, 10),
    sharex=True
)

# -----------------------------------------------------
# Average Departure Delay
# -----------------------------------------------------

for airline, group in df.groupby("airline"):
    axs[0, 0].plot(
        group["window_start"],
        group["dep_delay_mean"],
        linewidth=2,
        label=airline
    )

axs[0, 0].set_title("Average Departure Delay")
axs[0, 0].set_ylabel("Minutes")
axs[0, 0].grid(True)

# -----------------------------------------------------
# Late Departure Rate
# -----------------------------------------------------

for airline, group in df.groupby("airline"):
    axs[0, 1].plot(
        group["window_start"],
        group["late_departure_rate"],
        linewidth=2
    )

axs[0, 1].set_title("Late Departure Rate")
axs[0, 1].set_ylabel("%")
axs[0, 1].grid(True)

# -----------------------------------------------------
# Cancellation Rate
# -----------------------------------------------------

for airline, group in df.groupby("airline"):
    axs[1, 0].plot(
        group["window_start"],
        group["cancellation_rate"],
        linewidth=2
    )

axs[1, 0].set_title("Cancellation Rate")
axs[1, 0].set_ylabel("%")
axs[1, 0].grid(True)

# -----------------------------------------------------
# Number of Flights
# -----------------------------------------------------

for airline, group in df.groupby("airline"):
    axs[1, 1].plot(
        group["window_start"],
        group["num_flights"],
        linewidth=2
    )

axs[1, 1].set_title("Number of Flights")
axs[1, 1].set_ylabel("Flights")
axs[1, 1].grid(True)

# =====================================================
# Formattazione asse temporale
# =====================================================

locator = mdates.AutoDateLocator()
formatter = mdates.DateFormatter("%d-%m\n%H:%M")

for ax in axs.flat:
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    ax.tick_params(axis="x", rotation=45)

# =====================================================
# Legenda unica
# =====================================================

handles, labels = axs[0, 0].get_legend_handles_labels()

fig.legend(
    handles,
    labels,
    loc="upper center",
    ncol=len(labels),
    frameon=False,
    fontsize=11
)

fig.suptitle(
    "Temporal Evolution of Query 1 Metrics",
    fontsize=16,
    fontweight="bold"
)

fig.supxlabel("Time")
plt.tight_layout(rect=[0, 0, 1, 0.94])

plt.savefig(
    OUTPUT_FILE,
    dpi=300,
    bbox_inches="tight"
)

plt.show()

print(f"Figura salvata in: {OUTPUT_FILE}")