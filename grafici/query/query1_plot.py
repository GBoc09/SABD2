from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


CSV_FILE = "/home/giulia/Documenti/SABD/project2/SABD2/output/query1/query1_WM100.csv"
#"/home/giulia/Documenti/SABD/project2/SABD2/output/query1/query1_WM15.csv"

OUTPUT_DIR = Path("/home/giulia/Documenti/SABD/project2/SABD2/output/query1/plot/WM100")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

colors = {
    "AA": "tab:blue",
    "DL": "tab:orange",
    "UA": "tab:green",
}
metrics = {
    "dep_delay_mean": (
        "Average Departure Delay",
        "Minutes",
        "departure_delay_by_hour.png",
    ),
    "cancellation_rate": (
        "Cancellation Rate",
        "%",
        "cancellation_rate_by_hour.png",
    ),
    "late_departure_rate": (
        "Late Departure Rate",
        "%",
        "late_departure_rate_by_hour.png",
    ),
}

df = pd.read_csv(CSV_FILE)

df = df.drop_duplicates()

df["window_start"] = pd.to_datetime(df["window_start"])

df["date"] = df["window_start"].dt.date
df["hour"] = df["window_start"].dt.hour

df = df.sort_values("window_start")


for metric, (title, ylabel, filename) in metrics.items():

    fig, axs = plt.subplots(
        6,
        4,
        figsize=(18, 20),
        sharex=False,
        sharey=False,
    )

    axs = axs.flatten()

    for hour in range(24):

        ax = axs[hour]

        df_hour = df[df["hour"] == hour]

        if df_hour.empty:
            ax.set_visible(False)
            continue

        for airline, group in df_hour.groupby("airline"):

            group = group.sort_values("date")

            ax.plot(
                group["date"],
                group[metric],
                marker="o",
                linewidth=2,
                markersize=4,
                label=airline,
                color=colors.get(airline, None),
            )

        ax.set_title(f"{hour:02d}:00 - {hour:02d}:59", fontsize=10)

        ax.grid(True, alpha=0.3)

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        ax.tick_params(axis="y", labelsize=8)

    for i in range(24, len(axs)):
        axs[i].set_visible(False)

    handles, labels = axs[0].get_legend_handles_labels()

    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=len(labels),
        frameon=False,
        fontsize=12,
    )

    fig.suptitle(
        title,
        fontsize=18,
        fontweight="bold",
    )

    fig.supxlabel("Day")
    fig.supylabel(ylabel)

    plt.tight_layout(rect=[0.03, 0.03, 1, 0.95])

    plt.savefig(
        OUTPUT_DIR / filename,
        dpi=300,
        bbox_inches="tight",
    )

    plt.close(fig)

print("Grafici salvati in:", OUTPUT_DIR)