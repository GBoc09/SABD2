from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

OUTPUT_DIR = Path("/home/giulia/Documenti/SABD/project2/SABD2/performance/plot/throughput")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

files = {
    "Adaptive": "/home/giulia/Documenti/SABD/project2/SABD2/performance/throughput_ADAPTIVE_p4.csv",
    "WM15": "/home/giulia/Documenti/SABD/project2/SABD2/performance/throughput_WM15_p4.csv",
    "WM100": "/home/giulia/Documenti/SABD/project2/SABD2/performance/throughput_WM100_p4.csv",
}

colors = {
    "Adaptive": "tab:green",
    "WM15": "tab:blue",
    "WM100": "tab:orange",
}

# =====================================================
# Instant Throughput
# =====================================================

plt.figure(figsize=(10,5))

for name, file in files.items():
    df = pd.read_csv(file)

    # elimina il warm-up
    df = df[df["window_events"] > 100]

    # tempo relativo
    t = (df["window_end_ms"] - df["window_end_ms"].iloc[0]) / 1000

    plt.plot(
        t,
        df["instant_throughput_events_per_second"],
        label=name,
        color=colors[name],
        linewidth=2,
    )

plt.title("Instant Throughput")
plt.xlabel("Tempo (s)")
plt.ylabel("Eventi / secondo")
plt.grid(True)
plt.legend()
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "instant_throughput_comparison.png",
    dpi=300,
    bbox_inches="tight"
)

# =====================================================
# Average Throughput
# =====================================================

plt.figure(figsize=(10,5))

for name, file in files.items():
    df = pd.read_csv(file)

    df = df[df["window_events"] > 100]

    t = (df["window_end_ms"] - df["window_end_ms"].iloc[0]) / 1000

    plt.plot(
        t,
        df["average_throughput_events_per_second"],
        label=name,
        color=colors[name],
        linewidth=2,
    )

plt.title("Average Throughput")
plt.xlabel("Tempo (s)")
plt.ylabel("Eventi / secondo")
plt.grid(True)
plt.legend()
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "average_throughput_comparison.png",
    dpi=300,
    bbox_inches="tight"
)

plt.show()