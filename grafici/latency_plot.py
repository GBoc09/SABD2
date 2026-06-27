from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


OUTPUT_DIR = Path("/home/giulia/Documenti/SABD/project2/SABD2/performance/plot/latency")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

files = {
    "Adaptive": "/home/giulia/Documenti/SABD/project2/SABD2/performance/latency_ADAPTIVE_p4.csv",
    "WM15": "/home/giulia/Documenti/SABD/project2/SABD2/performance/latency_WM15_p4.csv",
    "WM100": "/home/giulia/Documenti/SABD/project2/SABD2/performance/latency_WM100_p4.csv",
}

colors = {
    "Adaptive": "tab:green",
    "WM15": "tab:blue",
    "WM100": "tab:orange",
}

# ===============================
# Average latency
# ===============================

plt.figure(figsize=(10,5))

for name, file in files.items():
    df = pd.read_csv(file)

    # elimina la finestra di warm-up
    df = df[df["window_events"] > 100]

    # tempo relativo
    t = (df["window_end_ms"] - df["window_end_ms"].iloc[0]) / 1000

    plt.plot(
        t,
        df["avg_latency_ms"],
        label=name,
        color=colors[name],
        linewidth=2,
    )

plt.title("Average Latency")
plt.xlabel("Tempo (s)")
plt.ylabel("Average latency (ms)")
plt.grid(True)
plt.legend()
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "average_latency_comparison.png",
    dpi=300,
    bbox_inches="tight")

# ===============================
# Maximum latency
# ===============================

plt.figure(figsize=(10,5))

for name, file in files.items():
    df = pd.read_csv(file)

    df = df[df["window_events"] > 100]

    t = (df["window_end_ms"] - df["window_end_ms"].iloc[0]) / 1000

    plt.plot(
        t,
        df["max_latency_ms"],
        label=name,
        color=colors[name],
        linewidth=2,
    )

plt.title("Maximum Latency")
plt.xlabel("Tempo (s)")
plt.ylabel("Maximum latency (ms)")
plt.grid(True)
plt.legend()
plt.tight_layout()

# Grafico Maximum Latency
plt.savefig(
    OUTPUT_DIR / "maximum_latency_comparison.png",
    dpi=300,
    bbox_inches="tight"
)

plt.show()