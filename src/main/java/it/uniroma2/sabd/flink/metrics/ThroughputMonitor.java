package it.uniroma2.sabd.flink.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Operatore passthrough che misura il throughput della pipeline.
 * Non modifica gli eventi — li conta e li riemette invariati.

 * Uso: .process(new ThroughputMonitor<>("label", config.getMetricsThroughputReportIntervalMs()))
 */
public class ThroughputMonitor<T> extends ProcessFunction<T, T> {

    private final String label;
    private final long reportIntervalMs;

    private transient long totalEvents;
    private transient long windowEvents;
    private transient long startMs;
    private transient long windowStartMs;

    public ThroughputMonitor(String label, long reportIntervalMs) {
        this.label = label;
        this.reportIntervalMs = reportIntervalMs;
    }

    @Override
    public void open(Configuration parameters) {
        totalEvents   = 0;
        windowEvents  = 0;
        startMs       = System.currentTimeMillis();
        windowStartMs = startMs;
    }

    @Override
    public void processElement(T event, Context ctx, Collector<T> out) {
        totalEvents++;
        windowEvents++;

        long now = System.currentTimeMillis();
        long windowElapsed = now - windowStartMs;

        if (windowElapsed >= reportIntervalMs) {
            double instantThroughput = (windowEvents * 1000.0) / windowElapsed;
            double avgThroughput     = (totalEvents  * 1000.0) / (now - startMs);

            System.out.printf(
                    "%n[THROUGHPUT] %s%n" +
                            "  Istantaneo : %.1f eventi/sec (ultimi %.1f sec)%n" +
                            "  Medio      : %.1f eventi/sec (totale %d eventi)%n",
                    label,
                    instantThroughput, windowElapsed / 1000.0,
                    avgThroughput, totalEvents
            );

            windowEvents  = 0;
            windowStartMs = now;
        }

        out.collect(event);
    }
}
