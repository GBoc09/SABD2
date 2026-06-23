package it.uniroma2.sabd.flink.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Operatore passthrough che misura la latenza di processamento.
 * Confronta il processing time (orologio reale Flink) con un campo
 * producedAt (ms epoch) che il producer Kafka ha scritto nell'evento.
 */
public class LatencyMonitor<T extends HasProducedAt> extends ProcessFunction<T, T> {

    private final String label;
    private final long reportEveryMs;

    private transient long totalEvents;
    private transient long totalLatencyMs;
    private transient long minLatencyMs;
    private transient long maxLatencyMs;
    private transient long windowStartMs;

    public LatencyMonitor(String label) {
        this(label, 5_000);
    }

    public LatencyMonitor(String label, long reportEveryMs) {
        this.label = label;
        this.reportEveryMs = reportEveryMs;
    }

    @Override
    public void open(Configuration parameters) {
        totalEvents   = 0;
        totalLatencyMs = 0;
        minLatencyMs  = Long.MAX_VALUE;
        maxLatencyMs  = Long.MIN_VALUE;
        windowStartMs = System.currentTimeMillis();
    }

    @Override
    public void processElement(T event, Context ctx, Collector<T> out) {
        long processingTime = ctx.timerService().currentProcessingTime();
        long producedAt     = event.getProducedAt();
        long latencyMs      = processingTime - producedAt;

        totalEvents++;
        totalLatencyMs += latencyMs;
        if (latencyMs < minLatencyMs) minLatencyMs = latencyMs;
        if (latencyMs > maxLatencyMs) maxLatencyMs = latencyMs;

        long now = System.currentTimeMillis();
        if (now - windowStartMs >= reportEveryMs) {
            System.out.printf(
                    "%n[LATENCY] %s%n" +
                            "  Min         : %d ms%n" +
                            "  Max         : %d ms%n" +
                            "  Media       : %.2f ms%n" +
                            "  Su          : %d eventi%n",
                    label,
                    minLatencyMs,
                    maxLatencyMs,
                    totalEvents > 0 ? (double) totalLatencyMs / totalEvents : 0,
                    totalEvents
            );
            windowStartMs = now;
        }

        out.collect(event);
    }
}