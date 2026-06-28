package it.uniroma2.sabd.flink.controller;

import it.uniroma2.sabd.flink.model.LatencyMetric;
import it.uniroma2.sabd.model.HasProcessingStartTime;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operatore passthrough che misura la latenza di processamento.
 * Confronta il processing time corrente con il timestamp di ingresso in
 * Flink della tupla piu recente che ha contribuito al risultato.
 */
public class LatencyMonitor<T extends HasProcessingStartTime> extends ProcessFunction<T, T> {

    private final String label;
    private final long reportEveryMs;
    private final OutputTag<LatencyMetric> metricOutputTag;

    private transient long totalEvents;
    private transient long skippedEvents;
    private transient long windowEvents;
    private transient long windowLatencyMs;
    private transient long windowMinLatencyMs;
    private transient long windowMaxLatencyMs;
    private transient long windowStartMs;
    private transient int subtaskIndex;

    private static final Logger LOG =
            LoggerFactory.getLogger(LatencyMonitor.class);

    public LatencyMonitor(String label) {
        this(label, 5_000);
    }

    public LatencyMonitor(String label, long reportEveryMs) {
        this(label, reportEveryMs, PerformanceMetricTags.LATENCY);
    }

    public LatencyMonitor(
            String label,
            long reportEveryMs,
            OutputTag<LatencyMetric> metricOutputTag) {
        this.label = label;
        this.reportEveryMs = reportEveryMs;
        this.metricOutputTag = metricOutputTag;
    }

    @Override
    public void open(Configuration parameters) {
        totalEvents = 0;
        skippedEvents = 0;
        windowEvents = 0;
        windowLatencyMs = 0;
        windowMinLatencyMs = Long.MAX_VALUE;
        windowMaxLatencyMs = Long.MIN_VALUE;
        windowStartMs = System.currentTimeMillis();
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(T event, Context ctx, Collector<T> out) {
        long processingTime = ctx.timerService().currentProcessingTime();
        long processingStartTime = event.getProcessingStartTimeMs();

        if (processingStartTime <= 0) {
            skippedEvents++;
            if (skippedEvents == 1) {
                LOG.warn(
                        "LATENCY_MONITOR label={} subtask={} skipped event without processingStartTimeMs",
                        label,
                        subtaskIndex);
            }
            out.collect(event);
            return;
        }

        long latencyMs = processingTime - processingStartTime;

        totalEvents++;
        windowEvents++;
        windowLatencyMs += latencyMs;
        if (latencyMs < windowMinLatencyMs) windowMinLatencyMs = latencyMs;
        if (latencyMs > windowMaxLatencyMs) windowMaxLatencyMs = latencyMs;

        long now = processingTime;
        if (now - windowStartMs >= reportEveryMs) {
            double avgLatency =
                    windowEvents > 0 ? (double) windowLatencyMs / windowEvents : 0;

            ctx.output(
                    metricOutputTag,
                    new LatencyMetric(
                            label,
                            now,
                            subtaskIndex,
                            windowStartMs,
                            now,
                            windowEvents,
                            totalEvents,
                            windowMinLatencyMs,
                            windowMaxLatencyMs,
                            avgLatency));

            LOG.info(
                    "LATENCY_MONITOR | label={} | subtask={} | min={}ms | max={}ms | avg={}ms | windowEvents={} | totalEvents={}",
                    label,
                    subtaskIndex,
                    windowMinLatencyMs,
                    windowMaxLatencyMs,
                    String.format("%.2f", avgLatency),
                    windowEvents,
                    totalEvents);

            windowStartMs = now;
            windowEvents = 0;
            windowLatencyMs = 0;
            windowMinLatencyMs = Long.MAX_VALUE;
            windowMaxLatencyMs = Long.MIN_VALUE;
        }

        out.collect(event);
    }
}
