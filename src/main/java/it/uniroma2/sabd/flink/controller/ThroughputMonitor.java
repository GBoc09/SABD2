package it.uniroma2.sabd.flink.controller;

import it.uniroma2.sabd.flink.model.ThroughputMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operatore passthrough che misura il throughput della pipeline.
 * Non modifica gli eventi — li conta e li riemette invariati.
 */
public class ThroughputMonitor<T> extends ProcessFunction<T, T> {

    private final String label;
    private final long reportIntervalMs;
    private final OutputTag<ThroughputMetric> metricOutputTag;

    private transient long totalEvents;
    private transient long windowEvents;
    private transient long startMs;
    private transient long windowStartMs;
    private transient int subtaskIndex;

    private static final Logger LOG =
            LoggerFactory.getLogger(ThroughputMonitor.class);

    public ThroughputMonitor(String label, long reportIntervalMs) {
        this(label, reportIntervalMs, PerformanceMetricTags.THROUGHPUT);
    }

    public ThroughputMonitor(
            String label,
            long reportIntervalMs,
            OutputTag<ThroughputMetric> metricOutputTag) {
        this.label = label;
        this.reportIntervalMs = reportIntervalMs;
        this.metricOutputTag = metricOutputTag;
    }

    @Override
    public void open(Configuration parameters) {
        totalEvents   = 0;
        windowEvents  = 0;
        startMs       = System.currentTimeMillis();
        windowStartMs = startMs;
        subtaskIndex  = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(T event, Context ctx, Collector<T> out) {
        totalEvents++;
        windowEvents++;

        long now = ctx.timerService().currentProcessingTime();
        long windowElapsed = now - windowStartMs;

        if (windowElapsed >= reportIntervalMs) {
            long safeWindowElapsed = Math.max(1, windowElapsed);
            long safeTotalElapsed = Math.max(1, now - startMs);
            double instantThroughput = (windowEvents * 1000.0) / safeWindowElapsed;
            double avgThroughput     = (totalEvents  * 1000.0) / safeTotalElapsed;

            ctx.output(
                    metricOutputTag,
                    new ThroughputMetric(
                            label,
                            now,
                            subtaskIndex,
                            windowStartMs,
                            now,
                            windowEvents,
                            totalEvents,
                            instantThroughput,
                            avgThroughput));

            LOG.info(
                    "THROUGHPUT_MONITOR label={} subtask={} instant={} ev/sec windowSec={} avg={} ev/sec totalEvents={}",
                    label,
                    subtaskIndex,
                    String.format("%.1f", instantThroughput),
                    String.format("%.1f", windowElapsed / 1000.0),
                    String.format("%.1f", avgThroughput),
                    totalEvents
            );

            windowEvents  = 0;
            windowStartMs = now;
        }

        out.collect(event);
    }
}
