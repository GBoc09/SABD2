package it.uniroma2.sabd.flink.controller;

import it.uniroma2.sabd.flink.model.ThroughputMetric;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class GlobalThroughputAggregator
        extends ProcessWindowFunction<ThroughputMetric, ThroughputMetric, String, TimeWindow> {

    @Override
    public void process(
            String label,
            Context context,
            Iterable<ThroughputMetric> metrics,
            Collector<ThroughputMetric> out) {

        Map<Integer, ThroughputMetric> latestBySubtask = new HashMap<>();
        for (ThroughputMetric metric : metrics) {
            latestBySubtask.merge(
                    metric.getSourceSubtaskIndex(),
                    metric,
                    (current, candidate) ->
                            candidate.getTimestampMs() >= current.getTimestampMs()
                                    ? candidate
                                    : current);
        }

        if (latestBySubtask.isEmpty()) {
            return;
        }

        long timestampMs = Long.MIN_VALUE;
        long windowStartMs = Long.MAX_VALUE;
        long windowEndMs = Long.MIN_VALUE;
        long windowEvents = 0L;
        long totalEvents = 0L;
        double instantThroughput = 0.0;
        double averageThroughput = 0.0;

        for (ThroughputMetric metric : latestBySubtask.values()) {
            timestampMs = Math.max(timestampMs, metric.getTimestampMs());
            windowStartMs = Math.min(windowStartMs, metric.getWindowStartMs());
            windowEndMs = Math.max(windowEndMs, metric.getWindowEndMs());
            windowEvents += metric.getWindowEvents();
            totalEvents += metric.getTotalEvents();
            instantThroughput += metric.getInstantThroughputEventsPerSecond();
            averageThroughput += metric.getAverageThroughputEventsPerSecond();
        }

        out.collect(new ThroughputMetric(
                label + "-global",
                timestampMs,
                -1,
                windowStartMs,
                windowEndMs,
                windowEvents,
                totalEvents,
                instantThroughput,
                averageThroughput));
    }
}
