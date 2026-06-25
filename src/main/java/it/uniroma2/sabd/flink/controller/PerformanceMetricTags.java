package it.uniroma2.sabd.flink.controller;

import it.uniroma2.sabd.flink.model.LatencyMetric;
import it.uniroma2.sabd.flink.model.ThroughputMetric;
import org.apache.flink.util.OutputTag;

public final class PerformanceMetricTags {

    public static final OutputTag<LatencyMetric> LATENCY =
            new OutputTag<LatencyMetric>("performance-latency") { };

    public static final OutputTag<ThroughputMetric> THROUGHPUT =
            new OutputTag<ThroughputMetric>("performance-throughput") { };

    private PerformanceMetricTags() {
    }
}
