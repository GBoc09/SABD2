package it.uniroma2.sabd.flink.model;

import java.io.Serializable;

public class LatencyMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    private String label;
    private long timestampMs;
    private int sourceSubtaskIndex;
    private long windowStartMs;
    private long windowEndMs;
    private long windowDurationMs;
    private long windowEvents;
    private long totalEvents;
    private long minLatencyMs;
    private long maxLatencyMs;
    private double avgLatencyMs;

    public LatencyMetric() {
    }

    public LatencyMetric(
            String label,
            long timestampMs,
            int sourceSubtaskIndex,
            long windowStartMs,
            long windowEndMs,
            long windowEvents,
            long totalEvents,
            long minLatencyMs,
            long maxLatencyMs,
            double avgLatencyMs) {
        this.label = label;
        this.timestampMs = timestampMs;
        this.sourceSubtaskIndex = sourceSubtaskIndex;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.windowDurationMs = windowEndMs - windowStartMs;
        this.windowEvents = windowEvents;
        this.totalEvents = totalEvents;
        this.minLatencyMs = minLatencyMs;
        this.maxLatencyMs = maxLatencyMs;
        this.avgLatencyMs = avgLatencyMs;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public int getSourceSubtaskIndex() {
        return sourceSubtaskIndex;
    }

    public void setSourceSubtaskIndex(int sourceSubtaskIndex) {
        this.sourceSubtaskIndex = sourceSubtaskIndex;
    }

    public long getWindowStartMs() {
        return windowStartMs;
    }

    public void setWindowStartMs(long windowStartMs) {
        this.windowStartMs = windowStartMs;
    }

    public long getWindowEndMs() {
        return windowEndMs;
    }

    public void setWindowEndMs(long windowEndMs) {
        this.windowEndMs = windowEndMs;
    }

    public long getWindowDurationMs() {
        return windowDurationMs;
    }

    public void setWindowDurationMs(long windowDurationMs) {
        this.windowDurationMs = windowDurationMs;
    }

    public long getWindowEvents() {
        return windowEvents;
    }

    public void setWindowEvents(long windowEvents) {
        this.windowEvents = windowEvents;
    }

    public long getTotalEvents() {
        return totalEvents;
    }

    public void setTotalEvents(long totalEvents) {
        this.totalEvents = totalEvents;
    }

    public long getMinLatencyMs() {
        return minLatencyMs;
    }

    public void setMinLatencyMs(long minLatencyMs) {
        this.minLatencyMs = minLatencyMs;
    }

    public long getMaxLatencyMs() {
        return maxLatencyMs;
    }

    public void setMaxLatencyMs(long maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }

    public double getAvgLatencyMs() {
        return avgLatencyMs;
    }

    public void setAvgLatencyMs(double avgLatencyMs) {
        this.avgLatencyMs = avgLatencyMs;
    }
}
