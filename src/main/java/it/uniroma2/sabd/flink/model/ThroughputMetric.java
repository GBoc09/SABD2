package it.uniroma2.sabd.flink.model;

import java.io.Serializable;

public class ThroughputMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    private String label;
    private long timestampMs;
    private int sourceSubtaskIndex;
    private long windowStartMs;
    private long windowEndMs;
    private long windowDurationMs;
    private long windowEvents;
    private long totalEvents;
    private double instantThroughputEventsPerSecond;
    private double averageThroughputEventsPerSecond;

    public ThroughputMetric() {
    }

    public ThroughputMetric(
            String label,
            long timestampMs,
            int sourceSubtaskIndex,
            long windowStartMs,
            long windowEndMs,
            long windowEvents,
            long totalEvents,
            double instantThroughputEventsPerSecond,
            double averageThroughputEventsPerSecond) {
        this.label = label;
        this.timestampMs = timestampMs;
        this.sourceSubtaskIndex = sourceSubtaskIndex;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.windowDurationMs = windowEndMs - windowStartMs;
        this.windowEvents = windowEvents;
        this.totalEvents = totalEvents;
        this.instantThroughputEventsPerSecond = instantThroughputEventsPerSecond;
        this.averageThroughputEventsPerSecond = averageThroughputEventsPerSecond;
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

    public double getInstantThroughputEventsPerSecond() {
        return instantThroughputEventsPerSecond;
    }

    public void setInstantThroughputEventsPerSecond(double instantThroughputEventsPerSecond) {
        this.instantThroughputEventsPerSecond = instantThroughputEventsPerSecond;
    }

    public double getAverageThroughputEventsPerSecond() {
        return averageThroughputEventsPerSecond;
    }

    public void setAverageThroughputEventsPerSecond(double averageThroughputEventsPerSecond) {
        this.averageThroughputEventsPerSecond = averageThroughputEventsPerSecond;
    }
}
