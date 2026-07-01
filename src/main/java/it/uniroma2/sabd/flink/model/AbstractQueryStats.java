package it.uniroma2.sabd.flink.model;

import it.uniroma2.sabd.model.HasProcessingStartTime;
import java.time.Instant;

public abstract class AbstractQueryStats implements HasProcessingStartTime {

    protected Instant windowStart;
    protected Instant windowEnd;
    protected long processingStartTimeMs;

    protected AbstractQueryStats(Instant windowStart, Instant windowEnd, long processingStartTimeMs) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.processingStartTimeMs = processingStartTimeMs;
    }

    @Override
    public long getProcessingStartTimeMs() { return processingStartTimeMs; }

    @Override
    public void setProcessingStartTimeMs(long processingStartTimeMs) {
        this.processingStartTimeMs = processingStartTimeMs;
    }

    public Instant getWindowStart() { return windowStart; }
    public Instant getWindowEnd() { return windowEnd; }

    public long getWindowStartEpoch() { return windowStart != null ? windowStart.toEpochMilli() : 0L; }
    public long getWindowEndEpoch() { return windowEnd != null ? windowEnd.toEpochMilli() : 0L; }

    public abstract String toCSV();
}
