package it.uniroma2.sabd.flink.engineering.watermarks;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdaptiveWatermarkGenerator implements WatermarkGenerator<FlightEvent> {

    private TDigest digest = TDigest.createDigest(100);
    
    private long maxEventTime = Long.MIN_VALUE;
    private long lastEmittedWatermark = Long.MIN_VALUE;
    
    private int sampleCount = 0;
    private static final int MAX_SAMPLES = 5000;
    private static final int    MIN_SAMPLES       = 100;
    private static final long   FALLBACK_BOUND_MS = 2 * 60 * 1000L; // 2 minuti
    private static final double PERCENTILE        = 0.95;

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveWatermarkGenerator.class);

    @Override
    public void onEvent(FlightEvent event, long eventTimestamp, WatermarkOutput output) {
        if (maxEventTime != Long.MIN_VALUE && eventTimestamp < maxEventTime) {
            long lateness = maxEventTime - eventTimestamp;
        
            digest.add(lateness);
            sampleCount++;
        }

        maxEventTime = Math.max(maxEventTime, eventTimestamp);

        // Strategia di rotazione: se superiamo i 5000 campioni, resettiamo
        // per non portarci dietro dati vecchi
        if (sampleCount > MAX_SAMPLES) {
            digest = TDigest.createDigest(100);
            sampleCount = 0;
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (maxEventTime == Long.MIN_VALUE) {
            return;
        }

        long currentWatermark;

        // Se abbiamo meno di 100 campioni, usiamo il fallback di 2 minuti
        if (digest.size() < MIN_SAMPLES) {
            currentWatermark = maxEventTime - FALLBACK_BOUND_MS;
        } else {
            digest.compress();  // Comprime prima di interrogare il digest: riduce i centroidi e migliora precisione e velocità della query sul quantile.
            long p95 = (long) digest.quantile(PERCENTILE);

            currentWatermark = maxEventTime - p95;
        }
        if (currentWatermark > lastEmittedWatermark) {
            lastEmittedWatermark = currentWatermark;
            output.emitWatermark(new Watermark(lastEmittedWatermark));
        }
        LOG.info("Watermark attuale emesso: {}", currentWatermark);
    }
}