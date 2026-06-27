package it.uniroma2.sabd.flink.engineering.watermarks;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdaptiveWatermarkGenerator implements WatermarkGenerator<FlightEvent> {

    // Il T-Digest. Il parametro "compression" (es. 100) bilancia precisione e memoria.
    // 100 è il valore standard: ottima precisione sui percentili estremi (P95, P99).
    private TDigest digest = TDigest.createDigest(100);
    
    private long maxEventTime = Long.MIN_VALUE;
    private long lastEmittedWatermark = Long.MIN_VALUE;
    
    // Contatore per svuotare il digest ed evitare che memorizzi dati vecchi di ore
    private int sampleCount = 0;
    private static final int MAX_SAMPLES = 5000;

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveWatermarkGenerator.class);

    @Override
    public void onEvent(FlightEvent event, long eventTimestamp, WatermarkOutput output) {
        if (maxEventTime != Long.MIN_VALUE && eventTimestamp < maxEventTime) {
            long lateness = maxEventTime - eventTimestamp;
            
            // Inserimento nel T-Digest: Operazione O(1) velocissima
            digest.add(lateness);
            sampleCount++;
        }

        maxEventTime = Math.max(maxEventTime, eventTimestamp);

        // Strategia di rotazione: se superiamo i 5000 campioni, resettiamo
        // per non portarci dietro la latenza di un'ora fa.
        if (sampleCount > MAX_SAMPLES) {
            TDigest oldDigest = digest;
            digest = TDigest.createDigest(100);
            // Opzionale: puoi fondere una parte del vecchio nel nuovo per non partire da zero
            digest.add(oldDigest); 
            sampleCount = (int)digest.size(); // aggiorna il contatore reale dei nodi rimasti
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (maxEventTime == Long.MIN_VALUE) {
            return;
        }

        long currentWatermark;

        // Se abbiamo meno di 100 campioni, usiamo il fallback di 30 minuti
        if (digest.size() < 100) {
            currentWatermark = maxEventTime - 30 * 60 * 1000;
        } else {
            // QUERY ISTANTANEA O(1): Chiediamo il 95° percentile (0.95)
            // Non c'è nessuna copia di liste e nessun Collections.sort()!
            long p95 = (long) digest.quantile(0.95);

            currentWatermark = maxEventTime - p95;
        }

        // Garanzia di monotonicità obbligatoria per Flink
        if (currentWatermark > lastEmittedWatermark) {
            lastEmittedWatermark = currentWatermark;
            output.emitWatermark(new Watermark(lastEmittedWatermark));
        }
        LOG.info("Watermark attuale emesso: {}", currentWatermark);
    }
}