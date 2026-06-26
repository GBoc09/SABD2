package it.uniroma2.sabd.flink.engineering.watermarks;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.*;
import java.util.*;

public class AdaptiveWatermarkGenerator implements WatermarkGenerator<FlightEvent> {

    // ArrayDeque per rimozioni O(1) in testa
    private final Queue<Long> latenessSamples = new ArrayDeque<>();
    private long maxEventTime = Long.MIN_VALUE;
    private long lastEmittedWatermark = Long.MIN_VALUE; // Per garantire la monotonicità

    @Override
    public void onEvent(FlightEvent event, long eventTimestamp, WatermarkOutput output) {
        if (maxEventTime != Long.MIN_VALUE && eventTimestamp < maxEventTime) {
            long lateness = maxEventTime - eventTimestamp;
            latenessSamples.add(lateness);
        }

        maxEventTime = Math.max(maxEventTime, eventTimestamp);

        // Rimozione efficiente O(1)
        if (latenessSamples.size() > 5000) {
            latenessSamples.poll(); 
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (maxEventTime == Long.MIN_VALUE) {
            return;
        }

        long currentWatermark;

        if (latenessSamples.size() < 100) {
            // Fallback iniziale (30 minuti)
            currentWatermark = maxEventTime - 30 * 60 * 1000;
        } else {
            // Nota: L'ordinamento qui è ancora presente per semplicità, 
            // ma l'uso di ArrayDeque alleggerisce la onEvent.
            List<Long> sorted = new ArrayList<>(latenessSamples);
            Collections.sort(sorted);

            int index = (int) Math.ceil(0.95 * sorted.size()) - 1;
            index = Math.max(index, 0);
            long p95 = sorted.get(index);

            currentWatermark = maxEventTime - p95;
        }

        // GARANZIA DI MONOTONICITÀ: Il watermark può solo crescere
        if (currentWatermark > lastEmittedWatermark) {
            lastEmittedWatermark = currentWatermark;
            output.emitWatermark(new Watermark(lastEmittedWatermark));
        }
    }
}