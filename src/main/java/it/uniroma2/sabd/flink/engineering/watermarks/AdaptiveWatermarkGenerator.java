package it.uniroma2.sabd.flink.engineering.watermarks;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AdaptiveWatermarkGenerator
        implements WatermarkGenerator<FlightEvent> {

    private final List<Long> latenessSamples = new ArrayList<>();
    private long maxEventTime = Long.MIN_VALUE;

    @Override
public void onEvent(
        FlightEvent event,
        long eventTimestamp,
        WatermarkOutput output) {

    if (maxEventTime != Long.MIN_VALUE &&
        eventTimestamp < maxEventTime) {

        long lateness = maxEventTime - eventTimestamp;
        latenessSamples.add(lateness);
    }

    maxEventTime = Math.max(maxEventTime, eventTimestamp);

    if (latenessSamples.size() > 5000) {
        latenessSamples.remove(0);
    }
}

    @Override
public void onPeriodicEmit(WatermarkOutput output) {

    if (maxEventTime == Long.MIN_VALUE) {
        return;
    }

    if (latenessSamples.size() < 100) {

        output.emitWatermark(
                new Watermark(
                        maxEventTime - 30 * 60 * 1000
                ));

        return;
    }

    List<Long> sorted = new ArrayList<>(latenessSamples);
    Collections.sort(sorted);

    int index = (int) Math.ceil(0.95 * sorted.size()) - 1;
    index = Math.max(index, 0);

    long p95 = sorted.get(index);

    long watermark = maxEventTime - p95;

    output.emitWatermark(new Watermark(watermark));

    System.out.println(
            "maxEventTime=" + maxEventTime +
            " p95=" + p95 +
            " watermark=" + watermark
    );
}
        }