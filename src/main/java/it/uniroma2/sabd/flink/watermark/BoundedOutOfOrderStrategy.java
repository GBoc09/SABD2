package it.uniroma2.sabd.flink.watermark;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import java.time.Duration;
import java.time.ZoneOffset;

public class BoundedOutOfOrderStrategy implements WatermarkFactory {

    private final long maxOutOfOrderMs;

    public BoundedOutOfOrderStrategy(long maxOutOfOrderMs) {
        this.maxOutOfOrderMs = maxOutOfOrderMs;
    }

    @Override
    public WatermarkStrategy<FlightEvent> create() {
        return WatermarkStrategy
                .<FlightEvent>forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderMs))
                .withIdleness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, ts) ->
                        event.getEventTime().toInstant(ZoneOffset.UTC).toEpochMilli());
    }
}