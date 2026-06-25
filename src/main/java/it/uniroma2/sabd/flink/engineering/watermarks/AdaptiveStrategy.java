package it.uniroma2.sabd.flink.engineering.watermarks;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.ZoneOffset;

public class AdaptiveStrategy implements WatermarkFactory {

    @Override
    public WatermarkStrategy<FlightEvent> create() {

        return WatermarkStrategy
                .<FlightEvent>forGenerator(
                        new AdaptiveWatermarkGeneratorSupplier()
                )
                .withTimestampAssigner((event, ts) ->
                        event.getEventTime()
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli()
                );
    }
}