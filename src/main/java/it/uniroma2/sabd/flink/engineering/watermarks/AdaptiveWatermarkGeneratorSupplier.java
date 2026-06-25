package it.uniroma2.sabd.flink.engineering.watermarks;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;

public class AdaptiveWatermarkGeneratorSupplier
        implements WatermarkGeneratorSupplier<FlightEvent> {

    @Override
    public WatermarkGenerator<FlightEvent> createWatermarkGenerator(Context context) {
        return new AdaptiveWatermarkGenerator();
    }
}