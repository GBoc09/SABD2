package it.uniroma2.sabd.flink.watermark;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class CustomStrategy implements WatermarkFactory{
    @Override
    public WatermarkStrategy<FlightEvent> create() {
        return null;
    }
    // TODO: seconda strategia
}
