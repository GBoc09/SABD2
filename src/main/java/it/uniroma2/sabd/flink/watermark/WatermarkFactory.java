package it.uniroma2.sabd.flink.watermark;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public interface WatermarkFactory {
    WatermarkStrategy<FlightEvent> create();
}
