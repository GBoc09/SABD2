package it.uniroma2.sabd.flink.controller;

import it.uniroma2.sabd.flink.io.sink.CsvValues;
import it.uniroma2.sabd.model.FlightEvent;

import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WatermarkLateEventDetector
        extends ProcessFunction<FlightEvent, FlightEvent> {

    public static final OutputTag<String> LATE_AFTER_WATERMARK_TAG =
            new OutputTag<String>("late-after-watermark") { };

    @Override
    public void processElement(
            FlightEvent event,
            Context ctx,
            Collector<FlightEvent> out) {

        Long eventTimestamp = ctx.timestamp();
        long currentWatermark = ctx.timerService().currentWatermark();

        if (eventTimestamp == null && event.getEventTime() != null) {
            eventTimestamp = event.getEventTime()
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        }

        if (eventTimestamp != null
                && currentWatermark != Long.MIN_VALUE
                && eventTimestamp <= currentWatermark) {

            long latenessMs = currentWatermark - eventTimestamp;
            ctx.output(
                    LATE_AFTER_WATERMARK_TAG,
                    toCsv(event, eventTimestamp, currentWatermark, latenessMs));
        }

        out.collect(event);
    }

    private String toCsv(
            FlightEvent event,
            long eventTimestamp,
            long currentWatermark,
            long latenessMs) {

        return String.join(",",
                CsvValues.text(event.getEventTime() == null
                        ? ""
                        : event.getEventTime().toString()),
                Long.toString(eventTimestamp),
                CsvValues.text(Instant.ofEpochMilli(currentWatermark).toString()),
                Long.toString(currentWatermark),
                Long.toString(latenessMs),
                CsvValues.text(event.getCarrier()),
                Integer.toString(event.getOriginAirportId()),
                Integer.toString(event.getDestAirportId()),
                Integer.toString(event.getCrsDepTime()),
                CsvValues.decimal(event.getDepDelay()),
                CsvValues.decimal(event.getCancelled()),
                CsvValues.decimal(event.getDiverted()),
                Long.toString(event.getProducedAt()));
    }
}
