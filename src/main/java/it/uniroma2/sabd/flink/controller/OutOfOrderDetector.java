package it.uniroma2.sabd.flink.controller;

import it.uniroma2.sabd.flink.model.OutOfOrderEvent;
import it.uniroma2.sabd.model.FlightEvent;

import java.time.ZoneOffset;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class OutOfOrderDetector
        extends ProcessFunction<FlightEvent, OutOfOrderEvent> {

    private transient int subtaskIndex;
    private long maxTimestampMs;

    @Override
    public void open(Configuration parameters) {
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        maxTimestampMs = Long.MIN_VALUE;
    }

    @Override
    public void processElement(
            FlightEvent event,
            Context ctx,
            Collector<OutOfOrderEvent> out) throws Exception {

        if (event.getEventTime() == null) {
            return;
        }

        long currentMs = event.getEventTime()
                .toEpochSecond(ZoneOffset.UTC) * 1000L;

        if (maxTimestampMs == Long.MIN_VALUE) {
            maxTimestampMs = currentMs;
            return;
        }

        if (currentMs < maxTimestampMs) {

            long latenessMs = maxTimestampMs - currentMs;

            out.collect(
                    new OutOfOrderEvent(
                            subtaskIndex,
                            event.getCarrier(),
                            event.getEventTime(),
                            latenessMs));
        } else {
            maxTimestampMs = currentMs;
        }
    }
}
