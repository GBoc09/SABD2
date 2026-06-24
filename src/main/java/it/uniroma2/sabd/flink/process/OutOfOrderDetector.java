package it.uniroma2.sabd.flink.process;

import it.uniroma2.sabd.model.FlightEvent;

import java.time.ZoneOffset;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;



public class OutOfOrderDetector
        extends KeyedProcessFunction<String, FlightEvent, OutOfOrderEvent> {

    private ValueState<Long> maxTimestampState;

    @Override
    public void open(Configuration parameters) {

        maxTimestampState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "maxTimestamp",
                        Long.class));
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

        Long maxMs = maxTimestampState.value();

        if (maxMs == null) {

            maxTimestampState.update(currentMs);
            return;
        }

        if (currentMs < maxMs) {

            long latenessMs = maxMs - currentMs;

            out.collect(
                    new OutOfOrderEvent(
                            event.getCarrier(),
                            event.getEventTime(),
                            latenessMs));
        } else {

            maxTimestampState.update(currentMs);
        }
    }
}