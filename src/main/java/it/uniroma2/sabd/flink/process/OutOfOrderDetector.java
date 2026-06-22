package it.uniroma2.sabd.flink.process;

import it.uniroma2.sabd.model.FlightEvent;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.ZoneOffset;

public class OutOfOrderDetector
        extends KeyedProcessFunction<String, FlightEvent, FlightEvent> {

    private ValueState<Long> maxTimestampState;

    // Statistiche
    private long totalEvents;
    private long outOfOrderEvents;
    private long maxLatenessMs;
    private long totalLatenessMs;
    private long lastEventTimeSeen;

    // Stampa un report ogni N eventi
    private static final long REPORT_EVERY = 1_000;

    @Override
    public void open(Configuration parameters) {

        maxTimestampState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "maxTimestamp",
                        Long.class
                )
        );

        totalEvents = 0;
        outOfOrderEvents = 0;
        maxLatenessMs = 0;
        totalLatenessMs = 0;
    }

    @Override
    public void processElement(
            FlightEvent event,
            Context ctx,
            Collector<FlightEvent> out) throws Exception {

        totalEvents++;

        if (event.getEventTime() == null) {
            out.collect(event);
            return;
        }

        long currentMs = event.getEventTime()
                .toEpochSecond(ZoneOffset.UTC) * 1000L;

        Long maxMs = maxTimestampState.value();

        if (maxMs == null) {

            // Primo evento osservato
            maxTimestampState.update(currentMs);

        } else if (currentMs < maxMs) {

            long latenessMs = maxMs - currentMs;

            outOfOrderEvents++;
            totalLatenessMs += latenessMs;

            if (latenessMs > maxLatenessMs) {
                maxLatenessMs = latenessMs;
            }

            System.out.println(
                    "\n⚠️ OUT OF ORDER EVENT DETECTED\n" +
                            "Carrier        : " + event.getCarrier() + "\n" +
                            "Event Time     : " + event.getEventTime() + "\n" +
                            "Current ms     : " + currentMs + "\n" +
                            "Max seen ms    : " + maxMs + "\n" +
                            "Lateness       : " + latenessMs + " ms\n" +
                            "Origin airport : " + event.getOriginAirportId() + "\n"
            );
        } else {

            maxTimestampState.update(currentMs);
        }

        // Report periodico
        if (totalEvents % REPORT_EVERY == 0) {

            double percentage =
                    (100.0 * outOfOrderEvents) / totalEvents;

            double avgLateness =
                    outOfOrderEvents == 0
                            ? 0
                            : ((double) totalLatenessMs / outOfOrderEvents);

            System.out.println(
                    "\n========================================\n" +
                            " OUT-OF-ORDER REPORT\n" +
                            "========================================\n" +
                            "Processed events     : " + totalEvents + "\n" +
                            "Out-of-order events  : " + outOfOrderEvents + "\n" +
                            "Percentage           : " + String.format("%.4f", percentage) + "%\n" +
                            "Max lateness         : " + maxLatenessMs + " ms\n" +
                            "Average lateness     : " + String.format("%.2f", avgLateness) + " ms\n" +
                            "========================================\n"
            );
        }

        out.collect(event);
    }
}
