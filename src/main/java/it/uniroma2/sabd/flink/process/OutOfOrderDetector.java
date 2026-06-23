package it.uniroma2.sabd.flink.process;

import it.uniroma2.sabd.model.FlightEvent;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;

public class OutOfOrderDetector
        extends KeyedProcessFunction<String, FlightEvent, FlightEvent> {

    private ValueState<Long> maxTimestampState;

    private static final Logger LOG =
        LoggerFactory.getLogger(OutOfOrderDetector.class);

    // Statistiche
    private long totalEvents;
    private long outOfOrderEvents;
    private long maxLatenessMs;
    private long totalLatenessMs;
    private long lastEventTimeSeen;

    private long over1s;
    private long over5s;
    private long over10s;
    private long over30s;

    // Variabile dinamica
    private final long reportEvery;

    // NUOVO COSTRUTTORE CHE RISOLVE IL CONFLITTO
    public OutOfOrderDetector(long reportEvery) {
        this.reportEvery = reportEvery;
    }

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

            /*System.out.println(
                    "\n⚠️ OUT OF ORDER EVENT DETECTED\n" +
                            "Carrier        : " + event.getCarrier() + "\n" +
                            "Event Time     : " + event.getEventTime() + "\n" +
                            "Current ms     : " + currentMs + "\n" +
                            "Max seen ms    : " + maxMs + "\n" +
                            "Lateness       : " + latenessMs + " ms\n" +
                            "Origin airport : " + event.getOriginAirportId() + "\n"
            );*/

            LOG.warn(
    "OUT_OF_ORDER subtask={} carrier={} eventTime={} currentMs={} maxMs={} latenessMs={} originAirport={}",
    getRuntimeContext().getIndexOfThisSubtask(),
    event.getCarrier(),
    event.getEventTime(),
    currentMs,
    maxMs,
    latenessMs,
    event.getOriginAirportId()
        );
        } else {

            maxTimestampState.update(currentMs);
        }

        // Report periodico
        if (totalEvents % reportEvery == 0) {

            double percentage =
                    (100.0 * outOfOrderEvents) / totalEvents;

            double avgLateness =
                    outOfOrderEvents == 0
                            ? 0
                            : ((double) totalLatenessMs / outOfOrderEvents);

            /*System.out.println(
                    "\n========================================\n" +
                            " OUT-OF-ORDER REPORT\n" +
                            "========================================\n" +
                            "Processed events     : " + totalEvents + "\n" +
                            "Out-of-order events  : " + outOfOrderEvents + "\n" +
                            "Percentage           : " + String.format("%.4f", percentage) + "%\n" +
                            "Max lateness         : " + maxLatenessMs + " ms\n" +
                            "Average lateness     : " + String.format("%.2f", avgLateness) + " ms\n" +
                            "========================================\n"
            );*/
            LOG.info(
    "OUT_OF_ORDER_REPORT subtask={} processed={} outOfOrder={} percentage={} maxLatenessMs={} avgLatenessMs={}",
    getRuntimeContext().getIndexOfThisSubtask(),
    totalEvents,
    outOfOrderEvents,
    over1s,
    over5s,
    over10s,
    over30s,
    String.format("%.4f", percentage),
    maxLatenessMs,
    String.format("%.2f", avgLateness)
        );
        }

        out.collect(event);
    }
}
