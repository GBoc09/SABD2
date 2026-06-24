package it.uniroma2.sabd.flink.process;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutOfOrderStatisticProcessor
        extends KeyedProcessFunction<String,
                                     OutOfOrderEvent,
                                     OutOfOrderEvent> {

    private static final Logger LOG =
            LoggerFactory.getLogger(
                    OutOfOrderStatisticProcessor.class);

    private final long reportEvery;

    private long totalOutOfOrder;
    private long totalLatenessMs;
    private long maxLatenessMs;

    private String worstCarrier;
    private String worstFlightTime;

    private final Map<String, Long> carrierCounts =
            new HashMap<>();

    public OutOfOrderStatisticProcessor(long reportEvery) {
        this.reportEvery = reportEvery;
    }

    @Override
    public void open(Configuration parameters) {

        totalOutOfOrder = 0;
        totalLatenessMs = 0;
        maxLatenessMs = 0;
    }

    @Override
    public void processElement(
            OutOfOrderEvent event,
            Context ctx,
            Collector<OutOfOrderEvent> out) {

        totalOutOfOrder++;

        totalLatenessMs += event.getLatenessMs();

        carrierCounts.merge(
                event.getCarrier(),
                1L,
                Long::sum);

        if (event.getLatenessMs() > maxLatenessMs) {

            maxLatenessMs = event.getLatenessMs();
            worstCarrier = event.getCarrier();
            worstFlightTime =
                    event.getEventTime().toString();
        }

        if (totalOutOfOrder % reportEvery == 0) {

            printReport();
        }

        out.collect(event);
    }

    private void printReport() {

        double avgLatenessMs =
                totalOutOfOrder == 0
                        ? 0
                        : ((double) totalLatenessMs)
                        / totalOutOfOrder;

        /*LOG.info(
                "\n==============================\n" +
                " OUT OF ORDER REPORT\n" +
                "==============================\n" +
                "Out-of-order events : {}\n" +
                "Average lateness ms : {}\n" +
                "Maximum lateness ms : {}\n" +
                "Worst carrier       : {}\n" +
                "Worst flight time   : {}\n" +
                "Top carriers        : {}\n" +
                "==============================",
                totalOutOfOrder,
                String.format("%.2f", avgLatenessMs),
                maxLatenessMs,
                worstCarrier,
                worstFlightTime,
                topCarriers()
        );*/ 
        LOG.info(
        "OUT_OF_ORDER_REPORT total={} avgLatenessMs={} maxLatenessMs={} "
        + "worstCarrier={} worstFlightTime={} topCarriers={}",
        totalOutOfOrder,
        String.format("%.2f", avgLatenessMs),
        maxLatenessMs,
        worstCarrier,
        worstFlightTime,
        topCarriers()
    );
    }

    private String topCarriers() {

        return carrierCounts.entrySet()
                .stream()
                .sorted(
                        Map.Entry.<String, Long>
                                comparingByValue(
                                        Comparator.reverseOrder()))
                .limit(5)
                .map(e ->
                        e.getKey() + "=" + e.getValue())
                .reduce(
                        (a, b) -> a + ", " + b)
                .orElse("N/A");
    }
}