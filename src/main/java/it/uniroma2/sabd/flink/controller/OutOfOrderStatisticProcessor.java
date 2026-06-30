package it.uniroma2.sabd.flink.controller;

import it.uniroma2.sabd.flink.model.OutOfOrderEvent;
import java.io.Serializable;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutOfOrderStatisticProcessor
        extends KeyedProcessFunction<Integer,
                                     OutOfOrderEvent,
                                     OutOfOrderEvent> {

    private static final Logger LOG =
            LoggerFactory.getLogger(
                    OutOfOrderStatisticProcessor.class);

    private final long reportEvery;

    private transient ValueState<Stats> statsState;

    public OutOfOrderStatisticProcessor(long reportEvery) {
        this.reportEvery = reportEvery;
    }

    @Override
    public void open(Configuration parameters) {
        statsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("out-of-order-stats", Stats.class));
    }

    @Override
    public void processElement(
            OutOfOrderEvent event,
            Context ctx,
            Collector<OutOfOrderEvent> out) throws Exception {

        Stats stats = statsState.value();
        if (stats == null) {
            stats = new Stats(event.getSubtaskIndex());
        }

        stats.totalOutOfOrder++;
        stats.totalLatenessMs += event.getLatenessMs();

        if (event.getLatenessMs() > stats.maxLatenessMs) {
            stats.maxLatenessMs = event.getLatenessMs();
        }

        statsState.update(stats);

        if (stats.totalOutOfOrder % reportEvery == 0) {
            printReport(stats);
        }

        out.collect(event);
    }

    private void printReport(Stats stats) {

        double avgLatenessMs =
                stats.totalOutOfOrder == 0
                        ? 0
                        : ((double) stats.totalLatenessMs)
                        / stats.totalOutOfOrder;

        LOG.info(
                "OUT_OF_ORDER_REPORT sourceSubtask={} total={} avgLatenessMs={} "
                + "maxLatenessMs={}",
                stats.sourceSubtaskIndex,
                stats.totalOutOfOrder,
                String.format("%.2f", avgLatenessMs),
                stats.maxLatenessMs);
    }

    private static final class Stats implements Serializable {
        private int sourceSubtaskIndex;
        private long totalOutOfOrder;
        private long totalLatenessMs;
        private long maxLatenessMs;

        private Stats() {
        }

        private Stats(int sourceSubtaskIndex) {
            this.sourceSubtaskIndex = sourceSubtaskIndex;
        }
    }
}
