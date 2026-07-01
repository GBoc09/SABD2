package it.uniroma2.sabd.flink.query.query3;

import static it.uniroma2.sabd.flink.query.TargetAirlines.TARGET_AIRLINES;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.io.sink.DiscardedTupleSinks;
import it.uniroma2.sabd.flink.io.sink.QuerySinks;
import it.uniroma2.sabd.flink.model.Query3GlobalStats;
import it.uniroma2.sabd.flink.model.Query3Key;
import it.uniroma2.sabd.flink.model.Query3Stats;
import it.uniroma2.sabd.flink.query.common.QueryMonitoringPipeline;
import it.uniroma2.sabd.model.FlightEvent;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

public final class Query3 {

    private static final OutputTag<FlightEvent> LATE_1DAY_TAG =
            new OutputTag<FlightEvent>("q3-discarded-1day") { };

    private static final OutputTag<FlightEvent> LATE_7DAY_TAG =
            new OutputTag<FlightEvent>("q3-discarded-7day") { };

    private Query3() {}

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config, String watermarkName) {

        DataStream<FlightEvent> validFlights = flightStream
                .filter(event -> TARGET_AIRLINES.contains(event.getCarrier()))
                .filter(event -> event.getCancelled() == 0.0 && event.getDiverted() == 0.0);

        // --- 1 giorno ---
        SingleOutputStreamOperator<Query3Stats> daily = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .sideOutputLateData(LATE_1DAY_TAG)
                .aggregate(new Query3Accumulator(), new FinalizeQuery3Stats());

        DiscardedTupleSinks.writeFixedWindow(
                daily.getSideOutput(LATE_1DAY_TAG),
                watermarkName, "q3", "1day", Duration.ofDays(1));

        // --- 7 giorni ---
        SingleOutputStreamOperator<Query3Stats> weekly = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(7)))
                .sideOutputLateData(LATE_7DAY_TAG)
                .aggregate(new Query3Accumulator(), new FinalizeQuery3Stats());

        DiscardedTupleSinks.writeFixedWindow(
                weekly.getSideOutput(LATE_7DAY_TAG),
                watermarkName, "q3", "7day", Duration.ofDays(7));

        // --- globale ---
        SingleOutputStreamOperator<Query3GlobalStats> global = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .process(new GlobalTDigestProcessFunction());

        DiscardedTupleSinks.writeMonthlyWindow(
                global.getSideOutput(GlobalTDigestProcessFunction.DISCARDED_GLOBAL_TAG),
                watermarkName, "q3", "global");

        // --- monitoraggio + sink ---
        QueryMonitoringPipeline.monitorAndSink(
                daily, config, watermarkName, "q3", "1day",
                Query3Stats::toCSV, QuerySinks.query3OneDayCsv(watermarkName));

        QueryMonitoringPipeline.monitorAndSink(
                weekly, config, watermarkName, "q3", "7day",
                Query3Stats::toCSV, QuerySinks.query3SevenDaysCsv(watermarkName));

        QueryMonitoringPipeline.monitorAndSink(
                global, config, watermarkName, "q3", "global",
                Query3GlobalStats::toCSV, QuerySinks.queryGlobalCsv(watermarkName));
    }

    static Query3Key airlineDepartureHourKey(FlightEvent event) {
        return new Query3Key(event.getCarrier(), departureHour(event.getCrsDepTime()));
    }

    static int departureHour(int crsDepTime) {
        int hour = crsDepTime / 100;
        if (hour == 24) {
            return 0;
        }
        return Math.max(0, Math.min(23, hour));
    }
}