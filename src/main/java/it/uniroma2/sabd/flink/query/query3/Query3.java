package it.uniroma2.sabd.flink.query.query3;

import static it.uniroma2.sabd.flink.query.TargetAirlines.TARGET_AIRLINES;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.controller.LatencyMonitor;
import it.uniroma2.sabd.flink.controller.PerformanceMetricTags;
import it.uniroma2.sabd.flink.controller.ThroughputMonitor;
import it.uniroma2.sabd.flink.io.sink.DiscardedTupleSinks;
import it.uniroma2.sabd.flink.io.sink.PerformanceSinks;
import it.uniroma2.sabd.flink.io.sink.QuerySinks;
import it.uniroma2.sabd.flink.model.Query3GlobalStats;
import it.uniroma2.sabd.flink.model.Query3Key;
import it.uniroma2.sabd.flink.model.Query3Stats;
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

    private Query3() {
    }

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config, String watermarkName) {
        /*
        La query Q3 si concentra sul calcolo in tempo reale della distribuzione dei ritardi in partenza per
        compagnia aerea e fascia oraria.
        Facendo riferimento ai voli relativi alle compagnie AA (American Airlines), DL (Delta), UA (United)
        e WN (Southwest), considerare i valori di DEP DELAY dei voli non cancellati e non deviati. Per
        ciascun volo, ricavare la fascia oraria di partenza programmata a partire dal campo CRS DEP TIME,
        considerando le 24 fasce orarie giornaliere.
        Per ciascuna compagnia e per ciascuna fascia oraria, calcolare:
            • il numero di voli considerati;
            • il minimo di DEP DELAY;
            • il 25-esimo percentile;
            • il 50-esimo percentile, o mediana;
            • il 75-esimo percentile;
            • il 90-esimo percentile;
            • il massimo di DEP DELAY.
        Calcolare la query sulle seguenti finestre temporali:
            • 1 giorno (event time);
            • 7 giorni (event time);
            • dall’inizio del dataset
         */
        DataStream<FlightEvent> validFlights = flightStream
            .filter(event -> TARGET_AIRLINES.contains(event.getCarrier()))
            .filter(event -> event.getCancelled()==0.0 && event.getDiverted()==0.0);

        SingleOutputStreamOperator<Query3Stats> daily = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .sideOutputLateData(LATE_1DAY_TAG)
                .aggregate(
                        new Query3Accumulator(),
                        new FinalizeQuery3Stats()
                );  

        DiscardedTupleSinks.writeFixedWindow(
                daily.getSideOutput(LATE_1DAY_TAG),
                watermarkName,
                "q3",
                "1day",
                Duration.ofDays(1));

        SingleOutputStreamOperator<Query3Stats> weekly = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(7)))
                .sideOutputLateData(LATE_7DAY_TAG)
                .aggregate(
                        new Query3Accumulator(),
                        new FinalizeQuery3Stats()
                ); 

        DiscardedTupleSinks.writeFixedWindow(
                weekly.getSideOutput(LATE_7DAY_TAG),
                watermarkName,
                "q3",
                "7day",
                Duration.ofDays(7));

        SingleOutputStreamOperator<Query3GlobalStats> global = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .process(new GlobalTDigestProcessFunction());

        DiscardedTupleSinks.writeMonthlyWindow(
                global.getSideOutput(GlobalTDigestProcessFunction.DISCARDED_GLOBAL_TAG),
                watermarkName,
                "q3",
                "global");

        String dailyLabel = "q3-1day-result-" + watermarkName;
        String weeklyLabel = "q3-7day-result-" + watermarkName;
        String globalLabel = "q3-global-result-" + watermarkName;

        SingleOutputStreamOperator<Query3Stats> latencyMonitoredDaily = daily
                .process(new LatencyMonitor<>(dailyLabel,
                        config.getMetricsLatencyIntervalMs()))
                .name("Query3 1-Day Result Latency Monitor");

        SingleOutputStreamOperator<Query3Stats> monitoredDaily = latencyMonitoredDaily
                .process(new ThroughputMonitor<>(dailyLabel,
                        config.getMetricsThroughputIntervalMs()))
                .name("Query3 1-Day Result Throughput Monitor");

        SingleOutputStreamOperator<Query3Stats> latencyMonitoredWeekly = weekly
                .process(new LatencyMonitor<>(weeklyLabel,
                        config.getMetricsLatencyIntervalMs()))
                .name("Query3 7-Day Result Latency Monitor");

        SingleOutputStreamOperator<Query3Stats> monitoredWeekly = latencyMonitoredWeekly
                .process(new ThroughputMonitor<>(weeklyLabel,
                        config.getMetricsThroughputIntervalMs()))
                .name("Query3 7-Day Result Throughput Monitor");

        SingleOutputStreamOperator<Query3GlobalStats> latencyMonitoredGlobal = global
                .process(new LatencyMonitor<>(globalLabel,
                        config.getMetricsLatencyIntervalMs()))
                .name("Query3 Global Result Latency Monitor");

        SingleOutputStreamOperator<Query3GlobalStats> monitoredGlobal = latencyMonitoredGlobal
                .process(new ThroughputMonitor<>(globalLabel,
                        config.getMetricsThroughputIntervalMs()))
                .name("Query3 Global Result Throughput Monitor");

        PerformanceSinks.writeLatencyCsvAtPath(
                latencyMonitoredDaily.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q3_1day");
        PerformanceSinks.writeLatencyCsvAtPath(
                latencyMonitoredWeekly.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q3_7day");
        PerformanceSinks.writeLatencyCsvAtPath(
                latencyMonitoredGlobal.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q3_global");
        PerformanceSinks.writeGlobalThroughputCsvAtPath(
                monitoredDaily.getSideOutput(PerformanceMetricTags.THROUGHPUT),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/throughput/q3_1day",
                config.getMetricsThroughputIntervalMs());
        PerformanceSinks.writeGlobalThroughputCsvAtPath(
                monitoredWeekly.getSideOutput(PerformanceMetricTags.THROUGHPUT),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/throughput/q3_7day",
                config.getMetricsThroughputIntervalMs());
        PerformanceSinks.writeGlobalThroughputCsvAtPath(
                monitoredGlobal.getSideOutput(PerformanceMetricTags.THROUGHPUT),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/throughput/q3_global",
                config.getMetricsThroughputIntervalMs());

        monitoredDaily
                .map(Query3Stats::toCSV)
                .sinkTo(QuerySinks.query3OneDayCsv(watermarkName))
                .name("Query3 1-Day CSV Sink");

        monitoredWeekly
                .map(Query3Stats::toCSV)
                .sinkTo(QuerySinks.query3SevenDaysCsv(watermarkName))
                .name("Query3 7-Day CSV Sink");

        monitoredGlobal
                .map(Query3GlobalStats::toCSV)
                .sinkTo(QuerySinks.queryGlobalCsv(watermarkName))
                .name("Query3 Global CSV Sink");

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
