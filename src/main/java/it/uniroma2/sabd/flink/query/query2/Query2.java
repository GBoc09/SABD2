package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.controller.LatencyMonitor;
import it.uniroma2.sabd.flink.controller.PerformanceMetricTags;
import it.uniroma2.sabd.flink.controller.ThroughputMonitor;
import it.uniroma2.sabd.flink.io.sink.DiscardedTupleSinks;
import it.uniroma2.sabd.flink.io.sink.PerformanceSinks;
import it.uniroma2.sabd.flink.io.sink.QuerySinks;
import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.flink.model.Query2Stats;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

/**
 * Query 2: top-10 aeroporti per ritardi significativi (DEP_DELAY > 30 min).
 * Finestre: 1h, 6h, dall'inizio del dataset.
 * Pattern identico a Query3:
 *   - filtro voli validi
 *   - keyBy(originAirportId) → Accumulator → FinalizeStats per 1h e 6h
 *   - GlobalQuery2ProcessFunction per la finestra globale
 *   - RankingProcessFunction come secondo stage per il top-10
 */
public final class Query2 {
    private static final OutputTag<FlightEvent> LATE_1H_TAG =
            new OutputTag<FlightEvent>("q2-discarded-1h") { };

    private static final OutputTag<FlightEvent> LATE_6H_TAG =
            new OutputTag<FlightEvent>("q2-discarded-6h") { };

    private Query2() {}

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config, String watermarkName) {
        
        // Solo voli non cancellati e non deviati
        DataStream<FlightEvent> validFlights = flightStream
                .filter(e -> e.getCancelled() == 0.0 && e.getDiverted() == 0.0)
                .name("Q2 valid flights");

        SingleOutputStreamOperator<Query2Stats> aggregated1h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .sideOutputLateData(LATE_1H_TAG)
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats());

        DiscardedTupleSinks.writeFixedWindow(
                aggregated1h.getSideOutput(LATE_1H_TAG),
                watermarkName,
                "q2",
                "1h",
                Duration.ofHours(1));

        SingleOutputStreamOperator<Query2Stats> ranking1h = aggregated1h
                .name("Q2 aggregate 1h")
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 1h");

        SingleOutputStreamOperator<Query2Stats> aggregated6h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .sideOutputLateData(LATE_6H_TAG)
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats());

        DiscardedTupleSinks.writeFixedWindow(
                aggregated6h.getSideOutput(LATE_6H_TAG),
                watermarkName,
                "q2",
                "6h",
                Duration.ofHours(6));

        SingleOutputStreamOperator<Query2Stats> ranking6h = aggregated6h
                .name("Q2 aggregate 6h")
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 6h");

        // --- globale ---
        SingleOutputStreamOperator<Query2Stats> globalAggregates = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .process(new GlobalQuery2ProcessFunction())
                .name("Q2 global accumulate");

        DiscardedTupleSinks.writeMonthlyWindow(
                globalAggregates.getSideOutput(GlobalQuery2ProcessFunction.DISCARDED_GLOBAL_TAG),
                watermarkName,
                "q2",
                "global");

        SingleOutputStreamOperator<Query2Stats> rankingGlobal = globalAggregates
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking global");

        String result1hLabel = "q2-1h-result-" + watermarkName;
        String result6hLabel = "q2-6h-result-" + watermarkName;
        String resultGlobalLabel = "q2-global-result-" + watermarkName;

        SingleOutputStreamOperator<Query2Stats> latencyMonitoredRanking1h = ranking1h
                .process(new LatencyMonitor<>(result1hLabel,
                        config.getMetricsLatencyIntervalMs()))
                .name("Q2 1h Result Latency Monitor");

        SingleOutputStreamOperator<Query2Stats> monitoredRanking1h = latencyMonitoredRanking1h
                .process(new ThroughputMonitor<>(result1hLabel,
                        config.getMetricsThroughputIntervalMs()))
                .name("Q2 1h Result Throughput Monitor");

        SingleOutputStreamOperator<Query2Stats> latencyMonitoredRanking6h = ranking6h
                .process(new LatencyMonitor<>(result6hLabel,
                        config.getMetricsLatencyIntervalMs()))
                .name("Q2 6h Result Latency Monitor");

        SingleOutputStreamOperator<Query2Stats> monitoredRanking6h = latencyMonitoredRanking6h
                .process(new ThroughputMonitor<>(result6hLabel,
                        config.getMetricsThroughputIntervalMs()))
                .name("Q2 6h Result Throughput Monitor");

        SingleOutputStreamOperator<Query2Stats> latencyMonitoredRankingGlobal = rankingGlobal
                .process(new LatencyMonitor<>(resultGlobalLabel,
                        config.getMetricsLatencyIntervalMs()))
                .name("Q2 Global Result Latency Monitor");

        SingleOutputStreamOperator<Query2Stats> monitoredRankingGlobal = latencyMonitoredRankingGlobal
                .process(new ThroughputMonitor<>(resultGlobalLabel,
                        config.getMetricsThroughputIntervalMs()))
                .name("Q2 Global Result Throughput Monitor");

        PerformanceSinks.writeLatencyCsvAtPath(
                latencyMonitoredRanking1h.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q2_1h");
        PerformanceSinks.writeLatencyCsvAtPath(
                latencyMonitoredRanking6h.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q2_6h");
        PerformanceSinks.writeLatencyCsvAtPath(
                latencyMonitoredRankingGlobal.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q2_global");
        PerformanceSinks.writeGlobalThroughputCsvAtPath(
                monitoredRanking1h.getSideOutput(PerformanceMetricTags.THROUGHPUT),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/throughput/q2_1h",
                config.getMetricsThroughputIntervalMs());
        PerformanceSinks.writeGlobalThroughputCsvAtPath(
                monitoredRanking6h.getSideOutput(PerformanceMetricTags.THROUGHPUT),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/throughput/q2_6h",
                config.getMetricsThroughputIntervalMs());
        PerformanceSinks.writeGlobalThroughputCsvAtPath(
                monitoredRankingGlobal.getSideOutput(PerformanceMetricTags.THROUGHPUT),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/throughput/q2_global",
                config.getMetricsThroughputIntervalMs());

        // --- sink CSV ---
        monitoredRanking1h
                .map(Query2Stats::toCSV)
                .sinkTo(QuerySinks.query2OneHourCsv(watermarkName))
                .name("Q2 1h CSV Sink");

        monitoredRanking6h
                .map(Query2Stats::toCSV)
                .sinkTo(QuerySinks.query2SixHoursCsv(watermarkName))
                .name("Q2 6h CSV Sink");

        monitoredRankingGlobal
                .map(Query2Stats::toCSV)
                .sinkTo(QuerySinks.query2GlobalCsv(watermarkName))
                .name("Q2 Global CSV Sink");
    }
}
