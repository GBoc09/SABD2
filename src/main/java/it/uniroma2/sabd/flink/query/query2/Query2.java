package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.controller.LatencyMonitor;
import it.uniroma2.sabd.flink.controller.PerformanceMetricTags;
import it.uniroma2.sabd.flink.io.sink.PerformanceSinks;
import it.uniroma2.sabd.flink.io.sink.QuerySinks;
import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.flink.model.Query2Stats;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import java.time.Instant;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.ProcessFunction;

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
        public static final OutputTag<FlightEvent> q2Late1hTag = 
            new OutputTag<FlightEvent>("q2-late-flights-1h") {};
            
    public static final OutputTag<FlightEvent> q2Late6hTag = 
            new OutputTag<FlightEvent>("q2-late-flights-6h") {};

    private Query2() {}

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config, String watermarkName) {
        
        // Solo voli non cancellati e non deviati
        DataStream<FlightEvent> validFlights = flightStream
                .filter(e -> e.getCancelled() == 0.0 && e.getDiverted() == 0.0)
                .name("Q2 valid flights");

        // --- 1h ---
        /*DataStream<Query2Stats> ranking1h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats())
                .name("Q2 aggregate 1h")
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 1h");

        // --- 6h ---
        DataStream<Query2Stats> ranking6h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats())
                .name("Q2 aggregate 6h")
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 6h");*/

        SingleOutputStreamOperator<Query2Stats> aggregated1h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .sideOutputLateData(q2Late1hTag) // <--- INTERCETTA SCARTI 1H
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats());

        SingleOutputStreamOperator<Query2Stats> ranking1h = aggregated1h
                .name("Q2 aggregate 1h")
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 1h");

        SingleOutputStreamOperator<Query2Stats> aggregated6h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .sideOutputLateData(q2Late6hTag) // <--- INTERCETTA SCARTI 6H
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats());

        SingleOutputStreamOperator<Query2Stats> ranking6h = aggregated6h
                .name("Q2 aggregate 6h")
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 6h");

        // --- globale ---
        SingleOutputStreamOperator<Query2Stats> rankingGlobal = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .process(new GlobalQuery2ProcessFunction())
                .name("Q2 global accumulate")
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking global");

        SingleOutputStreamOperator<Query2Stats> monitoredRanking1h = ranking1h
                .process(new LatencyMonitor<>("q2-1h-result-" + watermarkName,
                        config.getMetricsLatencyIntervalMs()))
                .name("Q2 1h Result Latency Monitor");

        SingleOutputStreamOperator<Query2Stats> monitoredRanking6h = ranking6h
                .process(new LatencyMonitor<>("q2-6h-result-" + watermarkName,
                        config.getMetricsLatencyIntervalMs()))
                .name("Q2 6h Result Latency Monitor");

        SingleOutputStreamOperator<Query2Stats> monitoredRankingGlobal = rankingGlobal
                .process(new LatencyMonitor<>("q2-global-result-" + watermarkName,
                        config.getMetricsLatencyIntervalMs()))
                .name("Q2 Global Result Latency Monitor");

        PerformanceSinks.writeLatencyCsvAtPath(
                monitoredRanking1h.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q2_1h");
        PerformanceSinks.writeLatencyCsvAtPath(
                monitoredRanking6h.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q2_6h");
        PerformanceSinks.writeLatencyCsvAtPath(
                monitoredRankingGlobal.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName + "/latency/q2_global");

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


        // =====================================================================
        // LOGICHE DI EXPORT PER I DATI SCARTATI (ATTIVE SOLO SU WM15)
        // =====================================================================
        if ("WM15".equals(watermarkName)) {
            
            // Definiamo i path assoluti esatti per il container
            String path1h = "/opt/flink/output/WM15/query2/discarded_1h";
            String path6h = "/opt/flink/output/WM15/query2/discarded_6h";

            // Creiamo i sink di Flink per scrivere file di testo riga per riga
            org.apache.flink.connector.file.sink.FileSink<String> discard1hSink = 
                    org.apache.flink.connector.file.sink.FileSink
                    .forRowFormat(new org.apache.flink.core.fs.Path(path1h), new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>("UTF-8"))
                    .withRollingPolicy(org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy.builder().build())
                    .build();

            org.apache.flink.connector.file.sink.FileSink<String> discard6hSink = 
                    org.apache.flink.connector.file.sink.FileSink
                    .forRowFormat(new org.apache.flink.core.fs.Path(path6h), new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>("UTF-8"))
                    .withRollingPolicy(org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy.builder().build())
                    .build();

            // --- Scrittura scarti 1 ora ---
            aggregated1h.getSideOutput(q2Late1hTag)
                    .map(event -> {
                        long windowSizeMs = Duration.ofHours(1).toMillis();
                        long epoch = event.getEventTime().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
                        long windowStart = epoch - (epoch % windowSizeMs);
                        long windowEnd = windowStart + windowSizeMs;

                        return String.format("1h,%d,%s,%s,%s,%s",
                                event.getOriginAirportId(), event.getCarrier(), event.getEventTime(),
                                Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowEnd));
                    })
                    .sinkTo(discard1hSink)
                    .name("Q2 1h Discarded File Sink");

            // --- Scrittura scarti 6 ore ---
            aggregated6h.getSideOutput(q2Late6hTag)
                    .map(event -> {
                        long windowSizeMs = Duration.ofHours(6).toMillis();
                        long epoch = event.getEventTime().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
                        long windowStart = epoch - (epoch % windowSizeMs);
                        long windowEnd = windowStart + windowSizeMs;

                        return String.format("6h,%d,%s,%s,%s,%s",
                                event.getOriginAirportId(), event.getCarrier(), event.getEventTime(),
                                Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowEnd));
                    })
                    .sinkTo(discard6hSink)
                    .name("Q2 6h Discarded File Sink");
        }
    }
}
