package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.io.sink.DiscardedTupleSinks;
import it.uniroma2.sabd.flink.io.sink.QuerySinks;
import it.uniroma2.sabd.flink.model.Query2Stats;
import it.uniroma2.sabd.flink.query.common.QueryMonitoringPipeline;
import it.uniroma2.sabd.model.FlightEvent;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

/**
 * Query 2: top-10 aeroporti per ritardi significativi (DEP_DELAY > 30 min).
 * Finestre: 1h, 6h, dall'inizio del dataset.
 */
public final class Query2 {

    private static final OutputTag<FlightEvent> LATE_1H_TAG =
            new OutputTag<FlightEvent>("q2-discarded-1h") { };

    private static final OutputTag<FlightEvent> LATE_6H_TAG =
            new OutputTag<FlightEvent>("q2-discarded-6h") { };

    private Query2() {}

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config, String watermarkName) {

        DataStream<FlightEvent> validFlights = flightStream
                .filter(e -> e.getCancelled() == 0.0 && e.getDiverted() == 0.0)
                .name("Q2 valid flights");

        // --- 1h ---
        SingleOutputStreamOperator<Query2Stats> aggregated1h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .sideOutputLateData(LATE_1H_TAG)
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats());

        DiscardedTupleSinks.writeFixedWindow(
                aggregated1h.getSideOutput(LATE_1H_TAG),
                watermarkName, "q2", "1h", Duration.ofHours(1));

        SingleOutputStreamOperator<Query2Stats> ranking1h = aggregated1h
                .name("Q2 aggregate 1h")
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 1h");

        // --- 6h ---
        SingleOutputStreamOperator<Query2Stats> aggregated6h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .sideOutputLateData(LATE_6H_TAG)
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats());

        DiscardedTupleSinks.writeFixedWindow(
                aggregated6h.getSideOutput(LATE_6H_TAG),
                watermarkName, "q2", "6h", Duration.ofHours(6));

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
                watermarkName, "q2", "global");

        SingleOutputStreamOperator<Query2Stats> rankingGlobal = globalAggregates
                .keyBy(Query2Stats::getWindowEndEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking global");

        // --- monitoraggio + sink ---
        QueryMonitoringPipeline.monitorAndSink(
                ranking1h, config, watermarkName, "q2", "1h",
                Query2Stats::toCSV, QuerySinks.query2OneHourCsv(watermarkName));

        QueryMonitoringPipeline.monitorAndSink(
                ranking6h, config, watermarkName, "q2", "6h",
                Query2Stats::toCSV, QuerySinks.query2SixHoursCsv(watermarkName));

        QueryMonitoringPipeline.monitorAndSink(
                rankingGlobal, config, watermarkName, "q2", "global",
                Query2Stats::toCSV, QuerySinks.query2GlobalCsv(watermarkName));
    }
}