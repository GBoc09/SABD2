package it.uniroma2.sabd.flink.query.query1;

import static it.uniroma2.sabd.flink.query.TargetAirlines.TARGET_AIRLINES;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.io.sink.DiscardedTupleSinks;
import it.uniroma2.sabd.flink.io.sink.QuerySinks;
import it.uniroma2.sabd.flink.model.Query1Stats;
import it.uniroma2.sabd.flink.query.common.QueryMonitoringPipeline;
import it.uniroma2.sabd.model.FlightEvent;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

public final class Query1 {

    private static final OutputTag<FlightEvent> LATE_1H_TAG =
            new OutputTag<FlightEvent>("q1-discarded-1h") { };

    private Query1() {}

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config, String watermarkName) {

        SingleOutputStreamOperator<Query1Stats> stats = flightStream
                .filter(event -> TARGET_AIRLINES.contains(event.getCarrier()))
                .keyBy(FlightEvent::getCarrier)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .sideOutputLateData(LATE_1H_TAG)
                .aggregate(new Query1Accumulator(), new FinalizeQuery1Stats());

        DiscardedTupleSinks.writeFixedWindow(
                stats.getSideOutput(LATE_1H_TAG),
                watermarkName, "q1", "1h", Duration.ofHours(1));

        QueryMonitoringPipeline.monitorAndSink(
                stats, config, watermarkName, "q1", "1h",
                Query1Stats::toCSV,
                QuerySinks.query1Csv(watermarkName));
    }
}