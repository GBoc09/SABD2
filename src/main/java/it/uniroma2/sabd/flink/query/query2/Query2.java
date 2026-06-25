package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.io.sink.QuerySinks;
import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.flink.model.Query2Stats;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

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

    private Query2() {}

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config, String watermarkName) {

        // Solo voli non cancellati e non deviati
        DataStream<FlightEvent> validFlights = flightStream
                .filter(e -> e.getCancelled() == 0.0 && e.getDiverted() == 0.0)
                .name("Q2 valid flights");

        // --- 1h ---
        DataStream<Query2Stats> ranking1h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats())
                .name("Q2 aggregate 1h")
                .keyBy(Query2Stats::getWindowStartEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 1h");

        // --- 6h ---
        DataStream<Query2Stats> ranking6h = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .aggregate(new Query2Accumulator(), new FinalizeQuery2Stats())
                .name("Q2 aggregate 6h")
                .keyBy(Query2Stats::getWindowStartEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking 6h");

        // --- globale ---
        DataStream<Query2Stats> rankingGlobal = validFlights
                .keyBy(FlightEvent::getOriginAirportId)
                .process(new GlobalQuery2ProcessFunction())
                .name("Q2 global accumulate")
                .keyBy(Query2Stats::getWindowStartEpoch)
                .process(new RankingProcessFunction())
                .name("Q2 ranking global");

        // --- sink CSV ---
        ranking1h
                .map(Query2Stats::toCSV)
                .sinkTo(QuerySinks.query2OneHourCsv(watermarkName))
                .name("Q2 1h CSV Sink");

        ranking6h
                .map(Query2Stats::toCSV)
                .sinkTo(QuerySinks.query2SixHoursCsv(watermarkName))
                .name("Q2 6h CSV Sink");

        rankingGlobal
                .map(Query2Stats::toCSV)
                .sinkTo(QuerySinks.query2GlobalCsv(watermarkName))
                .name("Q2 Global CSV Sink");
    }
}
