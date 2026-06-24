package it.uniroma2.sabd.flink.query.query1;

import static it.uniroma2.sabd.flink.query.TargetAirlines.TARGET_AIRLINES;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.sink.QuerySinks;
import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.model.Query1Stats;
import java.time.Duration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public final class Query1 {
    /*
     * voli relativi alle compagnie AA (American Airlines), DL (Delta),
     * UA (United) e WN (Southwest), aggregare gli eventi usando finestre tumbling di durata pari a 1 ora,
     * basate sull'event time.
     * Per ciascuna finestra e per ciascuna compagnia, calcolare:
     *      - il numero totale di voli osservati;
     *      - il numero di voli completati (cioe non cancellati e non deviati), cancellati e deviati;
     *      - il valor medio di DEP DELAY, considerando solo i voli non cancellati;
     *      - il tasso di cancellazione, definito come percentuale di voli cancellati sul totale dei voli osservati
     *        nella finestra;
     *      - il tasso di partenze in ritardo, definito come percentuale di voli non cancellati con DEP DELAY
     *        maggiore di 15 minuti.
     */
    private Query1() {
    }

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config) {
        DataStream<Query1Stats> stats = flightStream
                .filter(event -> TARGET_AIRLINES.contains(event.getCarrier()))
                .keyBy(FlightEvent::getCarrier)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(
                        new Query1Accumulator(),
                        new FinalizeQuery1Stats()
                );

        stats
                .map(Query1Stats::toCSV)
                .sinkTo(QuerySinks.query1Csv())
                .name("Query1 CSV Sink");
    }
}
