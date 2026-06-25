package it.uniroma2.sabd.flink.query.query3;

import static it.uniroma2.sabd.flink.query.TargetAirlines.TARGET_AIRLINES;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.sink.QuerySinks;
import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.model.Query3GlobalStats;
import it.uniroma2.sabd.model.Query3Stats;
import java.time.Duration;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public final class Query3 {

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

        DataStream<Query3Stats> daily = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .aggregate(
                        new Query3Accumulator(),
                        new FinalizeQuery3Stats()
                );  

        DataStream<Query3Stats> weekly = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .window(TumblingEventTimeWindows.of(Duration.ofDays(7)))
                .aggregate(
                        new Query3Accumulator(),
                        new FinalizeQuery3Stats()
                ); 

        DataStream<Query3GlobalStats> global = validFlights
                .keyBy(Query3::airlineDepartureHourKey)
                .process(new GlobalTDigestProcessFunction());

        daily
                .map(Query3Stats::toCSV)
                .sinkTo(QuerySinks.query3OneDayCsv(watermarkName))
                .name("Query3 1-Day CSV Sink");

        weekly
                .map(Query3Stats::toCSV)
                .sinkTo(QuerySinks.query3SevenDaysCsv(watermarkName))
                .name("Query3 7-Day CSV Sink");

        global
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
