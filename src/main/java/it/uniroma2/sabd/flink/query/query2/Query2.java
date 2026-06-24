package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class Query2 {

    private Query2() {
    }

    public static void execute(DataStream<FlightEvent> flightStream, AppConfig config) {
        // TODO: implementare la logica della query 2.
    }
}
