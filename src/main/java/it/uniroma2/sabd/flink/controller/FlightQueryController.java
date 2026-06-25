package it.uniroma2.sabd.flink.controller;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.query.query1.Query1;
//import it.uniroma2.sabd.flink.query.query3.Query3;
import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlightQueryController {

    private final AppConfig config;
    private final String watermarkName;

    public FlightQueryController(AppConfig config, String watermarkName) {
        this.config = config;
        this.watermarkName = watermarkName;
    }

    public void buildQueries(DataStream<FlightEvent> flightStream) {
        Query1.execute(flightStream, config, watermarkName);
     //   Query2.execute(flightStream, config);
        //Query3.execute(flightStream, config, watermarkName);
    }
}
