package it.uniroma2.sabd.flink.io.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class FlightEventDeserializer
        implements MapFunction<String, FlightEvent> {

    private transient ObjectMapper mapper;

    @Override
    public FlightEvent map(String json) throws Exception {

        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
        }

        return mapper.readValue(json, FlightEvent.class);
    }
}
