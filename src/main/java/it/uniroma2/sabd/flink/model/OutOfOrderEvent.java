package it.uniroma2.sabd.flink.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class OutOfOrderEvent implements Serializable {

    private String carrier;
    private LocalDateTime eventTime;
    private long latenessMs;

    public OutOfOrderEvent() {
    }

    public OutOfOrderEvent(
            String carrier,
            LocalDateTime eventTime,
            long latenessMs) {

        this.carrier = carrier;
        this.eventTime = eventTime;
        this.latenessMs = latenessMs;
    }

    public String getCarrier() {
        return carrier;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public long getLatenessMs() {
        return latenessMs;
    }
}
