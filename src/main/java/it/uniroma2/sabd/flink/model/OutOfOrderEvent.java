package it.uniroma2.sabd.flink.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class OutOfOrderEvent implements Serializable {

    private int subtaskIndex;
    private LocalDateTime eventTime;
    private long latenessMs;

    public OutOfOrderEvent() {
    }

    public OutOfOrderEvent(
            int subtaskIndex,
            LocalDateTime eventTime,
            long latenessMs) {

        this.subtaskIndex = subtaskIndex;
        this.eventTime = eventTime;
        this.latenessMs = latenessMs;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public long getLatenessMs() {
        return latenessMs;
    }
}
