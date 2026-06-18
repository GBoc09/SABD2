package it.uniroma2.sabd.replay;

import it.uniroma2.sabd.model.FlightEvent;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

public class ReplayEngine {

    private final long accelerationFactor;

    public ReplayEngine(long accelerationFactor) {
        this.accelerationFactor = accelerationFactor;
    }

    public void replay(List<FlightEvent> events)
            throws InterruptedException {

        if (events.isEmpty()) {
            System.out.println("No events found.");
            return;
        }

        System.out.println("Sorting events...");

        events.sort(
                Comparator.comparing(
                        FlightEvent::getEventTime
                )
        );

        System.out.println(
                "Replay started. Events: "
                        + events.size()
        );

        FlightEvent previous =
                events.get(0);

        System.out.println(previous);

        for (int i = 1; i < events.size(); i++) {

            FlightEvent current =
                    events.get(i);

            long deltaMillis =
                    Duration.between(
                            previous.getEventTime(),
                            current.getEventTime()
                    ).toMillis();

            long sleepMillis =
                    deltaMillis / accelerationFactor;

            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }

            System.out.println(current);

            previous = current;
        }

        System.out.println("Replay completed.");
    }
}