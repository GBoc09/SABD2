package it.uniroma2.sabd.replay;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.kafka.KafkaFlightProducer;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReplayEngine {

    private final long accelerationFactor;
    private final KafkaFlightProducer producer;

    public ReplayEngine(long accelerationFactor, KafkaFlightProducer producer) {
        this.accelerationFactor = accelerationFactor;
        this.producer = producer;
    }

    public void replay(List<FlightEvent> events) throws InterruptedException {

        if (events.isEmpty()) {
            System.out.println("No events found.");
            return;
        }

        System.out.println("Sorting events...");

       /* events.sort(
                Comparator.comparing(
                        FlightEvent::getEventTime
                )
        );*/

        System.out.println(
                "Replay started. Events: "
                        + events.size()
        );

        FlightEvent previous = events.get(0);

        producer.send(previous);

        for (int i = 1; i < events.size(); i++) {
            FlightEvent current = events.get(i);

            long deltaMillis =
                    Duration.between(
                            previous.getEventTime(),
                            current.getEventTime()
                    ).toMillis();

            long sleepMillis = Math.max(
                    0,
                    deltaMillis / accelerationFactor
            );

            if (sleepMillis > 0) {
                TimeUnit.MILLISECONDS.sleep(sleepMillis);
            }

            producer.send(current);

            previous = current;
        }
        /*

        System.out.println(previous);

        for (int i = 1; i < events.size(); i++) {

            FlightEvent current = events.get(i);

            long deltaMillis =
                    Duration.between(
                            previous.getEventTime(),
                            current.getEventTime()
                    ).toMillis();

            long sleepMillis = deltaMillis / accelerationFactor;

            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }

            System.out.println(current);

            previous = current;
        }
        */

        System.out.println("Replay completed.");
    }
}