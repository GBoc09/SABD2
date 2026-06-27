package it.uniroma2.sabd.replay;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.kafka.KafkaFlightProducer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ReplayEngine {

    private final long accelerationFactor;
    private final List<KafkaFlightProducer> producers;
    private final long maxNetworkDelayMillis;
    private final int speedSkewPercent;
    private final long randomSeed;

    public ReplayEngine(
            long accelerationFactor,
            List<KafkaFlightProducer> producers,
            long maxNetworkDelayMillis,
            int speedSkewPercent,
            long randomSeed) {
        this.accelerationFactor = accelerationFactor;
        this.producers = producers;
        this.maxNetworkDelayMillis = maxNetworkDelayMillis;
        this.speedSkewPercent = speedSkewPercent;
        this.randomSeed = randomSeed;
    }

    public void replay(List<FlightEvent> events) throws InterruptedException {

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
                "Distributed replay started. Events: "
                        + events.size()
                        + ", producers: "
                        + producers.size()
        );

        List<List<FlightEvent>> subStreams =
                splitRoundRobin(events, producers.size());

        ExecutorService executor =
                Executors.newFixedThreadPool(producers.size());
        CountDownLatch ready =
                new CountDownLatch(producers.size());
        CountDownLatch start =
                new CountDownLatch(1);
        List<Future<?>> futures =
                new ArrayList<>();

        for (int i = 0; i < producers.size(); i++) {
            final int producerId = i;
            final List<FlightEvent> subStream = subStreams.get(i);
            final KafkaFlightProducer producer = producers.get(i);

            futures.add(
                    executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        replaySubStream(producerId, subStream, producer);
                        return null;
                    })
            );
        }

        try {
            ready.await();
            start.countDown();

            for (Future<?> future : futures) {
                future.get();
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("Distributed replay failed", e.getCause());
        } finally {
            executor.shutdownNow();
        }

        System.out.println("Distributed replay completed.");
    }

    private List<List<FlightEvent>> splitRoundRobin(
            List<FlightEvent> events,
            int producerCount) {

        List<List<FlightEvent>> subStreams =
                new ArrayList<>(producerCount);

        for (int i = 0; i < producerCount; i++) {
            subStreams.add(new ArrayList<>());
        }

        for (int i = 0; i < events.size(); i++) {
            subStreams.get(i % producerCount).add(events.get(i));
        }

        return subStreams;
    }

    private void replaySubStream(
            int producerId,
            List<FlightEvent> subStream,
            KafkaFlightProducer producer)
            throws InterruptedException {

        Random random =
                new Random(randomSeed + producerId);
        double speedMultiplier =
                createSpeedMultiplier(random);

        System.out.println(
                "Producer "
                        + producerId
                        + " started. Events: "
                        + subStream.size()
                        + ", speed multiplier: "
                        + String.format("%.2f", speedMultiplier)
        );

        FlightEvent previous =
                null;

        sleepMillis(randomInitialDelayMillis(random));

        for (FlightEvent current : subStream) {
            if (previous != null) {
                long deltaMillis =
                        Duration.between(
                                previous.getEventTime(),
                                current.getEventTime()
                        ).toMillis();

                long sleepMillis =
                        scaledSleepMillis(deltaMillis, speedMultiplier);

                sleepMillis(sleepMillis);
            }

            producer.send(current);

            previous = current;
        }

        producer.flush();

        System.out.println(
                "Producer "
                        + producerId
                        + " completed. Events: "
                        + subStream.size()
        );
    }

    private double createSpeedMultiplier(Random random) {
        if (speedSkewPercent == 0) {
            return 1.0;
        }

        double maxSkew =
                speedSkewPercent / 100.0;
        double skew =
                ((random.nextDouble() * 2.0) - 1.0) * maxSkew;

        return Math.max(0.1, 1.0 + skew);
    }

    private long scaledSleepMillis(
            long deltaMillis,
            double speedMultiplier) {

        if (deltaMillis <= 0) {
            return 0;
        }

        return Math.max(
                0,
                Math.round(
                        (deltaMillis / (double) accelerationFactor)
                                * speedMultiplier
                )
        );
    }

    private long randomInitialDelayMillis(Random random) {
        if (maxNetworkDelayMillis == 0) {
            return 0;
        }

        return (long) Math.floor(
                random.nextDouble()
                        * (maxNetworkDelayMillis + 1)
        );
    }

    private void sleepMillis(long sleepMillis) throws InterruptedException {
        if (sleepMillis > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepMillis);
        }
    }
}
