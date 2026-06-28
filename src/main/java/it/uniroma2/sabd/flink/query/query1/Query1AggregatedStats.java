package it.uniroma2.sabd.flink.query.query1;

final class Query1AggregatedStats {
    final long totalFlights;
    final long completedFlights;
    final long cancelledFlights;
    final long divertedFlights;
    final double avgDepDelay;
    final double cancellationRate;
    final double lateDepartureRate;
    final long processingStartTimeMs;

    Query1AggregatedStats(
            long totalFlights,
            long completedFlights,
            long cancelledFlights,
            long divertedFlights,
            double avgDepDelay,
            double cancellationRate,
            double lateDepartureRate,
            long processingStartTimeMs) {
        this.totalFlights = totalFlights;
        this.completedFlights = completedFlights;
        this.cancelledFlights = cancelledFlights;
        this.divertedFlights = divertedFlights;
        this.avgDepDelay = avgDepDelay;
        this.cancellationRate = cancellationRate;
        this.lateDepartureRate = lateDepartureRate;
        this.processingStartTimeMs = processingStartTimeMs;
    }
}
