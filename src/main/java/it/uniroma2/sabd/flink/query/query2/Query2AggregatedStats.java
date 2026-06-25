package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.flink.model.Query2Stats.DelayedFlight;
import java.io.Serializable;
import java.util.List;

/**
 * Risultato intermedio dell'aggregazione per aeroporto.
 * Stesso pattern di Query1AggregatedStats / Query3AggregatedStats.
 */
final class Query2AggregatedStats implements Serializable {

    final int originAirportId;
    final long numFlights;
    final long severeDelays;
    final double depDelaySum;
    final double depDelayMax;
    final List<DelayedFlight> delayedFlights;

    Query2AggregatedStats(
            int originAirportId,
            long numFlights,
            long severeDelays,
            double depDelaySum,
            double depDelayMax,
            List<DelayedFlight> delayedFlights) {
        this.originAirportId  = originAirportId;
        this.numFlights       = numFlights;
        this.severeDelays     = severeDelays;
        this.depDelaySum      = depDelaySum;
        this.depDelayMax      = depDelayMax;
        this.delayedFlights   = delayedFlights;
    }

    double depDelayMean() {
        return numFlights > 0 ? depDelaySum / numFlights : 0.0;
    }
}