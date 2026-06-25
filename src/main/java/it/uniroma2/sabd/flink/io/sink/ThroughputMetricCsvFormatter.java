package it.uniroma2.sabd.flink.io.sink;

import it.uniroma2.sabd.flink.model.ThroughputMetric;
import org.apache.flink.api.common.functions.MapFunction;

public class ThroughputMetricCsvFormatter implements MapFunction<ThroughputMetric, String> {

    @Override
    public String map(ThroughputMetric value) {
        return String.join(",",
                Long.toString(value.getTimestampMs()),
                CsvValues.text(value.getLabel()),
                Integer.toString(value.getSourceSubtaskIndex()),
                Long.toString(value.getWindowStartMs()),
                Long.toString(value.getWindowEndMs()),
                Long.toString(value.getWindowDurationMs()),
                Long.toString(value.getWindowEvents()),
                Long.toString(value.getTotalEvents()),
                CsvValues.decimal(value.getInstantThroughputEventsPerSecond()),
                CsvValues.decimal(value.getAverageThroughputEventsPerSecond()));
    }
}
