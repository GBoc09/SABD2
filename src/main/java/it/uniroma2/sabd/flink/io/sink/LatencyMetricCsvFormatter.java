package it.uniroma2.sabd.flink.io.sink;

import it.uniroma2.sabd.flink.model.LatencyMetric;
import org.apache.flink.api.common.functions.MapFunction;

public class LatencyMetricCsvFormatter implements MapFunction<LatencyMetric, String> {

    @Override
    public String map(LatencyMetric value) {
        return String.join(",",
                Long.toString(value.getTimestampMs()),
                CsvValues.text(value.getLabel()),
                Integer.toString(value.getSourceSubtaskIndex()),
                Long.toString(value.getWindowStartMs()),
                Long.toString(value.getWindowEndMs()),
                Long.toString(value.getWindowDurationMs()),
                Long.toString(value.getWindowEvents()),
                Long.toString(value.getTotalEvents()),
                Long.toString(value.getMinLatencyMs()),
                Long.toString(value.getMaxLatencyMs()),
                CsvValues.decimal(value.getAvgLatencyMs()));
    }
}
