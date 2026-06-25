package it.uniroma2.sabd.flink.io.sink;

import it.uniroma2.sabd.flink.model.LatencyMetric;
import it.uniroma2.sabd.flink.model.ThroughputMetric;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class PerformanceSinks {

    private static final int METRIC_SINK_PARALLELISM = 1;

    private PerformanceSinks() {
    }

    public static void writeLatencyCsv(
            DataStream<LatencyMetric> metrics,
            String baseOutputPath) {
        metrics
                .map(new LatencyMetricCsvFormatter())
                .name("Latency Performance CSV Formatter")
                .sinkTo(csvSink(baseOutputPath + "/latency"))
                .name("Latency Performance CSV Sink")
                .setParallelism(METRIC_SINK_PARALLELISM);
    }

    public static void writeThroughputCsv(
            DataStream<ThroughputMetric> metrics,
            String baseOutputPath) {
        metrics
                .map(new ThroughputMetricCsvFormatter())
                .name("Throughput Performance CSV Formatter")
                .sinkTo(csvSink(baseOutputPath + "/throughput"))
                .name("Throughput Performance CSV Sink")
                .setParallelism(METRIC_SINK_PARALLELISM);
    }

    private static FileSink<String> csvSink(String outputPath) {
        return FileSink
                .forRowFormat(
                        new Path(outputPath),
                        new SimpleStringEncoder<String>("UTF-8"))
                .build();
    }
}
