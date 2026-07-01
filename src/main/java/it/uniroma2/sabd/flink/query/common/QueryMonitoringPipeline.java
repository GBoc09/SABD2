package it.uniroma2.sabd.flink.query.common;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.controller.LatencyMonitor;
import it.uniroma2.sabd.flink.controller.PerformanceMetricTags;
import it.uniroma2.sabd.flink.controller.ThroughputMonitor;
import it.uniroma2.sabd.flink.io.sink.PerformanceSinks;
import it.uniroma2.sabd.flink.model.AbstractQueryStats;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Fattorizza la sequenza ripetuta identicamente in Query1, Query2, Query3
 * per ogni ramo (1h/6h/global, 1day/7day/global, ecc.):
 *   latency monitor -> throughput monitor -> performance CSV -> CSV finale
 * Prima di questo refactoring la stessa sequenza di ~25 righe era duplicata
 * 1 volta in Query1, 3 volte in Query2, 3 volte in Query3.
 */
public final class QueryMonitoringPipeline {

    private QueryMonitoringPipeline() {}

    /**
     * @param ranking     stream del risultato della query per questo ramo (es. Q2 1h)
     * @param config      configurazione applicativa (intervalli di reporting, path output)
     * @param watermarkName nome della strategia watermark in uso (WM15, WM100, ADAPTIVE)
     * @param queryName   prefisso della query, usato nei path e nelle label (es. "q2")
     * @param windowLabel etichetta della finestra, usata nei path e nelle label (es. "1h", "global")
     * @param toCsv       come trasformare lo stat in riga CSV
     * @param csvSink     sink finale dove scrivere il CSV della query
     */
    public static <T extends AbstractQueryStats> void monitorAndSink(
            DataStream<T> ranking,
            AppConfig config,
            String watermarkName,
            String queryName,
            String windowLabel,
            java.util.function.Function<T, String> toCsv,
            FileSink<String> csvSink) {

        String resultLabel = queryName + "-" + windowLabel + "-result-" + watermarkName;
        String namePrefix = queryName.toUpperCase() + " " + windowLabel + " Result";

        SingleOutputStreamOperator<T> latencyMonitored = ranking
                .process(new LatencyMonitor<T>(resultLabel, config.getMetricsLatencyIntervalMs()))
                .name(namePrefix + " Latency Monitor");

        SingleOutputStreamOperator<T> monitored = latencyMonitored
                .process(new ThroughputMonitor<T>(resultLabel, config.getMetricsThroughputIntervalMs()))
                .name(namePrefix + " Throughput Monitor");

        PerformanceSinks.writeLatencyCsvAtPath(
                latencyMonitored.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath() + "/" + watermarkName
                        + "/latency/" + queryName + "_" + windowLabel);

        PerformanceSinks.writeGlobalThroughputCsvAtPath(
                monitored.getSideOutput(PerformanceMetricTags.THROUGHPUT),
                config.getPerformanceOutputPath() + "/" + watermarkName
                        + "/throughput/" + queryName + "_" + windowLabel,
                config.getMetricsThroughputIntervalMs());

        monitored
                .map(toCsv::apply)
                .sinkTo(csvSink)
                .name(namePrefix + " CSV Sink");
    }
}
