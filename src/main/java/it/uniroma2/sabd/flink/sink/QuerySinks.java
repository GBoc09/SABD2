package it.uniroma2.sabd.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;

public final class QuerySinks {

    private static final String QUERY1_OUTPUT_PATH = "output/query1";
    private static final String QUERY3_1DAY_OUTPUT_PATH = "output/query3/1day";
    private static final String QUERY3_7DAY_OUTPUT_PATH = "output/query3/7day";
    private static final String QUERY3_GLOBAL_OUTPUT_PATH = "output/query3/global";

    private QuerySinks() {
    }

    public static FileSink<String> query1Csv() {
        return csvSink(QUERY1_OUTPUT_PATH);
    }

    public static FileSink<String> query3OneDayCsv() {
        return csvSink(QUERY3_1DAY_OUTPUT_PATH);
    }

    public static FileSink<String> query3SevenDaysCsv() {
        return csvSink(QUERY3_7DAY_OUTPUT_PATH);
    }

    public static FileSink<String> queryGlobalCsv() {
        return csvSink(QUERY3_GLOBAL_OUTPUT_PATH);
    }

    private static FileSink<String> csvSink(String outputPath) {
        return FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .build();
    }
}
