package it.uniroma2.sabd.flink.io.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;

public final class QuerySinks {

    private QuerySinks() {}

    public static FileSink<String> query1Csv(String watermark) {
        return csvSink("output/" + watermark + "/query1");
    }

    public static FileSink<String> query2SixHoursCsv(String watermark) {
        return csvSink("output/" + watermark + "/query2/6h");
    }
    public static FileSink<String> query2OneHourCsv(String watermark) {
        return csvSink("output/" + watermark + "/query2/1h");
    }
    public static FileSink<String> query2GlobalCsv(String watermark) {
        return csvSink("output/" + watermark + "/query2/global");
    }
    public static FileSink<String> query3OneDayCsv(String watermark) {
        return csvSink("output/" + watermark + "/query3/1day");
    }

    public static FileSink<String> query3SevenDaysCsv(String watermark) {
        return csvSink("output/" + watermark + "/query3/7day");
    }

    public static FileSink<String> queryGlobalCsv(String watermark) {
        return csvSink("output/" + watermark + "/query3/global");
    }

    public static FileSink<String> discardedTuplesCsv(
            String watermark,
            String query,
            String window) {

        return csvSink("output/" + watermark + "/discarded_tuples/" + query + "/" + window);
    }

    private static FileSink<String> csvSink(String outputPath) {
        return FileSink
                .forRowFormat(
                        new Path(outputPath),
                        new SimpleStringEncoder<String>("UTF-8"))
                .build();
    }
}
