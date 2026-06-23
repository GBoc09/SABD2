package it.uniroma2.sabd.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;

public final class QuerySinks {

    private static final String QUERY1_OUTPUT_PATH = "output/query1";

    private QuerySinks() {
    }

    public static FileSink<String> query1Csv() {
        return FileSink
                .forRowFormat(new Path(QUERY1_OUTPUT_PATH), new SimpleStringEncoder<String>("UTF-8"))
                .build();
    }
}
