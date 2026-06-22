package it.uniroma2.sabd.replay.loader;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.replay.parser.FlightCsvParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class HdfsFlightLoader {

    public List<FlightEvent> load(String hdfsUri, String filePath) throws Exception {
        List<FlightEvent> events = new ArrayList<>();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            reader.readLine(); // Salta header

            String line;
            while ((line = reader.readLine()) != null) {
                FlightEvent event = FlightCsvParser.parseLine(line);
                if (event != null) {
                    events.add(event);
                }
            }
        }
        return events;
    }
}

