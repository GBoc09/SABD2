package it.uniroma2.sabd.replay.loader;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.utils.EventTimeBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.List;

public class HdfsFlightLoader {

    private double parseDoubleOrZero(String value) {
        if (value == null) {
            return 0.0;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return 0.0;
        }
        return Double.parseDouble(value);
    }

    private int parseIntOrZero(String value) {  
        if (value == null) {
            return 0;
        }
        value = value.trim();   
        if (value.isEmpty()) {
            return 0;
        }
        return Integer.parseInt(value);
    }

    public List<FlightEvent> load(
            String hdfsUri,
            String filePath)
            throws IOException {

        List<FlightEvent> events =
                new ArrayList<>();

        Configuration conf =
                new Configuration();

        conf.set(
                "fs.defaultFS",
                hdfsUri
        );
        conf.set(
                "fs.hdfs.impl",
                DistributedFileSystem.class.getName()
        );
        conf.set(
                "fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );


        FileSystem fs =
                FileSystem.get(conf);

        Path path =
                new Path(filePath);

        try (
                FSDataInputStream input =
                        fs.open(path);

                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(input))
        ) {

            String line;

            // salto header
            reader.readLine();

            while ((line = reader.readLine()) != null) {

                String[] values =
                        line.split(",", -1);

                FlightEvent event =
                        new FlightEvent();

                event.setYear(
                        parseIntOrZero(values[0]));

                event.setMonth(
                        parseIntOrZero(values[1]));

                event.setDayOfMonth(
                        parseIntOrZero(values[2]));

                event.setCarrier(
                        values[3]);

                event.setOriginAirportId(
                        parseIntOrZero(values[4]));

                event.setCrsDepTime(
                        parseIntOrZero(values[5]));

                event.setDepDelay(
                        parseDoubleOrZero(values[6]));

                event.setCancelled(
                        parseDoubleOrZero(values[7]));

                event.setDiverted(
                        parseDoubleOrZero(values[8]));

                event.setEventTime(
                        EventTimeBuilder.build(
                                event.getYear(),
                                event.getMonth(),
                                event.getDayOfMonth(),
                                event.getCrsDepTime()
                        )
                );

                events.add(event);
            }
        }

        return events;
    }

    
}
