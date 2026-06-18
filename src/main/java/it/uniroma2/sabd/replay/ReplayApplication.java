package it.uniroma2.sabd.replay; 
import it.uniroma2.sabd.model.FlightEvent; 
import it.uniroma2.sabd.replay.loader.HdfsFlightLoader;

import java.util.List;


public class ReplayApplication {
    public static void main(String[] args) throws Exception {
        String hdfsUri =
                System.getenv()
                        .getOrDefault(
                                "HDFS_URI",
                                "hdfs://namenode:8020"
                        );

        String hdfsFilePath =
                System.getenv()
                        .getOrDefault(
                                "HDFS_FILE_PATH",
                                "/nifi_output/merge.csv"
                        );

        System.out.println(
                "Reading HDFS file "
                        + hdfsFilePath
                        + " from "
                        + hdfsUri
        );

        HdfsFlightLoader loader =
                new HdfsFlightLoader();

        List<FlightEvent> events =
                loader.load(
                        hdfsUri,
                        hdfsFilePath
                );
        System.out.println(
                "Loaded events: "
                        + events.size()
        );
        ReplayEngine replayEngine =
                new ReplayEngine(10000);

        replayEngine.replay(events);
        
    }
}
