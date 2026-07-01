package it.uniroma2.sabd.flink.engineering.watermarks;

import it.uniroma2.sabd.config.AppConfig;

public class WatermarkRegistry {
//5 * 60 * 1000
    public static WatermarkFactory get(
            WatermarkType type,
            AppConfig config) {

        switch(type) {

            case WM15:
                System.out.println("BOUNDED STRATEGY 15MIN ATTIVA");
                return new BoundedOutOfOrderStrategy(
                        15 * 60 * 1000);

            case WM100:
                System.out.println("BOUNDED STRATEGY 100MIN ATTIVA");
                return new BoundedOutOfOrderStrategy(
                        100 * 60 * 1000);

            case ADAPTIVE:
                System.out.println("STRATEGIA ADATTATIVA ATTIVA");
                return new AdaptiveStrategy();

            default:
                throw new IllegalArgumentException();
        }
    }
}
