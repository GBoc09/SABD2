package it.uniroma2.sabd.flink.watermark;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.watermark.WatermarkFactory;
import it.uniroma2.sabd.flink.watermark.BoundedOutOfOrderStrategy;
import it.uniroma2.sabd.flink.watermark.AdaptiveStrategy;

public class WatermarkRegistry {

    public static WatermarkFactory get(
            WatermarkType type,
            AppConfig config) {

        switch(type) {

            case WM15:
                return new BoundedOutOfOrderStrategy(
                        15 * 60 * 1000);

            case WM30:
                return new BoundedOutOfOrderStrategy(
                        30 * 60 * 1000);

            case ADAPTIVE:
                System.out.println(">>> ADAPTIVE STRATEGY ACTIVE");
                return new AdaptiveStrategy();

            default:
                throw new IllegalArgumentException();
        }
    }
}