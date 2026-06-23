package it.uniroma2.sabd.model;

import it.uniroma2.sabd.flink.metrics.HasProducedAt;
import java.time.LocalDateTime;

public class FlightEvent implements HasProducedAt {

    private LocalDateTime eventTime;
    private int year;
    private int month;
    private int dayOfMonth;
    private String carrier;
    private int originAirportId;
    private int crsDepTime;
    private double depDelay;
    private double cancelled;
    private double diverted;

    // Metadato di pipeline: timestamp ms in cui il producer ha inviato
    // questo evento a Kafka. Non fa parte del dominio del volo.
    // Valorizzato da KafkaFlightProducer con System.currentTimeMillis().
    private long producedAt;

    public FlightEvent() {}

    // --- HasProducedAt ---

    public long getProducedAt() { return producedAt; }
    public void setProducedAt(long producedAt) { this.producedAt = producedAt; }

    // --- getters/setters invariati ---
    public LocalDateTime getEventTime() { return eventTime; }
    public void setEventTime(LocalDateTime eventTime) { this.eventTime = eventTime; }

    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    public int getMonth() { return month; }
    public void setMonth(int month) { this.month = month; }

    public int getDayOfMonth() { return dayOfMonth; }
    public void setDayOfMonth(int dayOfMonth) { this.dayOfMonth = dayOfMonth; }

    public String getCarrier() { return carrier; }
    public void setCarrier(String carrier) { this.carrier = carrier; }

    public int getOriginAirportId() { return originAirportId; }
    public void setOriginAirportId(int originAirportId) { this.originAirportId = originAirportId; }

    public int getCrsDepTime() { return crsDepTime; }
    public void setCrsDepTime(int crsDepTime) { this.crsDepTime = crsDepTime; }

    public double getDepDelay() { return depDelay; }
    public void setDepDelay(double depDelay) { this.depDelay = depDelay; }

    public double getCancelled() { return cancelled; }
    public void setCancelled(double cancelled) { this.cancelled = cancelled; }

    public double getDiverted() { return diverted; }
    public void setDiverted(double diverted) { this.diverted = diverted; }

    @Override
    public String toString() {
        return "FlightEvent{" +
                "eventTime=" + eventTime +
                ", carrier='" + carrier + '\'' +
                ", originAirportId=" + originAirportId +
                ", crsDepTime=" + crsDepTime +
                ", depDelay=" + depDelay +
                '}';
    }
}