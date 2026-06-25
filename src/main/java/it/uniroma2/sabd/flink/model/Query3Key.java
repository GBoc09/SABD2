package it.uniroma2.sabd.flink.model;

import java.io.Serializable;
import java.util.Objects;

public class Query3Key implements Serializable {
    private String airline;
    private int departureHour;

    public Query3Key() {
    }

    public Query3Key(String airline, int departureHour) {
        this.airline = airline;
        this.departureHour = departureHour;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public int getDepartureHour() {
        return departureHour;
    }

    public void setDepartureHour(int departureHour) {
        this.departureHour = departureHour;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Query3Key query3Key = (Query3Key) o;
        return departureHour == query3Key.departureHour
                && Objects.equals(airline, query3Key.airline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(airline, departureHour);
    }
}
