package it.uniroma2.sabd.flink.metrics;

public interface HasProducedAt {
    long getProducedAt();
    void setProducedAt(long producedAt);
}
